// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method2;
use internal_error::ResultIntoInternal;
use kamu_core::*;
use kamu_datasets::{DatasetHeadCommitter, DatasetRegistry, ResolvedDataset};
use random_strings::get_random_name;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Repeats polling ingest iterations of one dataset for `kamu pull`,
/// committing `HEAD` after each of them
pub(crate) struct PullIngestLoop {
    catalog: dill::Catalog,
    polling_ingest_service: Arc<dyn PollingIngestService>,
}

impl PullIngestLoop {
    pub(crate) fn new(
        catalog: dill::Catalog,
        polling_ingest_service: Arc<dyn PollingIngestService>,
    ) -> Self {
        Self {
            catalog,
            polling_ingest_service,
        }
    }

    /// Ingests until the source reports no more data, or once when
    /// `exhaust_sources` is off. The result spans all committed iterations. An
    /// iteration that fails after others committed is
    /// [`PullError::IngestStopped`].
    pub(crate) async fn run(
        &self,
        target_handle: &odf::DatasetHandle,
        options: PollingIngestOptions,
        exhaust_sources: bool,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PollingIngestResult, PullError> {
        let (target, mut plan, env_vars) = match self.plan(target_handle).await {
            Ok(planned) => planned,
            Err(PollingIngestPlanError::SourceNotDefined(_)) => {
                if let Some(listener) = &maybe_listener {
                    listener.begin();
                    listener.no_polling_source();
                }
                return Ok(PollingIngestResult::UpToDate { uncacheable: false });
            }
            Err(PollingIngestPlanError::ScanMetadata(e)) => return Err(e.into()),
            Err(PollingIngestPlanError::Internal(e)) => return Err(e.into()),
        };

        let mut combined: Option<PollingIngestResult> = None;
        loop {
            let response = match self
                .polling_ingest_service
                .ingest(
                    target.clone(),
                    plan,
                    options.clone(),
                    &env_vars,
                    &get_random_name(None, 10),
                    maybe_listener.clone(),
                )
                .await
            {
                Ok(response) => response,
                Err(e) => return Err(Self::stopped(combined, e)),
            };

            let has_more = match &response.result {
                PollingIngestResult::UpToDate { .. } => false,
                PollingIngestResult::Updated {
                    old_head,
                    new_head,
                    has_more,
                    ..
                } => {
                    if let Err(e) = self
                        .commit_head(target.get_handle(), old_head, new_head)
                        .await
                    {
                        let e = PollingIngestError::CommitError(e.into());
                        return Err(Self::stopped(combined, e));
                    }
                    *has_more
                }
            };

            combined = Some(Self::merge_results(combined, response.result));

            if !has_more || !exhaust_sources {
                break;
            }

            let metadata_state = response
                .metadata_state
                .expect("An updated ingest returns its writer state");
            plan = PollingIngestPlan::new(target.get_handle(), metadata_state).int_err()?;
        }

        Ok(combined.expect("The loop runs at least once"))
    }

    /// Plans the first iteration and resolves the environment variables in one
    /// transaction, and returns the target detached from it
    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, polling_ingest_planner: Arc<dyn PollingIngestPlanner>)]
    async fn plan(
        &self,
        target_handle: &odf::DatasetHandle,
    ) -> Result<(ResolvedDataset, PollingIngestPlan, IngestEnvVars), PollingIngestPlanError> {
        let target = dataset_registry.get_dataset_by_handle(target_handle).await;

        let plan = polling_ingest_planner.plan_ingest(target.clone()).await?;
        let env_vars = polling_ingest_planner
            .resolve_env_vars(&target_handle.id)
            .await?;

        target.detach_from_transaction();

        Ok((target, plan, env_vars))
    }

    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, dataset_head_committer: Arc<dyn DatasetHeadCommitter>)]
    async fn commit_head(
        &self,
        target_handle: &odf::DatasetHandle,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        let target = dataset_registry.get_dataset_by_handle(target_handle).await;

        dataset_head_committer
            .commit_head(&target, Some(old_head), new_head)
            .await
    }

    fn stopped(combined: Option<PollingIngestResult>, error: PollingIngestError) -> PullError {
        match combined {
            Some(PollingIngestResult::Updated {
                old_head,
                new_head,
                increment,
                ..
            }) => PullError::IngestStopped(Box::new(IngestStoppedError {
                old_head,
                new_head,
                increment,
                source: error,
            })),
            None | Some(PollingIngestResult::UpToDate { .. }) => error.into(),
        }
    }

    /// Keeps the first `old_head` and the last `new_head`, and adds up the
    /// increments
    fn merge_results(
        combined: Option<PollingIngestResult>,
        next: PollingIngestResult,
    ) -> PollingIngestResult {
        match (combined, next) {
            (None | Some(PollingIngestResult::UpToDate { .. }), next) => next,
            (
                Some(PollingIngestResult::Updated {
                    old_head,
                    new_head,
                    increment,
                    new_source_state,
                    ..
                }),
                PollingIngestResult::UpToDate { uncacheable },
            ) => PollingIngestResult::Updated {
                old_head,
                new_head,
                has_more: false,
                uncacheable,
                increment,
                new_source_state,
            },
            (
                Some(PollingIngestResult::Updated {
                    old_head,
                    mut increment,
                    ..
                }),
                PollingIngestResult::Updated {
                    new_head,
                    has_more,
                    uncacheable,
                    increment: next_increment,
                    new_source_state,
                    ..
                },
            ) => {
                increment += next_increment;
                PollingIngestResult::Updated {
                    old_head,
                    new_head,
                    has_more,
                    uncacheable,
                    increment,
                    new_source_state,
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
