// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_core::*;
use random_strings::get_random_name;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Repeats polling ingest iterations of one dataset for `kamu pull`,
/// committing `HEAD` after each of them
pub(crate) struct PullIngestLoop {
    update_steps: Arc<dyn DatasetUpdateSteps>,
    polling_ingest_service: Arc<dyn PollingIngestService>,
}

impl PullIngestLoop {
    pub(crate) fn new(
        update_steps: Arc<dyn DatasetUpdateSteps>,
        polling_ingest_service: Arc<dyn PollingIngestService>,
    ) -> Self {
        Self {
            update_steps,
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
        let PlannedIngest {
            target,
            mut plan,
            env_vars,
        } = match self.update_steps.plan_ingest(target_handle).await {
            Ok(Some(planned)) => planned,
            Ok(None) => {
                return Err(PullError::NotFound(odf::DatasetNotFoundError::new(
                    target_handle.as_local_ref(),
                )));
            }
            Err(e) => {
                if let Some(listener) = &maybe_listener {
                    listener.begin();
                    if let PollingIngestPlanError::SourceNotDefined(_) = e {
                        listener.no_polling_source();
                    } else {
                        listener.plan_error(&e);
                    }
                }
                return match e {
                    PollingIngestPlanError::SourceNotDefined(_) => {
                        Ok(PollingIngestResult::UpToDate { uncacheable: false })
                    }
                    PollingIngestPlanError::ScanMetadata(e) => Err(e.into()),
                    PollingIngestPlanError::Internal(e) => Err(e.into()),
                };
            }
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
                Err(e) => return Err(Self::stopped(combined, e, maybe_listener.as_ref())),
            };

            let has_more = match &response.result {
                PollingIngestResult::UpToDate { .. } => false,
                PollingIngestResult::Updated {
                    old_head,
                    new_head,
                    has_more,
                    ..
                } => {
                    if let Err(e) = HeadCommit::into_set_ref_result(
                        self.update_steps
                            .commit_head(target.get_handle(), old_head, new_head)
                            .await,
                    ) {
                        let e = PollingIngestError::CommitError(e.into());
                        return Err(Self::stopped(combined, e, maybe_listener.as_ref()));
                    }
                    *has_more
                }
            };

            if let Some(listener) = &maybe_listener {
                listener.success(&response.result);
            }

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

    /// Reports the failed iteration and turns its error into the pull's
    fn stopped(
        combined: Option<PollingIngestResult>,
        error: PollingIngestError,
        maybe_listener: Option<&Arc<dyn PollingIngestListener>>,
    ) -> PullError {
        if let Some(listener) = maybe_listener {
            listener.error(&error);
        }

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
