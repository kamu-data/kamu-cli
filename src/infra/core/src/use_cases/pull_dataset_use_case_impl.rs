// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method1;
use dill::*;
use internal_error::InternalError;
use kamu_core::*;

use super::pull_authorization::{AuthorizedIteration, PullAuthorization};
use super::pull_ingest_loop::PullIngestLoop;
use super::pull_sync_job::PullSyncJob;
use super::pull_transform_job::PullTransformJob;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn PullDatasetUseCase)]
pub struct PullDatasetUseCaseImpl {
    transform_executor: Arc<dyn TransformExecutor>,
    compaction_planner: Arc<dyn CompactionPlanner>,
    compaction_executor: Arc<dyn CompactionExecutor>,
    tenancy_config: Arc<TenancyConfig>,
    polling_ingest_service: Arc<dyn PollingIngestService>,
    update_steps: Arc<dyn DatasetUpdateSteps>,
    catalog: dill::Catalog,
}

impl PullDatasetUseCaseImpl {
    #[tracing::instrument(level = "info", skip_all)]
    async fn pull_by_plan(
        &self,
        plan: Vec<PullPlanIteration>,
        options: PullOptions,
        listener: Option<Arc<dyn PullMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let mut results = Vec::new();

        // Prepare multi-listeners
        let maybe_ingest_multi_listener = listener
            .as_ref()
            .and_then(|l| l.clone().get_ingest_listener());

        let maybe_transform_multi_listener = listener
            .as_ref()
            .and_then(|l| l.clone().get_transform_listener());

        let maybe_sync_multi_listener = listener
            .as_ref()
            .and_then(|l| l.clone().get_sync_listener());

        // Execute each iteration
        for iteration in plan {
            tracing::info!(depth = %iteration.depth, jobs = ?iteration.jobs, "Running pull iteration");

            let AuthorizedIteration { iteration, denied } =
                PullAuthorization::new(self.catalog.clone())
                    .authorize(iteration)
                    .await?;

            if !denied.is_empty() {
                results.extend(denied);
                break;
            }

            // Run iteration jobs concurrently
            let mut tasks = tokio::task::JoinSet::new();
            for job in iteration.jobs {
                match job {
                    PullPlanIterationJob::Ingest(pii) => {
                        let maybe_listener = maybe_ingest_multi_listener
                            .as_ref()
                            .and_then(|l| l.begin_ingest(&pii.target));
                        tasks.spawn(
                            PullIngestLoop::new(
                                self.update_steps.clone(),
                                self.polling_ingest_service.clone(),
                            )
                            .run(pii, options.clone(), maybe_listener),
                        )
                    }
                    PullPlanIterationJob::Transform(pti) => {
                        let maybe_listener = maybe_transform_multi_listener
                            .as_ref()
                            .and_then(|l| l.begin_transform(&pti.target));

                        tasks.spawn(
                            PullTransformJob::new(
                                self.update_steps.clone(),
                                self.transform_executor.clone(),
                                self.compaction_planner.clone(),
                                self.compaction_executor.clone(),
                                self.catalog.clone(),
                            )
                            .run(
                                pti,
                                options.transform_options,
                                maybe_listener,
                            ),
                        )
                    }
                    PullPlanIterationJob::Sync(psi) => {
                        let maybe_listener = maybe_sync_multi_listener.as_ref().and_then(|l| {
                            l.begin_sync(
                                &psi.remote_ref.as_any_ref(),
                                &psi.local_target.as_local_ref().as_any_ref(),
                            )
                        });
                        tasks.spawn(PullSyncJob::new(self.update_steps.clone()).run(
                            psi,
                            options.clone(),
                            maybe_listener,
                        ))
                    }
                };
            }
            let iteration_results = tasks.join_all().await;
            tracing::info!(iteration_result=?iteration_results, "Pull iteration finished");

            // Deal with results
            let mut has_errors = false;
            for result in iteration_results {
                let result = result?;
                if result.result.is_err() {
                    has_errors = true;
                }
                results.push(result);
            }
            if has_errors {
                break;
            }
        }

        Ok(results)
    }

    #[transactional_method1(pull_request_planner: Arc<dyn PullRequestPlanner>)]
    async fn build_plan_for_requests(
        &self,
        requests: &[PullRequest],
        options: &PullOptions,
    ) -> Result<PullPlan, InternalError> {
        Ok(pull_request_planner
            .build_pull_multi_plan(requests, options, *self.tenancy_config)
            .await)
    }

    #[transactional_method1(pull_request_planner: Arc<dyn PullRequestPlanner>)]
    async fn build_plan_for_all_owned_datasets(
        &self,
        options: &PullOptions,
    ) -> Result<PullPlan, InternalError> {
        pull_request_planner
            .build_pull_plan_all_owner_datasets(options, *self.tenancy_config)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PullDatasetUseCase for PullDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = "PullDatasetUseCase::execute",
        skip_all,
        fields(?request, ?options)
    )]
    async fn execute(
        &self,
        request: PullRequest,
        options: PullOptions,
        listener: Option<Arc<dyn PullListener>>,
    ) -> Result<PullResponse, InternalError> {
        let listener =
            listener.map(|l| Arc::new(ListenerMultiAdapter(l)) as Arc<dyn PullMultiListener>);

        let mut responses = self.execute_multi(vec![request], options, listener).await?;

        assert_eq!(responses.len(), 1);
        Ok(responses.pop().unwrap())
    }

    #[tracing::instrument(
        level = "info",
        name = "PullDatasetUseCase::execute_multi",
        skip_all,
        fields(?requests, ?options)
    )]
    async fn execute_multi(
        &self,
        requests: Vec<PullRequest>,
        options: PullOptions,
        listener: Option<Arc<dyn PullMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        tracing::info!(?requests, ?options, "Performing pull");

        let PullPlan { iterations, errors } = self
            .build_plan_for_requests(requests.as_slice(), &options)
            .await?;

        tracing::info!(
            num_steps = iterations.len(),
            num_errors = errors.len(),
            "Prepared pull execution plan"
        );
        if !errors.is_empty() {
            return Ok(errors);
        }

        self.pull_by_plan(iterations, options, listener).await
    }

    #[tracing::instrument(
        level = "info",
        name = "PullDatasetUseCase::execute_all_owned",
        skip_all,
        fields(?options)
    )]
    async fn execute_all_owned(
        &self,
        options: PullOptions,
        listener: Option<Arc<dyn PullMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        tracing::info!(?options, "Performing pull (all owned)");

        let PullPlan { iterations, errors } =
            self.build_plan_for_all_owned_datasets(&options).await?;

        tracing::info!(
            num_steps = iterations.len(),
            num_errors = errors.len(),
            "Prepared pull execution plan (all owned)"
        );
        if !errors.is_empty() {
            return Ok(errors);
        }

        self.pull_by_plan(iterations, options, listener).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ListenerMultiAdapter(Arc<dyn PullListener>);

impl PullMultiListener for ListenerMultiAdapter {
    fn get_ingest_listener(self: Arc<Self>) -> Option<Arc<dyn PollingIngestMultiListener>> {
        Some(self)
    }

    fn get_transform_listener(self: Arc<Self>) -> Option<Arc<dyn TransformMultiListener>> {
        Some(self)
    }

    fn get_sync_listener(self: Arc<Self>) -> Option<Arc<dyn SyncMultiListener>> {
        Some(self)
    }
}

impl PollingIngestMultiListener for ListenerMultiAdapter {
    fn begin_ingest(
        &self,
        _dataset: &odf::DatasetHandle,
    ) -> Option<Arc<dyn PollingIngestListener>> {
        self.0.clone().get_ingest_listener()
    }
}

impl TransformMultiListener for ListenerMultiAdapter {
    fn begin_transform(&self, _dataset: &odf::DatasetHandle) -> Option<Arc<dyn TransformListener>> {
        self.0.clone().get_transform_listener()
    }
}

impl SyncMultiListener for ListenerMultiAdapter {
    fn begin_sync(
        &self,
        _src: &odf::DatasetRefAny,
        _dst: &odf::DatasetRefAny,
    ) -> Option<Arc<dyn SyncListener>> {
        self.0.clone().get_sync_listener()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
