// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
//use kamu_core::auth::DatasetActionAuthorizer;
use kamu_core::{
    DatasetRegistry,
    PollingIngestListener,
    PollingIngestMultiListener,
    PollingIngestService,
    PullDatasetUseCase,
    PullExecutionStepKind,
    PullItem,
    PullListener,
    PullMultiListener,
    PullMultiOptions,
    PullOptions,
    PullRequest,
    PullRequestPlanner,
    PullResponse,
    RemoteAliasKind,
    RemoteAliasesRegistry,
    ResolvedDataset,
    SyncListener,
    SyncMultiListener,
    SyncResponse,
    SyncResult,
    SyncResultMulti,
    SyncService,
    TransformListener,
    TransformMultiListener,
    TransformOptions,
    TransformService,
};
use opendatafabric::{DatasetHandle, DatasetRefAny};

use crate::SyncRequestBuilder;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn PullDatasetUseCase)]
pub struct PullDatasetUseCaseImpl {
    pull_request_planner: Arc<dyn PullRequestPlanner>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    //dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    ingest_svc: Arc<dyn PollingIngestService>,
    transform_svc: Arc<dyn TransformService>,
    sync_svc: Arc<dyn SyncService>,
    sync_request_builder: Arc<SyncRequestBuilder>,
    in_multi_tenant_mode: bool,
}

impl PullDatasetUseCaseImpl {
    pub fn new(
        pull_plan_builder: Arc<dyn PullRequestPlanner>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        //dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        ingest_svc: Arc<dyn PollingIngestService>,
        transform_svc: Arc<dyn TransformService>,
        sync_svc: Arc<dyn SyncService>,
        sync_request_builder: Arc<SyncRequestBuilder>,
        in_multi_tenant_mode: bool,
    ) -> Self {
        Self {
            pull_request_planner: pull_plan_builder,
            dataset_registry,
            //dataset_action_authorizer,
            remote_alias_reg,
            ingest_svc,
            transform_svc,
            sync_svc,
            sync_request_builder,
            in_multi_tenant_mode,
        }
    }

    async fn ingest_multi(
        &self,
        batch: Vec<PullItem>,
        options: &PullMultiOptions,
        listener: Option<Arc<dyn PollingIngestMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let mut ingest_requests = Vec::new();
        for item in &batch {
            let dataset_handle = self
                .dataset_registry
                .resolve_dataset_handle_by_ref(&item.local_ref)
                .await
                .int_err()?;
            let dataset = self.dataset_registry.get_dataset_by_handle(&dataset_handle);
            ingest_requests.push(ResolvedDataset {
                dataset,
                handle: dataset_handle.clone(),
            });
        }

        let ingest_responses = self
            .ingest_svc
            .ingest_multi(ingest_requests, options.ingest_options.clone(), listener)
            .await;

        assert_eq!(batch.len(), ingest_responses.len());

        Ok(std::iter::zip(batch, ingest_responses)
            .map(|(pi, res)| {
                assert_eq!(pi.local_ref, res.dataset_ref);
                pi.into_response_ingest(res)
            })
            .collect())
    }

    async fn sync_multi(
        &self,
        batch: Vec<PullItem>,
        options: &PullMultiOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let mut sync_requests = Vec::new();
        let mut batch_rest = Vec::new();
        let mut errors = Vec::new();

        for pi in batch {
            let src_ref: DatasetRefAny = pi.remote_ref.as_ref().unwrap().into();
            let dst_ref = pi.local_ref.as_any_ref();
            match self
                .sync_request_builder
                .build_sync_request(
                    src_ref.clone(),
                    dst_ref.clone(),
                    options.sync_options.create_if_not_exists,
                )
                .await
            {
                Ok(request) => {
                    sync_requests.push(request);
                    batch_rest.push(pi);
                }
                Err(e) => errors.push(pi.into_response_sync(SyncResultMulti {
                    src: src_ref,
                    dst: dst_ref,
                    result: Err(e),
                })),
            }
        }

        if !errors.is_empty() {
            return Ok(errors);
        }

        let sync_results = self
            .sync_svc
            .sync_multi(sync_requests, options.sync_options.clone(), listener)
            .await;

        assert_eq!(batch_rest.len(), sync_results.len());

        let mut results = Vec::new();
        for (pi, res) in std::iter::zip(batch_rest, sync_results) {
            assert_eq!(pi.local_ref.as_any_ref(), res.dst);

            // Associate newly-synced datasets with remotes
            if options.add_aliases {
                if let Ok(SyncResponse {
                    result: SyncResult::Updated { old_head: None, .. },
                    local_dataset,
                }) = &res.result
                {
                    if let Some(remote_ref) = &pi.remote_ref {
                        self.remote_alias_reg
                            .get_remote_aliases(local_dataset.clone())
                            .await
                            .int_err()?
                            .add(remote_ref, RemoteAliasKind::Pull)
                            .await?;
                    }
                }
            }

            results.push(pi.into_response_sync(res));
        }

        Ok(results)
    }

    async fn transform_multi(
        &self,
        batch: Vec<PullItem>,
        transform_listener: Option<Arc<dyn TransformMultiListener>>,
        reset_derivatives_on_diverged_input: bool,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let mut transform_requests = Vec::new();
        for item in &batch {
            let hdl = self
                .dataset_registry
                .resolve_dataset_handle_by_ref(&item.local_ref)
                .await
                .int_err()?;
            let dataset = self.dataset_registry.get_dataset_by_handle(&hdl);
            transform_requests.push(ResolvedDataset::new(dataset, hdl));
        }

        let transform_results = self
            .transform_svc
            .transform_multi(
                transform_requests,
                TransformOptions {
                    reset_derivatives_on_diverged_input,
                },
                transform_listener,
            )
            .await;

        assert_eq!(batch.len(), transform_results.len());

        Ok(std::iter::zip(batch, transform_results)
            .map(|(pi, res)| {
                assert_eq!(pi.local_ref, res.0);
                pi.into_response_transform(res)
            })
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PullDatasetUseCase for PullDatasetUseCaseImpl {
    async fn execute(
        &self,
        request: PullRequest,
        options: PullOptions,
        listener: Option<Arc<dyn PullListener>>,
    ) -> PullResponse {
        let listener =
            listener.map(|l| Arc::new(ListenerMultiAdapter(l)) as Arc<dyn PullMultiListener>);

        // TODO: PERF: If we are updating a single dataset using pull_multi will do A
        // LOT of unnecessary work like analyzing the whole dependency graph.
        let mut responses = self
            .execute_multi(
                vec![request],
                PullMultiOptions {
                    recursive: false,
                    reset_derivatives_on_diverged_input: options
                        .reset_derivatives_on_diverged_input,
                    add_aliases: options.add_aliases,
                    ingest_options: options.ingest_options,
                    sync_options: options.sync_options,
                },
                listener,
            )
            .await;

        assert_eq!(responses.len(), 1);
        responses.pop().unwrap()
    }

    async fn execute_multi(
        &self,
        requests: Vec<PullRequest>,
        options: PullMultiOptions,
        listener: Option<Arc<dyn PullMultiListener>>,
    ) -> Vec<PullResponse> {
        tracing::info!(?requests, ?options, "Performing pull");

        // TODO:
        //  - recursive complex planning may be skipped if there is just 1 dataset, and
        //    no recursive/all flags
        let (mut plan, errors) = self
            .pull_request_planner
            .collect_pull_graph(&requests, &options, self.in_multi_tenant_mode)
            .await;
        tracing::info!(
            num_items = plan.len(),
            num_errors = errors.len(),
            ?plan,
            "Resolved pull plan"
        );
        if !errors.is_empty() {
            return errors;
        }

        if !options.recursive {
            // Leave only datasets explicitly mentioned, preserving the depth order
            plan.retain(|pi| pi.original_request.is_some());
        }

        tracing::info!(num_items = plan.len(), ?plan, "Retained pull plan");
        let mut results = Vec::with_capacity(plan.len());

        let execution_steps = self.pull_request_planner.prepare_pull_execution_steps(plan);
        tracing::info!(
            num_steps = execution_steps.len(),
            "Prepared pull execution plan"
        );

        for execution_step in execution_steps {
            let results_level: Vec<_> = match execution_step.kind {
                // Ingestion
                PullExecutionStepKind::Ingest => {
                    tracing::info!(depth = %execution_step.depth, batch = ?execution_step.batch, "Running ingest batch");
                    self.ingest_multi(
                        execution_step.batch,
                        &options,
                        listener
                            .as_ref()
                            .and_then(|l| l.clone().get_ingest_listener()),
                    )
                    .await
                    .unwrap() // TODO
                }
                // Sync
                PullExecutionStepKind::Sync => {
                    tracing::info!(depth = %execution_step.depth, batch = ?execution_step.batch, "Running sync batch");
                    self.sync_multi(
                        execution_step.batch,
                        &options,
                        listener
                            .as_ref()
                            .and_then(|l| l.clone().get_sync_listener()),
                    )
                    .await
                    .unwrap() // TODO
                }
                // Transform
                PullExecutionStepKind::Transform => {
                    tracing::info!(depth = %execution_step.depth, batch = ?execution_step.batch, "Running transform batch");
                    self.transform_multi(
                        execution_step.batch,
                        listener
                            .as_ref()
                            .and_then(|l| l.clone().get_transform_listener()),
                        options.reset_derivatives_on_diverged_input,
                    )
                    .await
                    .unwrap() // TODO
                }
            };

            let errors = results_level.iter().any(|r| r.result.is_err());
            results.extend(results_level);
            if errors {
                break;
            }
        }

        results
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
    fn begin_ingest(&self, _dataset: &DatasetHandle) -> Option<Arc<dyn PollingIngestListener>> {
        self.0.clone().get_ingest_listener()
    }
}

impl TransformMultiListener for ListenerMultiAdapter {
    fn begin_transform(&self, _dataset: &DatasetHandle) -> Option<Arc<dyn TransformListener>> {
        self.0.clone().get_transform_listener()
    }
}

impl SyncMultiListener for ListenerMultiAdapter {
    fn begin_sync(
        &self,
        _src: &DatasetRefAny,
        _dst: &DatasetRefAny,
    ) -> Option<Arc<dyn SyncListener>> {
        self.0.clone().get_sync_listener()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
