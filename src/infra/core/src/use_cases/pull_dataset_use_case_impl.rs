// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use dill::*;
use internal_error::InternalError;
use kamu_core::auth::{
    ClassifyByAllowanceResponse,
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetActionUnauthorizedError,
};
use kamu_core::*;
use opendatafabric::{DatasetHandle, DatasetRefAny};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn PullDatasetUseCase)]
pub struct PullDatasetUseCaseImpl {
    pull_request_planner: Arc<dyn PullRequestPlanner>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
    polling_ingest_svc: Arc<dyn PollingIngestService>,
    transform_elaboration_svc: Arc<dyn TransformElaborationService>,
    transform_execution_svc: Arc<dyn TransformExecutionService>,
    sync_svc: Arc<dyn SyncService>,
    in_multi_tenant_mode: bool,
}

impl PullDatasetUseCaseImpl {
    pub fn new(
        pull_request_planner: Arc<dyn PullRequestPlanner>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
        polling_ingest_svc: Arc<dyn PollingIngestService>,
        transform_elaboration_svc: Arc<dyn TransformElaborationService>,
        transform_execution_svc: Arc<dyn TransformExecutionService>,
        sync_svc: Arc<dyn SyncService>,
        in_multi_tenant_mode: bool,
    ) -> Self {
        Self {
            pull_request_planner,
            dataset_action_authorizer,
            remote_alias_registry,
            polling_ingest_svc,
            transform_elaboration_svc,
            transform_execution_svc,
            sync_svc,
            in_multi_tenant_mode,
        }
    }

    async fn make_authorization_checks<TPullItem: PullItemCommon>(
        &self,
        batch: Vec<TPullItem>,
        error_conversion_callback: impl Fn(DatasetActionUnauthorizedError) -> PullError,
    ) -> Result<(Vec<TPullItem>, Vec<PullResponse>), InternalError> {
        let (existing_handle_items, other_items): (Vec<_>, Vec<_>) = batch
            .into_iter()
            .partition(|item| item.try_get_handle().is_some());

        let dataset_handles = existing_handle_items
            .iter()
            .map(|item| item.try_get_handle().expect("handle must exist").clone())
            .collect();

        let ClassifyByAllowanceResponse {
            authorized_handles,
            unauthorized_handles_with_errors,
        } = self
            .dataset_action_authorizer
            .classify_datasets_by_allowance(dataset_handles, DatasetAction::Write)
            .await?;

        if unauthorized_handles_with_errors.is_empty() {
            let mut batch = Vec::with_capacity(existing_handle_items.len() + other_items.len());
            batch.extend(existing_handle_items);
            batch.extend(other_items);
            return Ok((batch, vec![]));
        }

        let mut items_by_handle = HashMap::new();
        for item in existing_handle_items {
            items_by_handle.insert(
                item.try_get_handle().expect("handle must exist").clone(),
                item,
            );
        }

        let unauthorized_responses = unauthorized_handles_with_errors
            .into_iter()
            .map(|(hdl, auth_error)| {
                let item = items_by_handle.remove(&hdl).expect("item must be present");
                PullResponse {
                    maybe_local_ref: Some(hdl.as_local_ref()),
                    maybe_remote_ref: None,
                    maybe_original_request: item.into_original_pull_request(),
                    result: Err(error_conversion_callback(auth_error)),
                }
            })
            .collect();

        let mut authorized_items = Vec::with_capacity(authorized_handles.len() + other_items.len());
        authorized_handles
            .iter()
            .map(|hdl| items_by_handle.remove(hdl).expect("item must be present"))
            .collect_into(&mut authorized_items);
        authorized_items.extend(other_items);

        Ok((authorized_items, unauthorized_responses))
    }

    async fn ingest_multi(
        &self,
        batch: Vec<PullIngestItem>,
        options: &PullOptions,
        listener: Option<Arc<dyn PollingIngestMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        // Authorization checks
        let (batch, errors) = self
            .make_authorization_checks(batch, |auth_error| {
                PullError::PollingIngestError(match auth_error {
                    DatasetActionUnauthorizedError::Access(e) => PollingIngestError::Access(e),
                    DatasetActionUnauthorizedError::Internal(e) => PollingIngestError::Internal(e),
                })
            })
            .await?;
        if !errors.is_empty() {
            return Ok(errors);
        }

        // Main ingestion run
        let ingestion_requests = batch.iter().map(|pui| pui.target.clone()).collect();

        let ingest_responses = self
            .polling_ingest_svc
            .ingest_multi(ingestion_requests, options.ingest_options.clone(), listener)
            .await;

        // Convert ingest results into pull results
        tracing::debug!(batch=?batch, ingest_responses=?ingest_responses, "Ingest results");
        assert_eq!(batch.len(), ingest_responses.len());
        Ok(std::iter::zip(batch, ingest_responses)
            .map(|(pui, res)| {
                assert_eq!(pui.target.handle.as_local_ref(), res.dataset_ref);
                pui.into_response_ingest(res)
            })
            .collect())
    }

    async fn transform_multi(
        &self,
        batch: Vec<PullTransformItem>,
        transform_options: &TransformOptions,
        maybe_transform_multi_listener: Option<Arc<dyn TransformMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        // Authorization checks
        // TODO: checked targets for write, but should we check inputs for Read?
        let (batch, errors) = self
            .make_authorization_checks(batch, |auth_error| {
                PullError::TransformError(match auth_error {
                    DatasetActionUnauthorizedError::Access(e) => TransformError::Access(e),
                    DatasetActionUnauthorizedError::Internal(e) => TransformError::Internal(e),
                })
            })
            .await?;
        if !errors.is_empty() {
            return Ok(errors);
        }

        let transform_multi_listener =
            maybe_transform_multi_listener.unwrap_or_else(|| Arc::new(NullTransformMultiListener));

        // Remember original requests
        let original_requests: Vec<_> = batch
            .iter()
            .map(|pti| pti.maybe_original_request.clone())
            .collect();

        // Main transform run
        async fn run_transform(
            pti: PullTransformItem,
            transform_elaboration_svc: Arc<dyn TransformElaborationService>,
            transform_execution_svc: Arc<dyn TransformExecutionService>,
            transform_options: &TransformOptions,
            transform_multi_listener: Arc<dyn TransformMultiListener>,
        ) -> (ResolvedDataset, Result<TransformResult, PullError>) {
            // Notify listener
            let maybe_listener = transform_multi_listener.begin_transform(&pti.target.handle);

            // Elaborate phase
            match transform_elaboration_svc
                .elaborate_transform(
                    pti.target.clone(),
                    pti.plan,
                    transform_options,
                    maybe_listener.clone(),
                )
                .await
            {
                // Elaborate succeess
                Ok(TransformElaboration::Elaborated(plan)) => {
                    // Execute phase
                    let (target, result) = transform_execution_svc
                        .execute_transform(pti.target, plan, maybe_listener)
                        .await;
                    (target, result.map_err(PullError::TransformExecuteError))
                }
                // Already up-to-date
                Ok(TransformElaboration::UpToDate) => (pti.target, Ok(TransformResult::UpToDate)),
                // Elab error
                Err(e) => (pti.target, Err(PullError::TransformElaborateError(e))),
            }
        }

        // Run transforms concurrently
        let futures: Vec<_> = batch
            .into_iter()
            .map(|pti| {
                run_transform(
                    pti,
                    self.transform_elaboration_svc.clone(),
                    self.transform_execution_svc.clone(),
                    transform_options,
                    transform_multi_listener.clone(),
                )
            })
            .collect();
        let transform_results = futures::future::join_all(futures).await;

        // Convert transform results to pull results
        tracing::debug!(original_requests=?original_requests, transform_results=?transform_results, "Transform results");
        assert_eq!(original_requests.len(), transform_results.len());
        Ok(std::iter::zip(original_requests, transform_results)
            .map(|(maybe_original_request, (target, result))| PullResponse {
                maybe_original_request,
                maybe_local_ref: Some(target.handle.as_local_ref()),
                maybe_remote_ref: None,
                result: result.map(Into::into),
            })
            .collect())
    }

    async fn sync_multi(
        &self,
        batch: Vec<PullSyncItem>,
        sync_requests: Vec<SyncRequest>,
        options: &PullOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        // Authorization checks
        let (batch, errors) = self
            .make_authorization_checks(batch, |auth_error| {
                PullError::SyncError(match auth_error {
                    DatasetActionUnauthorizedError::Access(e) => SyncError::Access(e),
                    DatasetActionUnauthorizedError::Internal(e) => SyncError::Internal(e),
                })
            })
            .await?;
        if !errors.is_empty() {
            return Ok(errors);
        }

        let sync_results = self
            .sync_svc
            .sync_multi(sync_requests, options.sync_options.clone(), listener)
            .await;

        assert_eq!(batch.len(), sync_results.len());

        let mut results = Vec::new();
        for (psi, mut res) in std::iter::zip(batch, sync_results) {
            assert_eq!(psi.local_target.as_any_ref(), res.dst);

            // Associate newly-synced datasets with remotes
            if options.add_aliases
                && let Ok(SyncResponse {
                    result: SyncResult::Updated { old_head: None, .. },
                    local_dataset,
                }) = &res.result
            {
                let alias_add_result = match self
                    .remote_alias_registry
                    .get_remote_aliases(local_dataset.clone())
                    .await
                {
                    Ok(mut aliases) => aliases.add(&psi.remote_ref, RemoteAliasKind::Pull).await,
                    Err(e) => match e {
                        GetAliasesError::Internal(e) => Err(e),
                    },
                };

                if let Err(e) = alias_add_result {
                    res.result = Err(SyncError::Internal(e));
                }
            }

            results.push(psi.into_response_sync(res));
        }

        tracing::debug!(results=?results, "Sync results");
        Ok(results)
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
    ) -> Result<PullResponse, InternalError> {
        let listener =
            listener.map(|l| Arc::new(ListenerMultiAdapter(l)) as Arc<dyn PullMultiListener>);

        let mut responses = self.execute_multi(vec![request], options, listener).await?;

        assert_eq!(responses.len(), 1);
        Ok(responses.pop().unwrap())
    }

    async fn execute_multi(
        &self,
        requests: Vec<PullRequest>,
        options: PullOptions,
        listener: Option<Arc<dyn PullMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        tracing::info!(?requests, ?options, "Performing pull");

        let (plan, errors) = self
            .pull_request_planner
            .build_pull_multi_plan(&requests, &options, self.in_multi_tenant_mode)
            .await;

        tracing::info!(
            num_steps = plan.len(),
            num_errors = errors.len(),
            "Prepared pull execution plan"
        );
        if !errors.is_empty() {
            return Ok(errors);
        }

        let mut results = Vec::with_capacity(plan.len());

        for iteration in plan {
            let iteration_results: Vec<_> = match iteration.job {
                PullPlanIterationJob::Ingest(ingest_batch) => {
                    tracing::info!(depth = %iteration.depth, batch = ?ingest_batch, "Running ingest batch");
                    self.ingest_multi(
                        ingest_batch,
                        &options,
                        listener
                            .as_ref()
                            .and_then(|l| l.clone().get_ingest_listener()),
                    )
                    .await?
                }

                PullPlanIterationJob::Sync((sync_batch, sync_requests)) => {
                    tracing::info!(depth = %iteration.depth, batch = ?sync_batch, "Running sync batch");
                    self.sync_multi(
                        sync_batch,
                        sync_requests,
                        &options,
                        listener
                            .as_ref()
                            .and_then(|l| l.clone().get_sync_listener()),
                    )
                    .await?
                }

                PullPlanIterationJob::Transform(transform_batch) => {
                    tracing::info!(depth = %iteration.depth, batch = ?transform_batch, "Running transform batch");
                    self.transform_multi(
                        transform_batch,
                        &options.transform_options,
                        listener
                            .as_ref()
                            .and_then(|l| l.clone().get_transform_listener()),
                    )
                    .await?
                }
            };

            let errors = iteration_results.iter().any(|r| r.result.is_err());
            results.extend(iteration_results);
            if errors {
                break;
            }
        }

        Ok(results)
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
