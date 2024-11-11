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
    tenancy_config: Arc<TenancyConfig>,
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
        tenancy_config: Arc<TenancyConfig>,
    ) -> Self {
        Self {
            pull_request_planner,
            dataset_action_authorizer,
            remote_alias_registry,
            polling_ingest_svc,
            transform_elaboration_svc,
            transform_execution_svc,
            sync_svc,
            tenancy_config,
        }
    }

    async fn make_authorization_checks<TPullItem: PullItemCommon>(
        batch: Vec<TPullItem>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
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
        } = dataset_action_authorizer
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

    async fn ingest(
        pii: PullIngestItem,
        ingest_options: PollingIngestOptions,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        polling_ingest_svc: Arc<dyn PollingIngestService>,
        maybe_multi_listener: Option<Arc<dyn PollingIngestMultiListener>>,
    ) -> Result<PullResponse, InternalError> {
        // Authorization checks
        let (mut batch, mut errors) =
            Self::make_authorization_checks(vec![pii], dataset_action_authorizer, |auth_error| {
                PullError::PollingIngestError(match auth_error {
                    DatasetActionUnauthorizedError::Access(e) => PollingIngestError::Access(e),
                    DatasetActionUnauthorizedError::Internal(e) => PollingIngestError::Internal(e),
                })
            })
            .await?;
        if !errors.is_empty() {
            assert_eq!(errors.len(), 1);
            return Ok(errors.remove(0));
        }

        assert_eq!(batch.len(), 1);
        let pii = batch.remove(0);

        let multi_listener =
            maybe_multi_listener.unwrap_or_else(|| Arc::new(NullPollingIngestMultiListener));

        let ingest_response = polling_ingest_svc
            .ingest(
                pii.target.clone(),
                ingest_options,
                multi_listener.begin_ingest(&pii.target.handle),
            )
            .await;

        Ok(PullResponse {
            maybe_original_request: pii.maybe_original_request,
            maybe_local_ref: Some(pii.target.handle.as_local_ref()),
            maybe_remote_ref: None,
            result: match ingest_response {
                Ok(r) => Ok(r.into()),
                Err(e) => Err(e.into()),
            },
        })
    }

    async fn transform(
        pti: PullTransformItem,
        transform_options: TransformOptions,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        transform_elaboration_svc: Arc<dyn TransformElaborationService>,
        transform_execution_svc: Arc<dyn TransformExecutionService>,
        maybe_transform_multi_listener: Option<Arc<dyn TransformMultiListener>>,
    ) -> Result<PullResponse, InternalError> {
        // Authorization checks
        // TODO: checked targets for write, but should we check inputs for Read?
        let (mut batch, mut errors) =
            Self::make_authorization_checks(vec![pti], dataset_action_authorizer, |auth_error| {
                PullError::TransformError(match auth_error {
                    DatasetActionUnauthorizedError::Access(e) => TransformError::Access(e),
                    DatasetActionUnauthorizedError::Internal(e) => TransformError::Internal(e),
                })
            })
            .await?;
        if !errors.is_empty() {
            assert_eq!(errors.len(), 1);
            return Ok(errors.remove(0));
        }

        assert_eq!(batch.len(), 1);
        let pti = batch.remove(0);

        let transform_multi_listener =
            maybe_transform_multi_listener.unwrap_or_else(|| Arc::new(NullTransformMultiListener));

        // Remember original request
        let maybe_original_request = pti.maybe_original_request.clone();

        // Main transform run
        async fn run_transform(
            pti: PullTransformItem,
            transform_elaboration_svc: Arc<dyn TransformElaborationService>,
            transform_execution_svc: Arc<dyn TransformExecutionService>,
            transform_options: TransformOptions,
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
                    (
                        target,
                        result.map_err(|e| PullError::TransformError(TransformError::Execute(e))),
                    )
                }
                // Already up-to-date
                Ok(TransformElaboration::UpToDate) => (pti.target, Ok(TransformResult::UpToDate)),
                // Elab error
                Err(e) => (
                    pti.target,
                    Err(PullError::TransformError(TransformError::Elaborate(e))),
                ),
            }
        }

        let transform_result = run_transform(
            pti,
            transform_elaboration_svc,
            transform_execution_svc,
            transform_options,
            transform_multi_listener.clone(),
        )
        .await;

        Ok(PullResponse {
            maybe_original_request,
            maybe_local_ref: Some(transform_result.0.handle.as_local_ref()),
            maybe_remote_ref: None,
            result: transform_result.1.map(Into::into),
        })
    }

    async fn sync(
        psi: PullSyncItem,
        options: PullOptions,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        sync_svc: Arc<dyn SyncService>,
        remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<PullResponse, InternalError> {
        // Authorization checks
        let (mut batch, mut errors) =
            Self::make_authorization_checks(vec![psi], dataset_action_authorizer, |auth_error| {
                PullError::SyncError(match auth_error {
                    DatasetActionUnauthorizedError::Access(e) => SyncError::Access(e),
                    DatasetActionUnauthorizedError::Internal(e) => SyncError::Internal(e),
                })
            })
            .await?;
        if !errors.is_empty() {
            assert_eq!(errors.len(), 1);
            return Ok(errors.remove(0));
        }

        assert_eq!(batch.len(), 1);
        let psi = batch.remove(0);

        let listener = listener.as_ref().and_then(|l| {
            l.begin_sync(&psi.sync_request.src.src_ref, &psi.sync_request.dst.dst_ref)
        });

        let mut sync_result = sync_svc
            .sync(*psi.sync_request, options.sync_options, listener)
            .await;

        // Associate newly-synced datasets with remotes
        if options.add_aliases
            && let Ok((SyncResult::Updated { old_head: None, .. }, local_dataset)) = &sync_result
        {
            let alias_add_result = match remote_alias_registry
                .get_remote_aliases(local_dataset.clone())
                .await
            {
                Ok(mut aliases) => aliases.add(&psi.remote_ref, RemoteAliasKind::Pull).await,
                Err(e) => match e {
                    GetAliasesError::Internal(e) => Err(e),
                },
            };

            if let Err(e) = alias_add_result {
                sync_result = Err(SyncError::Internal(e));
            }
        }

        Ok(PullResponse {
            maybe_original_request: psi.maybe_original_request,
            maybe_local_ref: Some(psi.local_target.as_local_ref()), // TODO: multi-tenancy
            maybe_remote_ref: Some(psi.remote_ref),
            result: match sync_result {
                Ok(response) => Ok(response.0.into()),
                Err(e) => Err(e.into()),
            },
        })
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
            .build_pull_multi_plan(&requests, &options, *self.tenancy_config)
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
            tracing::info!(depth = %iteration.depth, jobs = ?iteration.jobs, "Running pull iteration");

            let mut tasks = tokio::task::JoinSet::new();
            for job in iteration.jobs {
                match job {
                    PullPlanIterationJob::Ingest(pii) => tasks.spawn(Self::ingest(
                        pii,
                        options.ingest_options.clone(),
                        self.dataset_action_authorizer.clone(),
                        self.polling_ingest_svc.clone(),
                        listener
                            .as_ref()
                            .and_then(|l| l.clone().get_ingest_listener()),
                    )),
                    PullPlanIterationJob::Transform(pti) => tasks.spawn(Self::transform(
                        pti,
                        options.transform_options,
                        self.dataset_action_authorizer.clone(),
                        self.transform_elaboration_svc.clone(),
                        self.transform_execution_svc.clone(),
                        listener
                            .as_ref()
                            .and_then(|l| l.clone().get_transform_listener()),
                    )),
                    PullPlanIterationJob::Sync(psi) => tasks.spawn(Self::sync(
                        psi,
                        options.clone(),
                        self.dataset_action_authorizer.clone(),
                        self.sync_svc.clone(),
                        self.remote_alias_registry.clone(),
                        listener
                            .as_ref()
                            .and_then(|l| l.clone().get_sync_listener()),
                    )),
                };
            }

            let iteration_results = tasks.join_all().await;
            tracing::debug!(iteration_result=?iteration_results, "Pull iteration finished");

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
