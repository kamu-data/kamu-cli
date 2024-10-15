// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use internal_error::InternalError;
use kamu_core::auth::{
    ClassifyByAllowanceResponse,
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetActionUnauthorizedError,
};
use kamu_core::{
    DatasetRegistry,
    DatasetRegistryExt,
    PushDatasetUseCase,
    PushError,
    PushMultiOptions,
    PushResponse,
    RemoteAliasKind,
    RemoteAliasResolver,
    RemoteAliasesRegistry,
    RemoteTarget,
    SyncError,
    SyncMultiListener,
    SyncOptions,
    SyncRequestNew,
    SyncResult,
    SyncService,
};
use opendatafabric::{DatasetHandle, DatasetPushTarget, DatasetRefAny};

use crate::SyncRequestBuilder;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn PushDatasetUseCase)]
pub struct PushDatasetUseCaseImpl {
    //push_service: Arc<dyn PushService>,
    sync_request_builder: Arc<SyncRequestBuilder>,
    sync_service: Arc<dyn SyncService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    remote_alias_resolver: Arc<dyn RemoteAliasResolver>,
    remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
}

impl PushDatasetUseCaseImpl {
    pub fn new(
        //push_service: Arc<dyn PushService>,
        sync_request_builder: Arc<SyncRequestBuilder>,
        sync_service: Arc<dyn SyncService>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        remote_alias_resolver: Arc<dyn RemoteAliasResolver>,
        remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
    ) -> Self {
        Self {
            // push_service,
            sync_request_builder,
            sync_service,
            dataset_registry,
            dataset_action_authorizer,
            remote_alias_resolver,
            remote_alias_registry,
        }
    }

    async fn make_authorization_checks(
        &self,
        dataset_handles: Vec<DatasetHandle>,
        push_target: Option<&DatasetPushTarget>,
    ) -> Result<(Vec<DatasetHandle>, Vec<PushResponse>), InternalError> {
        let ClassifyByAllowanceResponse {
            authorized_handles,
            unauthorized_handles_with_errors,
        } = self
            .dataset_action_authorizer
            .classify_datasets_by_allowance(dataset_handles, DatasetAction::Read)
            .await?;

        let unauthorized_responses = unauthorized_handles_with_errors
            .into_iter()
            .map(|(hdl, error)| PushResponse {
                local_handle: Some(hdl),
                target: push_target.cloned(),
                result: Err(PushError::SyncError(match error {
                    DatasetActionUnauthorizedError::Access(e) => SyncError::Access(e),
                    DatasetActionUnauthorizedError::Internal(e) => SyncError::Internal(e),
                })),
            })
            .collect();

        Ok((authorized_handles, unauthorized_responses))
    }

    async fn collect_push_plan(
        &self,
        dataset_handles: &[DatasetHandle],
        push_target: Option<&DatasetPushTarget>,
    ) -> (Vec<PushPlanItem>, Vec<PushResponse>) {
        let mut plan = Vec::new();
        let mut errors = Vec::new();

        for hdl in dataset_handles {
            match self.collect_push_plan_item(hdl.clone(), push_target).await {
                Ok(item) => plan.push(item),
                Err(err) => errors.push(err),
            }
        }

        (plan, errors)
    }

    async fn collect_push_plan_item(
        &self,
        local_handle: DatasetHandle,
        push_target: Option<&DatasetPushTarget>,
    ) -> Result<PushPlanItem, PushResponse> {
        let local_target = self
            .dataset_registry
            .get_resolved_dataset_by_handle(&local_handle);

        match self
            .remote_alias_resolver
            .resolve_push_target(local_target, push_target.cloned())
            .await
        {
            Ok(remote_target) => Ok(PushPlanItem {
                local_handle,
                remote_target,
                push_target: push_target.cloned(),
            }),
            Err(e) => Err(PushResponse {
                local_handle: Some(local_handle),
                target: push_target.cloned(),
                result: Err(e.into()),
            }),
        }
    }

    async fn build_sync_requests(
        &self,
        plan: &[PushPlanItem],
        sync_options: &SyncOptions,
        push_target: Option<&DatasetPushTarget>,
    ) -> (Vec<SyncRequestNew>, Vec<PushResponse>) {
        let mut sync_requests = Vec::new();
        let mut errors = Vec::new();

        for pi in plan {
            let src_ref = pi.local_handle.as_any_ref();
            let dst_ref = (&pi.remote_target.url).into();
            match self
                .sync_request_builder
                .build_sync_request(src_ref, dst_ref, sync_options.create_if_not_exists)
                .await
            {
                Ok(sync_request) => sync_requests.push(sync_request),
                Err(e) => errors.push(PushResponse {
                    local_handle: Some(pi.local_handle.clone()),
                    target: push_target.cloned(),
                    result: Err(e.into()),
                }),
            }
        }

        (sync_requests, errors)
    }
}

#[async_trait::async_trait]
impl PushDatasetUseCase for PushDatasetUseCaseImpl {
    async fn execute_multi(
        &self,
        dataset_handles: Vec<DatasetHandle>,
        options: PushMultiOptions,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<Vec<PushResponse>, InternalError> {
        // Check for unsupported options first
        if options.recursive {
            unimplemented!("Recursive push is not yet supported")
        }
        if options.all {
            unimplemented!("Pushing all datasets is not yet supported")
        }

        // Authorization checks upon all datasets come first
        let (authorized_handles, unauthorized_responses) = self
            .make_authorization_checks(dataset_handles, options.remote_target.as_ref())
            .await?;
        if !unauthorized_responses.is_empty() {
            return Ok(unauthorized_responses);
        }

        // Prepare a push plan
        let (plan, errors) = self
            .collect_push_plan(&authorized_handles, options.remote_target.as_ref())
            .await;
        if !errors.is_empty() {
            return Ok(errors);
        }

        // Create sync requests
        let (sync_requests, errors) = self
            .build_sync_requests(&plan, &options.sync_options, options.remote_target.as_ref())
            .await;
        if !errors.is_empty() {
            return Ok(errors);
        }

        // Run sync process
        let sync_results = self
            .sync_service
            .sync_multi_new(sync_requests, options.sync_options, sync_listener)
            .await;

        // Convert results
        assert_eq!(plan.len(), sync_results.len());
        let results: Vec<_> = std::iter::zip(&plan, sync_results)
            .map(|(pi, res)| {
                let remote_ref: DatasetRefAny = (&pi.remote_target.url).into();
                assert_eq!(pi.local_handle.as_any_ref(), res.src);
                assert_eq!(remote_ref, res.dst);
                pi.as_response(res.result)
            })
            .collect();

        // If no errors - add aliases to initial items
        if options.add_aliases && results.iter().all(|r| r.result.is_ok()) {
            for push_item in &plan {
                // TODO: Improve error handling
                let dataset = self
                    .dataset_registry
                    .get_dataset_by_handle(&push_item.local_handle);
                self.remote_alias_registry
                    .get_remote_aliases(dataset)
                    .await
                    .unwrap()
                    .add(
                        &((&push_item.remote_target.url).into()),
                        RemoteAliasKind::Push,
                    )
                    .await
                    .unwrap();
            }
        }

        Ok(results)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct PushPlanItem {
    local_handle: DatasetHandle,
    remote_target: RemoteTarget,
    push_target: Option<DatasetPushTarget>,
}

impl PushPlanItem {
    fn as_response(&self, result: Result<SyncResult, SyncError>) -> PushResponse {
        PushResponse {
            local_handle: Some(self.local_handle.clone()),
            target: self.push_target.clone(),
            result: result.map_err(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
