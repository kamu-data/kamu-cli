// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::{transactional_method1, transactional_method2};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_accounts::Principal;
use kamu_core::{
    PushDatasetUseCase,
    PushError,
    PushItem,
    PushMultiOptions,
    PushRequestPlanner,
    PushResponse,
    RemoteAliasKind,
    RemoteAliasesRegistry,
    SyncError,
    SyncListener,
    SyncMultiListener,
    SyncOptions,
    SyncRequest,
    SyncResult,
    SyncService,
};
use kamu_datasets::{
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetPermissionError,
};

use crate::SyncRequestBuilder;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn PushDatasetUseCase)]
pub struct PushDatasetUseCaseImpl {
    catalog: dill::Catalog,
    sync_service: Arc<dyn SyncService>,
}

#[common_macros::method_names_consts]
impl PushDatasetUseCaseImpl {
    #[transactional_method2(dataset_operation_guard: Arc<dyn DatasetOperationGuard>, principal: Arc<Principal>)]
    #[tracing::instrument(level = "debug", name = PushDatasetUseCaseImpl_make_authorization_checks, skip_all, fields(num_datasets = dataset_handles.len(), ?push_target))]
    async fn make_authorization_checks(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        push_target: Option<&odf::DatasetPushTarget>,
    ) -> Result<(Vec<odf::DatasetHandle>, Vec<PushResponse>), InternalError> {
        let requests: Vec<_> = dataset_handles
            .into_iter()
            .map(|hdl| DatasetOperationRequest::new(hdl, DatasetOperation::SyncTo))
            .collect();
        let verdicts = dataset_operation_guard
            .check_operations(&principal, &requests)
            .await?;

        let mut authorized_handles = Vec::with_capacity(requests.len());
        let mut unauthorized_responses = Vec::new();
        for (request, verdict) in requests.into_iter().zip(verdicts) {
            match verdict {
                Ok(()) => authorized_handles.push(request.target),
                Err(error) => unauthorized_responses.push(PushResponse {
                    local_handle: Some(request.target),
                    target: push_target.cloned(),
                    result: Err(PushError::SyncError({
                        use DatasetPermissionError as E;

                        match error {
                            E::NotFound(e) => SyncError::DatasetNotFound(e.into()),
                            E::Access(e) => SyncError::Access(e),
                            E::Internal(e) => SyncError::Internal(e),
                        }
                    })),
                }),
            }
        }

        Ok((authorized_handles, unauthorized_responses))
    }

    /// Pairs each sync request with the index of its plan item
    #[transactional_method1(sync_request_builder: Arc<SyncRequestBuilder>)]
    #[tracing::instrument(
        level = "debug",
        name = PushDatasetUseCaseImpl_build_sync_requests,
        skip_all,
        fields(num_items = plan.len(), ?sync_options, ?push_target)
    )]
    async fn build_sync_requests(
        &self,
        plan: &[PushItem],
        sync_options: SyncOptions,
        push_target: Option<&odf::DatasetPushTarget>,
    ) -> Result<(Vec<(usize, SyncRequest)>, Vec<PushResponse>), InternalError> {
        let mut sync_requests = Vec::new();
        let mut errors = Vec::new();

        for (index, pi) in plan.iter().enumerate() {
            let src_ref = pi.local_handle.as_local_ref();
            let dst_ref = (&pi.remote_target.url).into();
            match sync_request_builder
                .build_sync_request(src_ref, dst_ref, sync_options.create_if_not_exists)
                .await
            {
                Ok(sync_request) => {
                    sync_request.src.detach_from_transaction();
                    sync_requests.push((index, sync_request));
                }
                Err(e) => errors.push(PushResponse {
                    local_handle: Some(pi.local_handle.clone()),
                    target: push_target.cloned(),
                    result: Err(e.into()),
                }),
            }
        }

        Ok((sync_requests, errors))
    }

    #[transactional_method1(remote_alias_registry: Arc<dyn RemoteAliasesRegistry>)]
    async fn save_dataset_alias_transactional(
        &self,
        local_handle: &odf::DatasetHandle,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<bool, InternalError> {
        remote_alias_registry
            .get_remote_aliases(local_handle)
            .await?
            .add(remote_ref, RemoteAliasKind::Push)
            .await
    }

    #[transactional_method1(push_request_planner: Arc<dyn PushRequestPlanner>)]
    async fn prepare_push_plan_transactional(
        &self,
        authorized_handles: &[odf::DatasetHandle],
        remote_target: Option<&odf::DatasetPushTarget>,
    ) -> Result<(Vec<PushItem>, Vec<PushResponse>), InternalError> {
        Ok(push_request_planner
            .collect_plan(authorized_handles, remote_target)
            .await)
    }

    /// Syncs, then reports the result to the listener: a push to a remote
    /// commits there, so the result is final when the sync returns
    async fn sync_and_report(
        &self,
        sync_request: SyncRequest,
        sync_options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let result = self
            .sync_service
            .sync(sync_request, sync_options, listener.clone())
            .await;
        if let Some(listener) = &listener {
            match &result {
                Ok(sync_result) => listener.success(sync_result),
                Err(e) => listener.error(e),
            }
        }
        result
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl PushDatasetUseCase for PushDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = PushDatasetUseCaseImpl_execute_multi,
        skip_all,
        fields(num_datasets = dataset_handles.len(), ?options)
    )]
    async fn execute_multi(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
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

        // A dataset that fails a step is reported and the others go on
        let mut responses = Vec::new();

        // Authorization checks upon all datasets come first
        let (authorized_handles, unauthorized_responses) = self
            .make_authorization_checks(dataset_handles, options.remote_target.as_ref())
            .await?;
        responses.extend(unauthorized_responses);

        // Prepare a push plan
        let (plan, errors) = self
            .prepare_push_plan_transactional(
                authorized_handles.as_slice(),
                options.remote_target.as_ref(),
            )
            .await?;
        responses.extend(errors);

        tracing::debug!(?plan, "Obtained push plan");

        // Create sync requests
        let (sync_requests, errors) = self
            .build_sync_requests(&plan, options.sync_options, options.remote_target.as_ref())
            .await?;
        responses.extend(errors);

        // Run sync process
        let (plan_indices, sync_requests): (Vec<_>, Vec<_>) = sync_requests.into_iter().unzip();
        let futures: Vec<_> = sync_requests
            .into_iter()
            .map(|sync_request| {
                let listener = sync_listener.as_ref().and_then(|l| {
                    l.begin_sync(
                        &sync_request.src.get_handle().as_any_ref(),
                        &sync_request.dst.as_user_friendly_any_ref(),
                    )
                });
                self.sync_and_report(sync_request, options.sync_options, listener)
            })
            .collect();
        // TODO: PERF: use tokio-tasks?
        let sync_results = futures::future::join_all(futures).await;

        // Convert results, adding a push alias for each dataset pushed
        assert_eq!(plan_indices.len(), sync_results.len());
        for (index, sync_result) in std::iter::zip(plan_indices, sync_results) {
            let push_item = &plan[index];
            let sync_result = match sync_result {
                Ok(sync_result) if options.add_aliases => self
                    .save_dataset_alias_transactional(
                        &push_item.local_handle,
                        &((&push_item.remote_target.url).into()),
                    )
                    .await
                    .map(|_| sync_result)
                    .map_err(|e| SyncError::Internal(PushAliasNotSavedError(e).int_err())),
                sync_result => sync_result,
            };
            responses.push(push_item.as_response(sync_result));
        }

        Ok(responses)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A push that succeeded but whose push alias could not be saved
#[derive(thiserror::Error, Debug)]
#[error("Dataset was pushed, but its push alias was not saved")]
struct PushAliasNotSavedError(#[source] InternalError);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
