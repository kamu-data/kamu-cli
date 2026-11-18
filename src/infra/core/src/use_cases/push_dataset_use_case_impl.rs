// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::transaction;
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
    SyncResult,
    SyncToJob,
};
use kamu_datasets::{DatasetOperation, DatasetOperationGuard, DatasetOperationRequest};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn PushDatasetUseCase)]
pub struct PushDatasetUseCaseImpl {
    catalog: dill::Catalog,
    sync_to_job: Arc<dyn SyncToJob>,
}

#[common_macros::method_names_consts]
impl PushDatasetUseCaseImpl {
    #[tracing::instrument(level = "debug", name = PushDatasetUseCaseImpl_make_authorization_checks, skip_all, fields(num_datasets = dataset_handles.len(), ?push_target))]
    async fn make_authorization_checks(
        dataset_operation_guard: &dyn DatasetOperationGuard,
        principal: &Principal,
        dataset_handles: Vec<odf::DatasetHandle>,
        push_target: Option<&odf::DatasetPushTarget>,
    ) -> Result<(Vec<odf::DatasetHandle>, Vec<PushResponse>), InternalError> {
        let requests: Vec<_> = dataset_handles
            .into_iter()
            .map(|hdl| DatasetOperationRequest::new(hdl, DatasetOperation::SyncTo))
            .collect();
        let verdicts = dataset_operation_guard
            .check_operations(principal, &requests)
            .await?;

        let mut authorized_handles = Vec::with_capacity(requests.len());
        let mut unauthorized_responses = Vec::new();
        for (request, verdict) in requests.into_iter().zip(verdicts) {
            match verdict {
                Ok(()) => authorized_handles.push(request.target),
                Err(error) => unauthorized_responses.push(PushResponse {
                    local_handle: Some(request.target),
                    target: push_target.cloned(),
                    result: Err(PushError::SyncError(error.into())),
                }),
            }
        }

        Ok((authorized_handles, unauthorized_responses))
    }

    /// Syncs, then reports the result to the listener: a push to a remote
    /// commits there, so the result is final when the sync returns
    async fn sync_and_report(
        &self,
        push_item: &PushItem,
        sync_options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let result = self
            .sync_to_job
            .run(
                &push_item.local_handle,
                (&push_item.remote_target.url).into(),
                sync_options,
                listener.clone(),
            )
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
        let (authorized_handles, unauthorized_responses) =
            transaction!(|dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
                          principal: Arc<Principal>| {
                Self::make_authorization_checks(
                    dataset_operation_guard.as_ref(),
                    &principal,
                    dataset_handles,
                    options.remote_target.as_ref(),
                )
                .await?
            });
        responses.extend(unauthorized_responses);

        // Prepare a push plan
        let (plan, errors) = transaction!(|push_request_planner: Arc<dyn PushRequestPlanner>| {
            push_request_planner
                .collect_plan(&authorized_handles, options.remote_target.as_ref())
                .await
        });
        responses.extend(errors);

        tracing::debug!(?plan, "Obtained push plan");

        // Run sync process; each push opens a short transaction of its own
        let futures: Vec<_> = plan
            .iter()
            .map(|push_item| {
                let listener = sync_listener.as_ref().and_then(|l| {
                    l.begin_sync(
                        &push_item.local_handle.as_any_ref(),
                        &odf::DatasetRefRemote::from(&push_item.remote_target.url).as_any_ref(),
                    )
                });
                self.sync_and_report(push_item, options.sync_options, listener)
            })
            .collect();
        // TODO: PERF: use tokio-tasks?
        let sync_results = futures::future::join_all(futures).await;

        // Convert results, adding a push alias for each dataset pushed
        for (push_item, sync_result) in std::iter::zip(&plan, sync_results) {
            let sync_result = match sync_result {
                Ok(sync_result) if options.add_aliases => transaction!(
                    |remote_alias_registry: Arc<dyn RemoteAliasesRegistry>|
                     -> Result<_, InternalError> {
                        remote_alias_registry
                            .get_remote_aliases(&push_item.local_handle)
                            .await?
                            .add(&(&push_item.remote_target.url).into(), RemoteAliasKind::Push)
                            .await
                    }
                )
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
