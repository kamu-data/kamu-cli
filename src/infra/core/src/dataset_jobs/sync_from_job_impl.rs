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
use dill::{Lazy, component, interface};
use internal_error::ResultIntoInternal;
use kamu_accounts::Principal;
use kamu_core::{
    LocalSyncCommitter,
    PullLocalTarget,
    SyncFromJob,
    SyncFromJobError,
    SyncFromRequest,
    SyncFromTransfer,
    SyncFromTransferred,
    SyncFromTransferredTarget,
    SyncListener,
    SyncOptions,
    SyncResult,
    SyncService,
};
use kamu_datasets::{
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetRegistry,
};

use crate::utils::span_outcome::SpanOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SyncFromJob)]
pub struct SyncFromJobImpl {
    catalog: dill::Catalog,
    // Resolved only when a sync runs, so catalogs that never sync need no sync
    // service
    sync_service: Lazy<Arc<dyn SyncService>>,
    principal: Arc<Principal>,
}

impl SyncFromJobImpl {
    async fn transfer_and_commit(
        &self,
        target: &PullLocalTarget,
        src: &odf::DatasetRefRemote,
        options: SyncOptions,
        maybe_listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncFromJobError> {
        // 1: Authorize and read the head the commit will expect
        let dst =
            transaction!(|dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
                          dataset_registry: Arc<dyn DatasetRegistry>| {
                // A new dataset is authorized when it is created
                if let PullLocalTarget::Existing(handle) = target {
                    dataset_operation_guard
                        .check_operation(
                            &self.principal,
                            &DatasetOperationRequest::new(
                                handle.clone(),
                                DatasetOperation::SyncFrom,
                            ),
                        )
                        .await?;
                }

                let dst = target
                    .resolve_sync_target(dataset_registry.as_ref())
                    .await?
                    .ok_or_else(|| odf::DatasetNotFoundError::new(target.as_local_ref()))?;
                dst.detach_from_transaction();
                dst
            });

        // 2: Transfer blocks and data, with no transaction open
        let sync_service = self.sync_service.get().int_err()?;
        let transferred = match sync_service
            .sync_from(
                SyncFromRequest {
                    src: src.clone(),
                    dst,
                },
                options,
                maybe_listener,
            )
            .await?
        {
            SyncFromTransfer::UpToDate => return Ok(SyncResult::UpToDate),
            SyncFromTransfer::Transferred(transferred) => transferred,
        };

        // 3: Commit
        self.commit(transferred).await
    }

    /// Moves `HEAD` of an existing dataset, or creates a new one
    async fn commit(
        &self,
        transferred: SyncFromTransferred,
    ) -> Result<SyncResult, SyncFromJobError> {
        let maybe_existing_ref = match &transferred.target {
            SyncFromTransferredTarget::Existing { dataset, .. } => {
                Some(dataset.get_handle().as_local_ref())
            }
            SyncFromTransferredTarget::New { .. } => None,
        };

        let committed = transaction!(|committer: Arc<dyn LocalSyncCommitter>| {
            committer.commit(transferred).await?
        });

        match (committed, maybe_existing_ref) {
            (Some(sync_result), _) => Ok(sync_result),
            (None, Some(existing_ref)) => Err(SyncFromJobError::TargetDeleted(
                odf::DatasetNotFoundError::new(existing_ref),
            )),
            (None, None) => unreachable!("Only an existing target can be deleted"),
        }
    }

    fn span_outcome(result: &Result<SyncResult, SyncFromJobError>) -> SpanOutcome {
        match result {
            Ok(SyncResult::Updated { .. }) => SpanOutcome::Updated,
            Ok(SyncResult::UpToDate) => SpanOutcome::UpToDate,
            Err(SyncFromJobError::TargetDeleted(_)) => SpanOutcome::DatasetDeleted,
            Err(SyncFromJobError::Sync(e)) => SpanOutcome::from(e),
        }
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl SyncFromJob for SyncFromJobImpl {
    #[tracing::instrument(level = "info", name = SyncFromJobImpl_run, skip_all, fields(?target, %src, outcome))]
    async fn run(
        &self,
        target: &PullLocalTarget,
        src: &odf::DatasetRefRemote,
        options: SyncOptions,
        maybe_listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncFromJobError> {
        let result = self
            .transfer_and_commit(target, src, options, maybe_listener)
            .await;
        Self::span_outcome(&result).record();
        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
