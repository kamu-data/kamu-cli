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
use kamu_accounts::Principal;
use kamu_core::{
    PullLocalTarget,
    SyncError,
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
    CreateDatasetUseCase,
    CreateDatasetUseCaseOptions,
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetReferenceUpdater,
    DatasetRegistry,
};

use super::job_target::resolve_job_target;
use crate::utils::span_outcome::SpanOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SyncFromJob)]
pub struct SyncFromJobImpl {
    catalog: dill::Catalog,
    sync_service: Arc<dyn SyncService>,
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
        let transferred = match self
            .sync_service
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

    /// Moves `HEAD` of an existing dataset, or creates a new one, each in a
    /// transaction of its own
    async fn commit(
        &self,
        transferred: SyncFromTransferred,
    ) -> Result<SyncResult, SyncFromJobError> {
        let SyncFromTransferred {
            target,
            new_head,
            num_blocks,
            increment,
        } = transferred;

        let old_head = match target {
            SyncFromTransferredTarget::Existing {
                dataset,
                expected_head,
            } => {
                self.commit_into_existing(dataset.get_handle(), expected_head.as_ref(), &new_head)
                    .await?;
                expected_head
            }
            SyncFromTransferredTarget::New {
                alias,
                seed_block,
                seed_block_bytes,
                visibility,
            } => {
                let options = CreateDatasetUseCaseOptions {
                    dataset_visibility: visibility,
                    seed_block_bytes: Some(seed_block_bytes),
                    initial_head: Some(new_head.clone()),
                };
                self.commit_into_new(&alias, seed_block, options).await?;
                None
            }
        };

        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks,
            increment,
        })
    }

    /// Moves `HEAD` from the head the transfer started from. The transfer
    /// ran detached, so the dataset is re-resolved here
    async fn commit_into_existing(
        &self,
        target: &odf::DatasetHandle,
        expected_head: Option<&odf::Multihash>,
        new_head: &odf::Multihash,
    ) -> Result<(), SyncFromJobError> {
        transaction!(|dataset_registry: Arc<dyn DatasetRegistry>,
                      updater: Arc<dyn DatasetReferenceUpdater>| {
            let dataset =
                resolve_job_target::<SyncFromJobError>(dataset_registry.as_ref(), target).await?;
            updater
                .update_reference(&dataset, &odf::BlockRef::Head, expected_head, new_head)
                .await
                .map_err(SyncError::from)?;
        });
        Ok(())
    }

    /// Creates the dataset with `HEAD` already at the transferred head
    async fn commit_into_new(
        &self,
        alias: &odf::DatasetAlias,
        seed_block: odf::MetadataBlockTyped<odf::metadata::Seed>,
        options: CreateDatasetUseCaseOptions,
    ) -> Result<(), SyncFromJobError> {
        transaction!(|create_dataset_use_case: Arc<dyn CreateDatasetUseCase>| {
            create_dataset_use_case
                .execute(alias, seed_block, options)
                .await
                .map_err(SyncError::from)?;
        });
        Ok(())
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
