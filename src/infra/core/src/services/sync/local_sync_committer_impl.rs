// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{Lazy, component, interface};
use internal_error::ResultIntoInternal;
use kamu_core::{
    LocalSyncCommitter,
    SyncError,
    SyncFromTransferred,
    SyncFromTransferredTarget,
    SyncResult,
};
use kamu_datasets::{
    CreateDatasetUseCase,
    CreateDatasetUseCaseOptions,
    DatasetReferenceUpdater,
    DatasetRegistry,
    DatasetRegistryExt,
    ResolvedDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn LocalSyncCommitter)]
pub struct LocalSyncCommitterImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_reference_updater: Arc<dyn DatasetReferenceUpdater>,
    // Resolved only for a first pull, so catalogs that only update existing
    // datasets need no dataset creation
    create_dataset_use_case: Lazy<Arc<dyn CreateDatasetUseCase>>,
}

impl LocalSyncCommitterImpl {
    /// Moves `HEAD` of an existing dataset from the head the transfer started
    /// from. The transfer ran detached, so the dataset is re-resolved here
    async fn commit_into_existing(
        &self,
        dataset: &ResolvedDataset,
        expected_head: Option<&odf::Multihash>,
        new_head: &odf::Multihash,
    ) -> Result<(), SyncError> {
        let dataset = self
            .dataset_registry
            .get_dataset_by_id(dataset.get_id())
            .await?;
        self.dataset_reference_updater
            .update_reference(&dataset, &odf::BlockRef::Head, expected_head, new_head)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl LocalSyncCommitter for LocalSyncCommitterImpl {
    async fn commit(
        &self,
        transferred: SyncFromTransferred,
    ) -> Result<Option<SyncResult>, SyncError> {
        let SyncFromTransferred {
            target,
            new_head,
            num_blocks,
            increment,
        } = transferred;

        let committed = match target {
            SyncFromTransferredTarget::Existing {
                dataset,
                expected_head,
            } => self
                .commit_into_existing(&dataset, expected_head.as_ref(), &new_head)
                .await
                .map(|()| expected_head),
            SyncFromTransferredTarget::New {
                alias,
                seed_block,
                seed_block_bytes,
                visibility,
            } => {
                let create_dataset_use_case = self.create_dataset_use_case.get().int_err()?;
                create_dataset_use_case
                    .execute(
                        &alias,
                        seed_block,
                        CreateDatasetUseCaseOptions {
                            dataset_visibility: visibility,
                            seed_block_bytes: Some(seed_block_bytes),
                            initial_head: Some(new_head.clone()),
                        },
                    )
                    .await
                    .map(|_| None)
                    .map_err(SyncError::from)
            }
        };

        match committed {
            Ok(old_head) => Ok(Some(SyncResult::Updated {
                old_head,
                new_head,
                num_blocks,
                increment,
            })),
            // An existing target is re-resolved by its ID, so a deleted one is not found
            Err(SyncError::DatasetNotFound(_)) => Ok(None),
            Err(
                e @ (SyncError::RefCollision(_)
                | SyncError::NameCollision(_)
                | SyncError::CreateDatasetFailed(_)
                | SyncError::UnsupportedProtocol(_)
                | SyncError::UnsupportedIpfsStorageType(_)
                | SyncError::RemoteByIdUnsupported(_)
                | SyncError::InvalidDestination(_)
                | SyncError::RepositoryNotFound(_)
                | SyncError::DatasetsDiverged(_)
                | SyncError::DatasetIdMismatch(_)
                | SyncError::DestinationAhead(_)
                | SyncError::OverwriteSeedBlock(_)
                | SyncError::Corrupted(_)
                | SyncError::RefMoved(_)
                | SyncError::Access(_)
                | SyncError::Internal(_)),
            ) => Err(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
