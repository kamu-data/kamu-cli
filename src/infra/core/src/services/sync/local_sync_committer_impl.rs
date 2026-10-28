// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::*;
use kamu_datasets::{
    CreateDatasetUseCase,
    CreateDatasetUseCaseOptions,
    DatasetHeadCommitter,
    DatasetRegistry,
    DatasetRegistryExt,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn LocalSyncCommitter)]
pub struct LocalSyncCommitterImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_head_committer: Arc<dyn DatasetHeadCommitter>,
    // Only a first pull creates a dataset
    create_dataset_use_case: dill::Lazy<Arc<dyn CreateDatasetUseCase>>,
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl LocalSyncCommitter for LocalSyncCommitterImpl {
    #[tracing::instrument(level = "debug", name = LocalSyncCommitterImpl_commit, skip_all, fields(new_head = %transferred.new_head))]
    async fn commit(&self, transferred: TransferredSync) -> Result<SyncResult, SyncError> {
        let TransferredSync {
            target,
            new_head,
            num_blocks,
            increment,
        } = transferred;

        match target {
            TransferredSyncTarget::Existing {
                dataset,
                expected_head,
            } => {
                use odf::dataset::SetChainRefError as E;

                // The transfer ran detached, the commit needs this transaction. The
                // dataset may have been deleted meanwhile
                let dataset = self
                    .dataset_registry
                    .get_dataset_by_id(dataset.get_id())
                    .await?;

                self.dataset_head_committer
                    .commit_head(&dataset, expected_head.as_ref(), &new_head)
                    .await
                    .map_err(|e| match e {
                        E::CASFailed(e) => SyncError::HeadMoved(e),
                        E::Access(e) => SyncError::Access(e),
                        e @ (E::BlockNotFound(_) | E::Internal(_)) => {
                            SyncError::Internal(e.int_err())
                        }
                    })?;

                Ok(SyncResult::Updated {
                    old_head: expected_head,
                    new_head,
                    num_blocks,
                    increment,
                })
            }
            TransferredSyncTarget::New {
                alias,
                seed_block,
                seed_block_bytes,
                visibility,
            } => {
                self.create_dataset_use_case
                    .get()
                    .int_err()?
                    .execute(
                        &alias,
                        seed_block,
                        CreateDatasetUseCaseOptions {
                            dataset_visibility: visibility,
                            seed_block_bytes: Some(seed_block_bytes),
                            initial_head: Some(new_head.clone()),
                        },
                    )
                    .await?;

                Ok(SyncResult::Updated {
                    old_head: None,
                    new_head,
                    num_blocks,
                    increment,
                })
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
