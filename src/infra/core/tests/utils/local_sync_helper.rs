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
use kamu::LocalSyncCommitterImpl;
use kamu_core::*;
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pulls into a local dataset in the steps `SyncFromJob` runs: the target is
/// resolved in a transaction, the transfer runs with none open, and the commit
/// runs in a transaction of its own
pub struct LocalSyncHelper {
    catalog: dill::Catalog,
}

impl LocalSyncHelper {
    pub fn new(catalog: &dill::Catalog) -> Self {
        Self {
            catalog: dill::CatalogBuilder::new_chained(catalog)
                .add::<LocalSyncCommitterImpl>()
                .build(),
        }
    }

    pub async fn pull(
        &self,
        src_ref: odf::DatasetRefRemote,
        dst_ref: odf::DatasetRef,
        options: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        let request = self
            .build_request(src_ref, dst_ref, options.create_if_not_exists)
            .await?;
        self.transfer_and_commit(request, options, None).await
    }

    pub async fn build_request(
        &self,
        src_ref: odf::DatasetRefRemote,
        dst_ref: odf::DatasetRef,
        create_if_not_exists: bool,
    ) -> Result<SyncFromRequest, SyncError> {
        let target = self
            .pull_local_target(dst_ref.clone(), create_if_not_exists)
            .await?;

        let dst = transaction!(|dataset_registry: Arc<dyn DatasetRegistry>| {
            let dst = target
                .resolve_sync_target(dataset_registry.as_ref())
                .await?
                .ok_or_else(|| DatasetAnyRefUnresolvedError::new(dst_ref))?;
            dst.detach_from_transaction();
            dst
        });

        Ok(SyncFromRequest { src: src_ref, dst })
    }

    pub async fn transfer_and_commit(
        &self,
        request: SyncFromRequest,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let sync_service = self.catalog.get_one::<dyn SyncService>().unwrap();
        match sync_service.sync_from(request, options, listener).await? {
            SyncFromTransfer::UpToDate => Ok(SyncResult::UpToDate),
            SyncFromTransfer::Transferred(transferred) => Ok(self
                .commit(transferred)
                .await?
                .expect("The sync target must not be deleted")),
        }
    }

    /// Commits a transfer in a transaction of its own; `None` when the
    /// existing target was deleted meanwhile
    pub async fn commit(
        &self,
        transferred: SyncFromTransferred,
    ) -> Result<Option<SyncResult>, SyncError> {
        transaction!(
            |committer: Arc<dyn LocalSyncCommitter>| -> Result<_, SyncError> {
                committer.commit(transferred).await
            }
        )
    }

    /// Resolves `dst_ref` to a pull target the way the pull planner does
    async fn pull_local_target(
        &self,
        dst_ref: odf::DatasetRef,
        create_if_not_exists: bool,
    ) -> Result<PullLocalTarget, SyncError> {
        let maybe_handle = transaction!(|dataset_registry: Arc<dyn DatasetRegistry>| {
            dataset_registry
                .try_resolve_dataset_handle_by_ref(&dst_ref)
                .await?
        });

        match (maybe_handle, dst_ref.alias()) {
            (Some(handle), _) => Ok(PullLocalTarget::existing(handle)),
            (None, Some(alias)) if create_if_not_exists => {
                Ok(PullLocalTarget::to_create(alias.clone()))
            }
            (None, _) => Err(DatasetAnyRefUnresolvedError::new(dst_ref).into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
