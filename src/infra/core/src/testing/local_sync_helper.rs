// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::DatabaseTransactionRunner;
use kamu_core::*;

use crate::SyncRequestBuilder;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pulls into a local dataset the way the pull use case does: the request is
/// built in a transaction, the transfer runs with none open, and the commit
/// runs in a transaction of its own
pub struct LocalSyncHelper {
    catalog: dill::Catalog,
}

impl LocalSyncHelper {
    pub fn new(catalog: dill::Catalog) -> Self {
        Self { catalog }
    }

    pub async fn pull(
        &self,
        src_ref: odf::DatasetRefAny,
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
        src_ref: odf::DatasetRefAny,
        dst_ref: odf::DatasetRef,
        create_if_not_exists: bool,
    ) -> Result<LocalSyncRequest, SyncError> {
        DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(|sync_request_builder: Arc<SyncRequestBuilder>| async move {
                let request = sync_request_builder
                    .build_local_sync_request(src_ref, dst_ref, create_if_not_exists)
                    .await?;
                request.src.detach_from_transaction();
                request.dst.detach_from_transaction();
                Ok(request)
            })
            .await
    }

    pub async fn transfer_and_commit(
        &self,
        request: LocalSyncRequest,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let sync_service = self.catalog.get_one::<dyn SyncService>().unwrap();

        match sync_service
            .transfer_into_local(request, options, listener)
            .await?
        {
            SyncTransfer::UpToDate => Ok(SyncResult::UpToDate),
            SyncTransfer::Transferred(transferred) => {
                DatabaseTransactionRunner::new(self.catalog.clone())
                    .transactional_with(
                        |local_sync_committer: Arc<dyn LocalSyncCommitter>| async move {
                            local_sync_committer.commit(transferred).await
                        },
                    )
                    .await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
