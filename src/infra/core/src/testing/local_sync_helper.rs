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
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt};

use crate::DatasetUpdateStepsImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pulls into a local dataset through the update steps the pull use case
/// runs: the target is resolved in a transaction, the transfer runs with none
/// open, and the commit runs in a transaction of its own
pub struct LocalSyncHelper {
    catalog: dill::Catalog,
}

impl LocalSyncHelper {
    pub fn new(catalog: &dill::Catalog) -> Self {
        Self {
            catalog: dill::CatalogBuilder::new_chained(catalog)
                .add::<DatasetUpdateStepsImpl>()
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
    ) -> Result<LocalSyncRequest, SyncError> {
        let target = self
            .pull_local_target(dst_ref.clone(), create_if_not_exists)
            .await?;

        let dst = self
            .update_steps()
            .resolve_sync_target(&target)
            .await?
            .ok_or_else(|| DatasetAnyRefUnresolvedError::new(dst_ref))?;

        Ok(LocalSyncRequest { src: src_ref, dst })
    }

    pub async fn transfer_and_commit(
        &self,
        request: LocalSyncRequest,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        self.update_steps()
            .transfer_and_commit_sync(request, options, listener, None)
            .await
    }

    /// The target as the pull planner names it
    async fn pull_local_target(
        &self,
        dst_ref: odf::DatasetRef,
        create_if_not_exists: bool,
    ) -> Result<PullLocalTarget, SyncError> {
        let maybe_handle = DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(|dataset_registry: Arc<dyn DatasetRegistry>| {
                let dst_ref = dst_ref.clone();
                async move {
                    dataset_registry
                        .try_resolve_dataset_handle_by_ref(&dst_ref)
                        .await
                }
            })
            .await?;

        match (maybe_handle, dst_ref.alias()) {
            (Some(handle), _) => Ok(PullLocalTarget::existing(handle)),
            (None, Some(alias)) if create_if_not_exists => {
                Ok(PullLocalTarget::to_create(alias.clone()))
            }
            (None, _) => Err(DatasetAnyRefUnresolvedError::new(dst_ref).into()),
        }
    }

    fn update_steps(&self) -> Arc<dyn DatasetUpdateSteps> {
        self.catalog.get_one().unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
