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
use kamu::SyncFromJobImpl;
use kamu_core::*;
use kamu_datasets::{AlwaysHappyDatasetPermissionChecker, DatasetRegistry, DatasetRegistryExt};
use kamu_datasets_services::DatasetOperationGuardImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pulls into a local dataset with `SyncFromJob`, allowing the catalog's
/// acting principal every dataset operation
pub struct LocalSyncHelper {
    catalog: dill::Catalog,
}

impl LocalSyncHelper {
    pub fn new(catalog: &dill::Catalog) -> Self {
        Self {
            catalog: dill::CatalogBuilder::new_chained(catalog)
                .add::<SyncFromJobImpl>()
                .add::<DatasetOperationGuardImpl>()
                .add::<AlwaysHappyDatasetPermissionChecker>()
                .build(),
        }
    }

    /// Pulls into a destination that must not be deleted before the commit
    pub async fn pull(
        &self,
        src_ref: odf::DatasetRefRemote,
        dst_ref: odf::DatasetRef,
        options: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        self.pull_with_listener(src_ref, dst_ref, options, None)
            .await
    }

    /// Pulls into a destination that must not be deleted before the commit
    pub async fn pull_with_listener(
        &self,
        src_ref: odf::DatasetRefRemote,
        dst_ref: odf::DatasetRef,
        options: SyncOptions,
        maybe_listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        match self
            .run_job(src_ref, dst_ref, options, maybe_listener)
            .await
        {
            Ok(sync_result) => Ok(sync_result),
            Err(SyncFromJobError::Sync(e)) => Err(e),
            Err(SyncFromJobError::TargetDeleted(e)) => {
                panic!("The sync target must not be deleted: {e}")
            }
        }
    }

    pub async fn run_job(
        &self,
        src_ref: odf::DatasetRefRemote,
        dst_ref: odf::DatasetRef,
        options: SyncOptions,
        maybe_listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncFromJobError> {
        let target = self
            .pull_local_target(dst_ref, options.create_if_not_exists)
            .await?;
        self.catalog
            .get_one::<dyn SyncFromJob>()
            .unwrap()
            .run(&target, &src_ref, options, maybe_listener)
            .await
    }

    /// The request a pull would transfer, for tests of the transfer alone
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
