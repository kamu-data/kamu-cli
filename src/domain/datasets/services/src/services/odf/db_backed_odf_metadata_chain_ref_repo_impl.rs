// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use kamu_datasets::{DatasetReferenceService, GetDatasetReferenceError, SetDatasetReferenceError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatabaseBackedOdfMetadataChainRefRepositoryImpl<TStorageRefRepo>
where
    TStorageRefRepo: odf::storage::ReferenceRepository + Send + Sync,
{
    state: RwLock<State>,
    storage_ref_repo: TStorageRefRepo,
    dataset_id: odf::DatasetID,
}

struct State {
    cached_references: HashMap<odf::BlockRef, odf::Multihash>,
    maybe_dataset_ref_service: Option<Arc<dyn DatasetReferenceService>>,
}

impl<TStorageRefRepo> DatabaseBackedOdfMetadataChainRefRepositoryImpl<TStorageRefRepo>
where
    TStorageRefRepo: odf::storage::ReferenceRepository + Send + Sync,
{
    pub fn new(
        dataset_ref_service: Arc<dyn DatasetReferenceService>,
        storage_ref_repo: TStorageRefRepo,
        dataset_id: odf::DatasetID,
    ) -> Self {
        Self {
            state: RwLock::new(State {
                cached_references: HashMap::new(),
                maybe_dataset_ref_service: Some(dataset_ref_service),
            }),
            storage_ref_repo,
            dataset_id,
        }
    }
}

#[async_trait::async_trait]
impl<TStorageRefRepo> odf::dataset::MetadataChainReferenceRepository
    for DatabaseBackedOdfMetadataChainRefRepositoryImpl<TStorageRefRepo>
where
    TStorageRefRepo: odf::storage::ReferenceRepository + Send + Sync,
{
    fn detach_from_transaction(&self) {
        let mut write_guard = self.state.write().unwrap();
        write_guard.maybe_dataset_ref_service = None;
    }

    fn reattach_to_transaction(&self, catalog: &dill::Catalog) {
        let mut write_guard = self.state.write().unwrap();
        write_guard.maybe_dataset_ref_service =
            Some(catalog.get_one::<dyn DatasetReferenceService>().unwrap());
    }

    #[tracing::instrument(level="debug", skip_all, fields(%r))]
    async fn get_ref(
        &self,
        r: &odf::BlockRef,
    ) -> Result<odf::Multihash, odf::storage::GetRefError> {
        // Try cache first
        let maybe_dataset_ref_service = {
            let guard = self.state.read().unwrap();
            if let Some(cached_ref) = guard.cached_references.get(r) {
                tracing::debug!(dataset_id=%self.dataset_id, %r, %cached_ref, "Hit reference cache");
                return Ok(cached_ref.clone());
            }

            guard.maybe_dataset_ref_service.clone()
        };

        // Read from database
        if let Some(dataset_ref_service) = maybe_dataset_ref_service {
            let resolved_ref = dataset_ref_service
                .get_reference(&self.dataset_id, r)
                .await
                .map_err(|e| match e {
                    GetDatasetReferenceError::NotFound(_) => {
                        odf::storage::GetRefError::NotFound(odf::storage::RefNotFoundError {
                            block_ref_name: r.to_string(),
                        })
                    }
                    GetDatasetReferenceError::Internal(e) => odf::storage::GetRefError::Internal(e),
                })?;

            // Update cache
            let mut write_guard = self.state.write().unwrap();
            write_guard
                .cached_references
                .insert(r.clone(), resolved_ref.clone());

            tracing::debug!(dataset_id=%self.dataset_id, %r, %resolved_ref, "Updated reference cache");

            return Ok(resolved_ref);
        }

        // Fallback: read from storage, if database is inaccessible
        self.storage_ref_repo.get(r.as_str()).await
    }

    #[tracing::instrument(level="debug", skip_all, fields(%r, %hash))]
    async fn set_ref<'a>(
        &'a self,
        r: &odf::BlockRef,
        hash: &odf::Multihash,
        check_ref_is: Option<Option<&'a odf::Multihash>>,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        let maybe_prev_block_hash = if let Some(check_ref_is) = check_ref_is {
            Ok(check_ref_is.cloned())
        } else {
            match self.get_ref(r).await {
                Ok(hash) => Ok(Some(hash)),
                Err(odf::storage::GetRefError::NotFound(_)) => Ok(None),
                Err(odf::storage::GetRefError::Access(e)) => {
                    Err(odf::dataset::SetChainRefError::Access(e))
                }
                Err(odf::storage::GetRefError::Internal(e)) => {
                    Err(odf::dataset::SetChainRefError::Internal(e))
                }
            }
        }?;

        let maybe_dataset_ref_service = {
            // Drop cached reference: if commit succeeds, we will find it out on next read.
            // If commit fails, we did nothing harmful
            let mut writable_guard = self.state.write().unwrap();
            writable_guard.cached_references.remove(r);

            tracing::debug!(dataset_id=%self.dataset_id, %r, "Cleared reference cache");

            writable_guard.maybe_dataset_ref_service.clone()
        };

        if let Some(dataset_ref_service) = maybe_dataset_ref_service {
            dataset_ref_service
                .set_reference(&self.dataset_id, r, maybe_prev_block_hash.as_ref(), hash)
                .await
                .map_err(|e| match e {
                    SetDatasetReferenceError::CASFailed(e) => {
                        odf::dataset::SetChainRefError::CASFailed(odf::dataset::RefCASError {
                            reference: e.block_ref,
                            expected: e.expected_prev_block_hash,
                            actual: e.actual_prev_block_hash,
                        })
                    }
                    SetDatasetReferenceError::Internal(e) => {
                        odf::dataset::SetChainRefError::Internal(e)
                    }
                })?;

            return Ok(());
        }

        panic!("Cannot set dataset reference without attached reference service")
    }

    fn as_raw_ref_repo(&self) -> &dyn odf::storage::ReferenceRepository {
        &self.storage_ref_repo
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
