// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_datasets::{DatasetReferenceService, GetDatasetReferenceError, SetDatasetReferenceError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatabaseBackedOdfMetadataChainRefRepositoryImpl<TStorageRefRepo>
where
    TStorageRefRepo: odf::storage::ReferenceRepository + Send + Sync,
{
    dataset_ref_service: Arc<dyn DatasetReferenceService>,
    storage_ref_repo: TStorageRefRepo,
    dataset_id: odf::DatasetID,
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
            dataset_ref_service,
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
    async fn get_ref(
        &self,
        r: &odf::BlockRef,
    ) -> Result<odf::Multihash, odf::storage::GetRefError> {
        self.dataset_ref_service
            .get_reference(&self.dataset_id, r)
            .await
            .map_err(|e| match e {
                GetDatasetReferenceError::NotFound(_) => {
                    odf::storage::GetRefError::NotFound(odf::storage::RefNotFoundError {
                        block_ref_name: r.to_string(),
                    })
                }
                GetDatasetReferenceError::Internal(e) => odf::storage::GetRefError::Internal(e),
            })
    }

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

        self.dataset_ref_service
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
            })
    }

    fn as_raw_ref_repo(&self) -> &dyn odf::storage::ReferenceRepository {
        &self.storage_ref_repo
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
