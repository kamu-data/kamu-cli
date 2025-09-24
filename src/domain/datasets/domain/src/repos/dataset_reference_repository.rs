// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cheap_clone::CheapClone;
use internal_error::InternalError;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetReferenceRepository: Send + Sync {
    async fn has_any_references(&self) -> Result<bool, InternalError>;

    async fn get_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<odf::Multihash, GetDatasetReferenceError>;

    async fn get_all_dataset_references(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<(odf::BlockRef, odf::Multihash)>, InternalError>;

    async fn set_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        maybe_prev_block_hash: Option<&odf::Multihash>,
        new_block_hash: &odf::Multihash,
    ) -> Result<(), SetDatasetReferenceError>;

    async fn remove_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<(), RemoveDatasetReferenceError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum SetDatasetReferenceError {
    #[error(transparent)]
    CASFailed(#[from] DatasetReferenceCASError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetDatasetReferenceError {
    #[error(transparent)]
    NotFound(#[from] DatasetReferenceNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum RemoveDatasetReferenceError {
    #[error(transparent)]
    NotFound(#[from] DatasetReferenceNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error(
    "When updating reference '{block_ref}' for dataset '{dataset_id}', expected to point at \
     {expected_prev_block_hash:?} but points at {actual_prev_block_hash:?}"
)]
pub struct DatasetReferenceCASError {
    pub dataset_id: odf::DatasetID,
    pub block_ref: odf::BlockRef,
    pub expected_prev_block_hash: Option<odf::Multihash>,
    pub actual_prev_block_hash: Option<odf::Multihash>,
}

impl DatasetReferenceCASError {
    pub fn new(
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        expected_prev_block_hash: Option<&odf::Multihash>,
        actual_prev_block_hash: Option<&odf::Multihash>,
    ) -> Self {
        Self {
            dataset_id: dataset_id.clone(),
            block_ref: block_ref.cheap_clone(),
            expected_prev_block_hash: expected_prev_block_hash.cloned(),
            actual_prev_block_hash: actual_prev_block_hash.cloned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Reference '{block_ref}' could not be found for dataset '{dataset_id}'")]
pub struct DatasetReferenceNotFoundError {
    pub dataset_id: odf::DatasetID,
    pub block_ref: odf::BlockRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
