// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use odf_metadata::{DatasetID, MetadataBlockTyped, Multihash, Seed};
use thiserror::Error;

use crate::{Dataset, DatasetUnresolvedIdError, GetStoredDatasetError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(any(feature = "testing", test), mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetStorageUnitWriter: Sync + Send {
    async fn store_dataset(
        &self,
        seed_block: MetadataBlockTyped<Seed>,
        opts: StoreDatasetOpts,
    ) -> Result<StoreDatasetResult, StoreDatasetError>;

    async fn delete_dataset(&self, dataset_id: &DatasetID) -> Result<(), DeleteStoredDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug)]
pub struct StoreDatasetOpts {
    /// Set HEAD reference to the block after storing
    pub set_head: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct StoreDatasetResult {
    pub dataset_id: DatasetID,
    pub dataset: Arc<dyn Dataset>,
    pub seed: Multihash,
}

impl StoreDatasetResult {
    pub fn new(dataset_id: DatasetID, dataset: Arc<dyn Dataset>, seed: Multihash) -> Self {
        Self {
            dataset_id,
            dataset,
            seed,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum StoreDatasetError {
    #[error(transparent)]
    RefCollision(#[from] RefCollisionError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteStoredDatasetError {
    #[error(transparent)]
    UnresolvedId(#[from] DatasetUnresolvedIdError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetStoredDatasetError> for DeleteStoredDatasetError {
    fn from(value: GetStoredDatasetError) -> Self {
        match value {
            GetStoredDatasetError::UnresolvedId(e) => Self::UnresolvedId(e),
            GetStoredDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset with id {id} already exists")]
pub struct RefCollisionError {
    pub id: DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
