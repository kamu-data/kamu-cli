// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use odf_metadata::{DatasetAlias, DatasetID, MetadataBlockTyped, Seed};
use thiserror::Error;

use crate::{CreateDatasetResult, DatasetNotFoundError, GetStoredDatasetError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(any(feature = "testing", test), mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetStorageUnitWriter: Sync + Send {
    async fn store_dataset(
        &self,
        dataset_alias: &DatasetAlias,
        seed_block: MetadataBlockTyped<Seed>,
    ) -> Result<CreateDatasetResult, StoreDatasetError>;

    async fn delete_dataset(&self, dataset_id: &DatasetID) -> Result<(), DeleteStoredDatasetError>;
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
    NotFound(#[from] DatasetNotFoundError),

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
            GetStoredDatasetError::NotFound(e) => Self::NotFound(e),
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
