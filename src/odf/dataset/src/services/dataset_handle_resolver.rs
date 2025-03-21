// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::{DatasetUnresolvedIdError, GetStoredDatasetError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetHandleResolver: Send + Sync {
    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf_metadata::DatasetRef,
    ) -> Result<odf_metadata::DatasetHandle, DatasetRefUnresolvedError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DatasetRefUnresolvedError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetStoredDatasetError> for DatasetRefUnresolvedError {
    fn from(value: GetStoredDatasetError) -> Self {
        match value {
            GetStoredDatasetError::UnresolvedId(e) => Self::NotFound(e.into()),
            GetStoredDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset not found: {dataset_ref}")]
pub struct DatasetNotFoundError {
    pub dataset_ref: odf_metadata::DatasetRef,
}

impl From<DatasetUnresolvedIdError> for DatasetNotFoundError {
    fn from(value: DatasetUnresolvedIdError) -> Self {
        Self {
            dataset_ref: value.dataset_id.as_local_ref(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
