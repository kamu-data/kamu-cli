// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError};
use odf_metadata::*;
use thiserror::Error;
use tokio_stream::Stream;

use crate::{AppendError, Dataset, ValidateDatasetSnapshotError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Abstraction of datasets storage unit, a bunch of datasets stored together
#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetStorageUnit: Sync + Send {
    async fn resolve_stored_dataset_handle_by_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetHandle, GetDatasetError>;

    fn stored_dataset_handles(&self) -> DatasetHandleStream<'_>;

    fn stored_dataset_handles_by_owner(
        &self,
        account_name: &AccountName,
    ) -> DatasetHandleStream<'_>;

    fn get_stored_dataset_by_handle(&self, dataset_handle: &DatasetHandle) -> Arc<dyn Dataset>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetHandleStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DatasetHandle, InternalError>> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CreateDatasetResult {
    pub dataset_handle: DatasetHandle,
    pub dataset: Arc<dyn Dataset>,
    pub head: Multihash,
}

impl CreateDatasetResult {
    pub fn new(dataset_handle: DatasetHandle, dataset: Arc<dyn Dataset>, head: Multihash) -> Self {
        Self {
            dataset_handle,
            dataset,
            head,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CreateDatasetFromSnapshotResult {
    pub create_dataset_result: CreateDatasetResult,
    pub new_upstream_ids: Vec<DatasetID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset not found: {dataset_ref}")]
pub struct DatasetNotFoundError {
    pub dataset_ref: DatasetRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
pub struct MissingInputsError {
    pub dataset_ref: DatasetRef,
    pub missing_inputs: Vec<DatasetRef>,
}

impl std::fmt::Display for MissingInputsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Dataset {} is referencing non-existing inputs: ",
            self.dataset_ref
        )?;
        for (i, h) in self.missing_inputs.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{h}")?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
pub struct DanglingReferenceError {
    pub dataset_handle: DatasetHandle,
    pub children: Vec<DatasetHandle>,
}

impl std::fmt::Display for DanglingReferenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Dataset {} is referenced by: ", self.dataset_handle)?;
        for (i, h) in self.children.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{h}")?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset with name {alias} already exists")]
pub struct NameCollisionError {
    pub alias: DatasetAlias,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset with id {id} already exists")]
pub struct RefCollisionError {
    pub id: DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Invalid snapshot: {reason}")]
pub struct InvalidSnapshotError {
    pub reason: String,
}

impl InvalidSnapshotError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CreateDatasetError {
    #[error("Dataset is empty")]
    EmptyDataset,
    #[error(transparent)]
    NameCollision(#[from] NameCollisionError),
    #[error(transparent)]
    RefCollision(#[from] RefCollisionError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

#[derive(Error, Debug)]
pub enum CreateDatasetFromSnapshotError {
    #[error(transparent)]
    InvalidSnapshot(#[from] InvalidSnapshotError),
    #[error(transparent)]
    MissingInputs(#[from] MissingInputsError),
    #[error(transparent)]
    NameCollision(#[from] NameCollisionError),
    #[error(transparent)]
    RefCollision(#[from] RefCollisionError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<CreateDatasetError> for CreateDatasetFromSnapshotError {
    fn from(v: CreateDatasetError) -> Self {
        match v {
            CreateDatasetError::EmptyDataset => unreachable!(),
            CreateDatasetError::NameCollision(e) => Self::NameCollision(e),
            CreateDatasetError::RefCollision(e) => Self::RefCollision(e),
            CreateDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<AppendError> for CreateDatasetFromSnapshotError {
    fn from(v: AppendError) -> Self {
        match v {
            AppendError::InvalidBlock(e) => {
                Self::InvalidSnapshot(InvalidSnapshotError::new(e.to_string()))
            }
            AppendError::RefCASFailed(_) | AppendError::Access(_) | AppendError::RefNotFound(_) => {
                Self::Internal(v.int_err())
            }
            AppendError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<ValidateDatasetSnapshotError> for CreateDatasetFromSnapshotError {
    fn from(v: ValidateDatasetSnapshotError) -> Self {
        match v {
            ValidateDatasetSnapshotError::InvalidSnapshot(e) => {
                CreateDatasetFromSnapshotError::InvalidSnapshot(e)
            }
            ValidateDatasetSnapshotError::MissingInputs(e) => {
                CreateDatasetFromSnapshotError::MissingInputs(e)
            }
            ValidateDatasetSnapshotError::Internal(e) => {
                CreateDatasetFromSnapshotError::Internal(e)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum RenameDatasetError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),
    #[error(transparent)]
    NameCollision(#[from] NameCollisionError),
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetDatasetError> for RenameDatasetError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::NotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteDatasetError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),
    #[error(transparent)]
    DanglingReference(#[from] DanglingReferenceError),
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
