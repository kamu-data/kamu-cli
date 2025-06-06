// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, InternalError};
use thiserror::Error;

use crate::{
    CreateDatasetError,
    CreateDatasetResult,
    CreateDatasetUseCaseOptions,
    DatasetReferenceCASError,
    NameCollisionError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CreateDatasetFromSnapshotUseCase: Send + Sync {
    async fn execute(
        &self,
        snapshot: odf::DatasetSnapshot,
        options: CreateDatasetUseCaseOptions,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CreateDatasetFromSnapshotError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    InvalidSnapshot(#[from] odf::dataset::InvalidSnapshotError),

    #[error(transparent)]
    MissingInputs(#[from] odf::dataset::MissingInputsError),

    #[error(transparent)]
    NameCollision(#[from] NameCollisionError),

    #[error(transparent)]
    RefCollision(#[from] odf::dataset::RefCollisionError),

    #[error(transparent)]
    CASFailed(#[from] Box<DatasetReferenceCASError>),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<CreateDatasetError> for CreateDatasetFromSnapshotError {
    fn from(v: CreateDatasetError) -> Self {
        match v {
            CreateDatasetError::EmptyDataset => unreachable!(),
            CreateDatasetError::Access(e) => Self::Access(e),
            CreateDatasetError::NameCollision(e) => Self::NameCollision(e),
            CreateDatasetError::RefCollision(e) => Self::RefCollision(e),
            CreateDatasetError::CASFailed(e) => Self::CASFailed(e),
            CreateDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<odf::dataset::AppendError> for CreateDatasetFromSnapshotError {
    fn from(v: odf::dataset::AppendError) -> Self {
        match v {
            odf::dataset::AppendError::InvalidBlock(e) => {
                Self::InvalidSnapshot(odf::dataset::InvalidSnapshotError::new(e.to_string()))
            }
            odf::dataset::AppendError::RefCASFailed(_)
            | odf::dataset::AppendError::Access(_)
            | odf::dataset::AppendError::RefNotFound(_) => Self::Internal(v.int_err()),
            odf::dataset::AppendError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<odf::dataset::ValidateDatasetSnapshotError> for CreateDatasetFromSnapshotError {
    fn from(v: odf::dataset::ValidateDatasetSnapshotError) -> Self {
        match v {
            odf::dataset::ValidateDatasetSnapshotError::InvalidSnapshot(e) => {
                CreateDatasetFromSnapshotError::InvalidSnapshot(e)
            }
            odf::dataset::ValidateDatasetSnapshotError::MissingInputs(e) => {
                CreateDatasetFromSnapshotError::MissingInputs(e)
            }
            odf::dataset::ValidateDatasetSnapshotError::Internal(e) => {
                CreateDatasetFromSnapshotError::Internal(e)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
