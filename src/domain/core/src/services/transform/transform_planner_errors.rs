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

use crate::{InputNotPulledError, InvalidInputIntervalError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Errors of reading the transform a dataset defines
#[derive(Debug, Error)]
pub enum TransformPlanError {
    #[error(transparent)]
    TransformNotDefined(
        #[from]
        #[backtrace]
        TransformNotDefinedError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum TransformElaborateError {
    #[error(transparent)]
    TransformNotDefined(
        #[from]
        #[backtrace]
        TransformNotDefinedError,
    ),
    #[error(transparent)]
    InputsChanged(
        #[from]
        #[backtrace]
        TransformInputsChangedError,
    ),
    #[error(transparent)]
    InputNotPulled(
        #[from]
        #[backtrace]
        InputNotPulledError,
    ),
    #[error(transparent)]
    InvalidInputInterval(
        #[from]
        #[backtrace]
        InvalidInputIntervalError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl TransformElaborateError {
    /// Whether elaborating again could succeed. A missing or changed transform
    /// and a rewritten input history need a change of metadata first; an
    /// input without data can still receive it.
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::TransformNotDefined(_)
            | Self::InputsChanged(_)
            | Self::InvalidInputInterval(_) => false,
            Self::InputNotPulled(_) | Self::Internal(_) => true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum TransformRebuildError {
    #[error(transparent)]
    HeadMoved(
        #[from]
        #[backtrace]
        TransformHeadMovedError,
    ),
    #[error(transparent)]
    Mismatch(
        #[from]
        #[backtrace]
        TransformPlanMismatchError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum TransformStatusError {
    #[error(transparent)]
    TransformNotDefined(
        #[from]
        #[backtrace]
        TransformNotDefinedError,
    ),
    #[error(transparent)]
    InputsChanged(
        #[from]
        #[backtrace]
        TransformInputsChangedError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum VerifyTransformPlanError {
    #[error(transparent)]
    DatasetNotFound(
        #[from]
        #[backtrace]
        odf::DatasetNotFoundError,
    ),

    #[error(transparent)]
    RefNotFound(
        #[from]
        #[backtrace]
        odf::storage::RefNotFoundError,
    ),

    #[error(transparent)]
    BlockNotFound(
        #[from]
        #[backtrace]
        odf::storage::BlockNotFoundError,
    ),

    #[error(transparent)]
    BlockVersion(
        #[from]
        #[backtrace]
        odf::storage::BlockVersionError,
    ),

    #[error(transparent)]
    BlockMalformed(
        #[from]
        #[backtrace]
        odf::storage::BlockMalformedError,
    ),

    #[error(transparent)]
    InvalidInterval(
        #[from]
        #[backtrace]
        odf::dataset::InvalidIntervalError,
    ),

    #[error(transparent)]
    InputsChanged(
        #[from]
        #[backtrace]
        TransformInputsChangedError,
    ),

    #[error(transparent)]
    InputNotPulled(
        #[from]
        #[backtrace]
        InputNotPulledError,
    ),

    #[error(transparent)]
    InvalidInputInterval(
        #[from]
        #[backtrace]
        InvalidInputIntervalError,
    ),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<odf::GetRefError> for VerifyTransformPlanError {
    fn from(v: odf::GetRefError) -> Self {
        match v {
            odf::GetRefError::NotFound(e) => Self::RefNotFound(e),
            odf::GetRefError::Access(e) => Self::Access(e),
            odf::GetRefError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<odf::GetBlockError> for VerifyTransformPlanError {
    fn from(v: odf::GetBlockError) -> Self {
        match v {
            odf::GetBlockError::NotFound(e) => Self::BlockNotFound(e),
            odf::GetBlockError::BlockVersion(e) => Self::BlockVersion(e),
            odf::GetBlockError::BlockMalformed(e) => Self::BlockMalformed(e),
            odf::GetBlockError::Access(e) => Self::Access(e),
            odf::GetBlockError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Dataset does not define a transform")]
pub struct TransformNotDefinedError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The inputs of the transform in effect differ from those the last transform
/// consumed. Continuing from the consumed positions is not supported yet.
#[derive(Debug, Error)]
#[error(
    "Transform of {dataset_handle} changed its inputs since it last ran, which is not supported \
     yet"
)]
pub struct TransformInputsChangedError {
    pub dataset_handle: odf::DatasetHandle,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Another writer moved the dataset's head since the stored plan was made, so
/// the plan no longer applies
#[derive(Debug, Error)]
#[error(
    "Head of {dataset_id} moved from {expected_head} to {actual_head} since its transform was \
     planned"
)]
pub struct TransformHeadMovedError {
    pub dataset_id: odf::DatasetID,
    pub expected_head: odf::Multihash,
    pub actual_head: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The request rebuilt from a stored plan differs from that plan. Metadata at
/// pinned heads is immutable, so this indicates a defect; the stored plan
/// cannot be trusted.
#[derive(Debug, Error)]
#[error("Transform of {dataset_id} rebuilt from its stored plan differs from that plan: {reason}")]
pub struct TransformPlanMismatchError {
    pub dataset_id: odf::DatasetID,
    pub reason: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
