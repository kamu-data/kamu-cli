// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;
use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError};
use opendatafabric::*;
use thiserror::Error;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait VerificationService: Send + Sync {
    async fn verify(
        &self,
        request: VerificationRequest<ResolvedDataset>,
        listener: Option<Arc<dyn VerificationListener>>,
    ) -> VerificationResult;

    async fn verify_multi(
        &self,
        requests: Vec<VerificationRequest<ResolvedDataset>>,
        listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Vec<VerificationResult>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DTOs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct VerificationRequest<TTarget> {
    pub target: TTarget,
    pub block_range: (Option<Multihash>, Option<Multihash>),
    pub options: VerificationOptions,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VerificationResult {
    /// Handle of the dataset, if were able to resolve the requested reference
    pub dataset_handle: Option<DatasetHandle>,
    pub outcome: Result<(), VerificationError>,
}

impl VerificationResult {
    pub fn err(dataset_handle: DatasetHandle, e: impl Into<VerificationError>) -> Self {
        Self {
            dataset_handle: Some(dataset_handle),
            outcome: Err(e.into()),
        }
    }

    pub fn err_no_handle(e: impl Into<VerificationError>) -> Self {
        Self {
            dataset_handle: None,
            outcome: Err(e.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct VerificationOptions {
    pub check_integrity: bool,
    pub check_logical_hashes: bool,
    pub replay_transformations: bool,
}

impl Default for VerificationOptions {
    fn default() -> Self {
        Self {
            check_integrity: true,
            check_logical_hashes: true,
            replay_transformations: true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Listeners
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationPhase {
    DataIntegrity,
    ReplayTransform,
    MetadataIntegrity,
}

// The call pattern is:
//   begin()
//     begin_phase(MetadataIntegrity)
//     end_phase(MetadataIntegrity)
//     begin_phase(DataIntegrity)
//       begin_block()
//       end_block()
//       ...
//     end_phase(DataIntegrity)
//     begin_phase(ReplayTransform)
//       begin_block()
//         get_transform_listener()
//       end_block()
//       ...
//     end_phase(ReplayTransform)
//   success()
pub trait VerificationListener: Send + Sync {
    fn begin(&self) {}
    fn success(&self, _result: &VerificationResult) {}
    fn error(&self, _error: &VerificationError) {}
    fn transform_error(&self, _error: &VerifyTransformExecuteError) {}

    fn begin_phase(&self, _phase: VerificationPhase) {}
    fn end_phase(&self, _phase: VerificationPhase) {}

    fn begin_block(
        &self,
        _block_hash: &Multihash,
        _block_index: usize,
        _num_blocks: usize,
        _phase: VerificationPhase,
    ) {
    }
    fn end_block(
        &self,
        _block_hash: &Multihash,
        _block_index: usize,
        _num_blocks: usize,
        _phase: VerificationPhase,
    ) {
    }

    fn get_transform_listener(self: Arc<Self>) -> Option<Arc<dyn TransformListener>> {
        None
    }
}

pub struct NullVerificationListener;
impl VerificationListener for NullVerificationListener {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait VerificationMultiListener: Send + Sync {
    fn begin_verify(&self, _dataset: &DatasetHandle) -> Option<Arc<dyn VerificationListener>> {
        None
    }
}

pub struct NullVerificationMultiListener;
impl VerificationMultiListener for NullVerificationMultiListener {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum VerificationError {
    #[error(transparent)]
    DatasetNotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
    ),
    #[error(transparent)]
    RefNotFound(
        #[from]
        #[backtrace]
        RefNotFoundError,
    ),
    #[error(transparent)]
    BlockNotFound(
        #[from]
        #[backtrace]
        BlockNotFoundError,
    ),
    #[error(transparent)]
    BlockVersion(
        #[from]
        #[backtrace]
        BlockVersionError,
    ),
    #[error(transparent)]
    BlockMalformed(
        #[from]
        #[backtrace]
        BlockMalformedError,
    ),
    #[error(transparent)]
    InvalidInterval(
        #[from]
        #[backtrace]
        InvalidIntervalError,
    ),
    #[error("Data doesn't match metadata")]
    DataDoesNotMatchMetadata(
        #[from]
        #[backtrace]
        DataDoesNotMatchMetadata,
    ),
    #[error("Data is not reproducible")]
    DataNotReproducible(
        #[from]
        #[backtrace]
        DataNotReproducible,
    ),
    #[error("Checkpoint doesn't match metadata")]
    CheckpointDoesNotMatchMetadata(
        #[from]
        #[backtrace]
        CheckpointDoesNotMatchMetadata,
    ),
    #[error(transparent)]
    VerifyTransform(
        #[from]
        #[backtrace]
        VerifyTransformError,
    ),
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

impl From<GetDatasetError> for VerificationError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => VerificationError::DatasetNotFound(e),
            GetDatasetError::Internal(e) => VerificationError::Internal(e),
        }
    }
}

impl From<GetRefError> for VerificationError {
    fn from(v: GetRefError) -> Self {
        match v {
            GetRefError::NotFound(e) => VerificationError::RefNotFound(e),
            GetRefError::Access(e) => VerificationError::Internal(e.int_err()),
            GetRefError::Internal(e) => VerificationError::Internal(e),
        }
    }
}

impl From<IterBlocksError> for VerificationError {
    fn from(v: IterBlocksError) -> Self {
        match v {
            IterBlocksError::RefNotFound(e) => VerificationError::RefNotFound(e),
            IterBlocksError::BlockNotFound(e) => VerificationError::BlockNotFound(e),
            IterBlocksError::BlockVersion(e) => VerificationError::BlockVersion(e),
            IterBlocksError::BlockMalformed(e) => VerificationError::BlockMalformed(e),
            IterBlocksError::InvalidInterval(e) => VerificationError::InvalidInterval(e),
            IterBlocksError::Access(e) => VerificationError::Internal(e.int_err()),
            IterBlocksError::Internal(e) => VerificationError::Internal(e),
        }
    }
}

impl From<auth::DatasetActionUnauthorizedError> for VerificationError {
    fn from(v: auth::DatasetActionUnauthorizedError) -> Self {
        match v {
            auth::DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            auth::DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<GetBlockError> for VerificationError {
    fn from(v: GetBlockError) -> Self {
        match v {
            GetBlockError::NotFound(e) => Self::BlockNotFound(e),
            GetBlockError::BlockVersion(e) => Self::BlockVersion(e),
            GetBlockError::BlockMalformed(e) => Self::BlockMalformed(e),
            GetBlockError::Access(e) => Self::Internal(e.int_err()),
            GetBlockError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataVerificationError {
    SizeMismatch {
        expected: u64,
        actual: u64,
    },
    PhysicalHashMismatch {
        expected: Multihash,
        actual: Multihash,
    },
    LogicalHashMismatch {
        expected: Multihash,
        actual: Multihash,
    },
}

#[derive(Error, Debug)]
pub struct DataDoesNotMatchMetadata {
    pub block_hash: Multihash,
    pub error: DataVerificationError,
}

impl Display for DataDoesNotMatchMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.error {
            DataVerificationError::SizeMismatch { expected, actual } => write!(
                f,
                "Data size for block {} is expected to be {expected} but actual {actual}",
                self.block_hash,
            ),
            DataVerificationError::PhysicalHashMismatch { expected, actual } => write!(
                f,
                "Data physical hash for block {} is expected to be {expected} but actual {actual}",
                self.block_hash,
            ),
            DataVerificationError::LogicalHashMismatch { expected, actual } => write!(
                f,
                "Data logical hash for block {} is expected to be {expected} but actual {actual}",
                self.block_hash,
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub struct DataNotReproducible {
    pub block_hash: Multihash,
    pub expected_event: Box<MetadataEvent>,
    pub actual_event: Box<MetadataEvent>,
}

impl Display for DataNotReproducible {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "At block {:?} expected event {:?} but got non-equivalent event {:?}",
            self.block_hash, self.expected_event, self.actual_event
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointVerificationError {
    SizeMismatch {
        expected: u64,
        actual: u64,
    },
    PhysicalHashMismatch {
        expected: Multihash,
        actual: Multihash,
    },
}

#[derive(Error, Debug)]
pub struct CheckpointDoesNotMatchMetadata {
    pub block_hash: Multihash,
    pub error: CheckpointVerificationError,
}

impl Display for CheckpointDoesNotMatchMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.error {
            CheckpointVerificationError::SizeMismatch { expected, actual } => write!(
                f,
                "Checkpoint size for block {} is expected to be {expected} but actual {actual}",
                self.block_hash,
            ),
            CheckpointVerificationError::PhysicalHashMismatch { expected, actual } => write!(
                f,
                "Checkpoint physical hash for block {} is expected to be {expected} but actual \
                 {actual}",
                self.block_hash,
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
