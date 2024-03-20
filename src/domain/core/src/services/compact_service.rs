// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use opendatafabric::*;
use thiserror::Error;

use crate::*;

#[async_trait::async_trait]
pub trait CompactService: Send + Sync {
    async fn compact_dataset(
        &self,
        dataset_handle: &DatasetHandle,
        dataset_dir_path: &Path,
        max_slice_size: u64,
        listener: Option<Arc<dyn CompactionMultiListener>>,
    ) -> Result<(), CompactError>;
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum CompactError {
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
    #[error(transparent)]
    ObjectNotFound(
        #[from]
        #[backtrace]
        ObjectNotFoundError,
    ),
    #[error(transparent)]
    MetadataAppendError(
        #[from]
        #[backtrace]
        AppendError,
    ),
    #[error(transparent)]
    InvalidDatasetKind(
        #[from]
        #[backtrace]
        InvalidDatasetKindError,
    ),
}

impl From<GetDatasetError> for CompactError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<auth::DatasetActionUnauthorizedError> for CompactError {
    fn from(v: auth::DatasetActionUnauthorizedError) -> Self {
        match v {
            auth::DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            auth::DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<GetRefError> for CompactError {
    fn from(v: GetRefError) -> Self {
        match v {
            GetRefError::NotFound(e) => Self::RefNotFound(e),
            GetRefError::Access(e) => Self::Internal(e.int_err()),
            GetRefError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<IterBlocksError> for CompactError {
    fn from(v: IterBlocksError) -> Self {
        match v {
            IterBlocksError::RefNotFound(e) => CompactError::RefNotFound(e),
            IterBlocksError::BlockNotFound(e) => CompactError::BlockNotFound(e),
            IterBlocksError::BlockVersion(e) => CompactError::BlockVersion(e),
            IterBlocksError::BlockMalformed(e) => CompactError::BlockMalformed(e),
            IterBlocksError::InvalidInterval(e) => CompactError::InvalidInterval(e),
            IterBlocksError::Access(e) => CompactError::Access(e),
            IterBlocksError::Internal(e) => CompactError::Internal(e),
        }
    }
}

impl From<SetRefError> for CompactError {
    fn from(v: SetRefError) -> Self {
        match v {
            SetRefError::Access(e) => CompactError::Access(e),
            SetRefError::Internal(e) => CompactError::Internal(e),
            SetRefError::BlockNotFound(e) => CompactError::Internal(e.int_err()),
            SetRefError::CASFailed(e) => CompactError::Internal(e.int_err()),
        }
    }
}

impl From<CommitError> for CompactError {
    fn from(v: CommitError) -> Self {
        match v {
            CommitError::ObjectNotFound(e) => CompactError::ObjectNotFound(e),
            CommitError::MetadataAppendError(e) => CompactError::MetadataAppendError(e),
            CommitError::Internal(e) => CompactError::Internal(e),
        }
    }
}

#[derive(Error, Debug)]
#[error("Dataset {dataset_name} in not root kind")]
pub struct InvalidDatasetKindError {
    pub dataset_name: DatasetName,
}

///////////////////////////////////////////////////////////////////////////////
// Progress bar
///////////////////////////////////////////////////////////////////////////////

pub trait CompactionListener: Send + Sync {
    fn begin(&self) {}
    // ToDo add result
    fn success(&self) {}
    // ToDo add error
    fn error(&self) {}

    fn begin_phase(&self, _phase: CompactionPhase) {}
    fn end_phase(&self, _phase: CompactionPhase) {}
}

pub struct NullCompactionListener;
impl CompactionListener for NullCompactionListener {}

///////////////////////////////////////////////////////////////////////////////

pub trait CompactionMultiListener: Send + Sync {
    fn begin_compact(&self, _dataset: &DatasetHandle) -> Option<Arc<dyn CompactionListener>> {
        None
    }
}

pub struct NullCompactionMultiListener;
impl CompactionMultiListener for NullCompactionMultiListener {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPhase {
    GatherChainInfo,
    MergeDataslices,
    ReplaceChainHead,
    CleanOldFiles,
}
