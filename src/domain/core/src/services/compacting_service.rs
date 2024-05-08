// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use ::serde::{Deserialize, Serialize};
use opendatafabric::*;
use thiserror::Error;

use crate::*;

pub const DEFAULT_MAX_SLICE_SIZE: u64 = 1_073_741_824;
pub const DEFAULT_MAX_SLICE_RECORDS: u64 = 10000;

#[async_trait::async_trait]
pub trait CompactingService: Send + Sync {
    async fn compact_dataset(
        &self,
        dataset_handle: &DatasetHandle,
        options: CompactingOptions,
        listener: Option<Arc<dyn CompactingMultiListener>>,
    ) -> Result<CompactingResult, CompactingError>;
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum CompactingError {
    #[error(transparent)]
    DatasetNotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
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
    InvalidDatasetKind(
        #[from]
        #[backtrace]
        InvalidDatasetKindError,
    ),
}

impl From<GetDatasetError> for CompactingError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<auth::DatasetActionUnauthorizedError> for CompactingError {
    fn from(v: auth::DatasetActionUnauthorizedError) -> Self {
        match v {
            auth::DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            auth::DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<GetRefError> for CompactingError {
    fn from(v: GetRefError) -> Self {
        match v {
            GetRefError::NotFound(e) => Self::Internal(e.int_err()),
            GetRefError::Access(e) => Self::Access(e),
            GetRefError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<IterBlocksError> for CompactingError {
    fn from(v: IterBlocksError) -> Self {
        match v {
            IterBlocksError::Access(e) => CompactingError::Access(e),
            IterBlocksError::Internal(e) => CompactingError::Internal(e),
            _ => CompactingError::Internal(v.int_err()),
        }
    }
}

impl From<SetRefError> for CompactingError {
    fn from(v: SetRefError) -> Self {
        match v {
            SetRefError::Access(e) => CompactingError::Access(e),
            SetRefError::Internal(e) => CompactingError::Internal(e),
            _ => CompactingError::Internal(v.int_err()),
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

pub trait CompactingListener: Send + Sync {
    fn begin(&self) {}
    fn success(&self, _res: &CompactingResult) {}
    fn error(&self, _err: &CompactingError) {}

    fn begin_phase(&self, _phase: CompactingPhase) {}
    fn end_phase(&self, _phase: CompactingPhase) {}
}

pub struct NullCompactingListener;
impl CompactingListener for NullCompactingListener {}

///////////////////////////////////////////////////////////////////////////////

pub trait CompactingMultiListener: Send + Sync {
    fn begin_compact(&self, _dataset: &DatasetHandle) -> Option<Arc<dyn CompactingListener>> {
        None
    }
}

pub struct NullCompactingMultiListener;
impl CompactingMultiListener for NullCompactingMultiListener {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactingPhase {
    GatherChainInfo,
    MergeDataslices,
    CommitNewBlocks,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompactingResult {
    NothingToDo,
    Success {
        old_head: Multihash,
        new_head: Multihash,
        old_num_blocks: usize,
        new_num_blocks: usize,
    },
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CompactingOptions {
    pub max_slice_size: Option<u64>,
    pub max_slice_records: Option<u64>,
    pub is_keep_metadata_only: bool,
}

impl Default for CompactingOptions {
    fn default() -> Self {
        Self {
            max_slice_size: Some(DEFAULT_MAX_SLICE_SIZE),
            max_slice_records: Some(DEFAULT_MAX_SLICE_RECORDS),
            is_keep_metadata_only: false,
        }
    }
}
