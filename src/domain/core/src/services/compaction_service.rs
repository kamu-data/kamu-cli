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

pub const DEFAULT_MAX_SLICE_SIZE: u64 = 300_000_000;
pub const DEFAULT_MAX_SLICE_RECORDS: u64 = 10_000;

#[async_trait::async_trait]
pub trait CompactionService: Send + Sync {
    async fn compact_dataset(
        &self,
        dataset_handle: &DatasetHandle,
        options: CompactionOptions,
        listener: Option<Arc<dyn CompactionMultiListener>>,
    ) -> Result<CompactionResult, CompactionError>;
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum CompactionError {
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

impl From<GetDatasetError> for CompactionError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<auth::DatasetActionUnauthorizedError> for CompactionError {
    fn from(v: auth::DatasetActionUnauthorizedError) -> Self {
        match v {
            auth::DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            auth::DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<GetRefError> for CompactionError {
    fn from(v: GetRefError) -> Self {
        match v {
            GetRefError::NotFound(e) => Self::Internal(e.int_err()),
            GetRefError::Access(e) => Self::Access(e),
            GetRefError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<IterBlocksError> for CompactionError {
    fn from(v: IterBlocksError) -> Self {
        match v {
            IterBlocksError::Access(e) => CompactionError::Access(e),
            IterBlocksError::Internal(e) => CompactionError::Internal(e),
            _ => CompactionError::Internal(v.int_err()),
        }
    }
}

impl From<SetRefError> for CompactionError {
    fn from(v: SetRefError) -> Self {
        match v {
            SetRefError::Access(e) => CompactionError::Access(e),
            SetRefError::Internal(e) => CompactionError::Internal(e),
            _ => CompactionError::Internal(v.int_err()),
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
    fn success(&self, _res: &CompactionResult) {}
    fn error(&self, _err: &CompactionError) {}

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
    CommitNewBlocks,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompactionResult {
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
pub struct CompactionOptions {
    pub max_slice_size: Option<u64>,
    pub max_slice_records: Option<u64>,
    pub keep_metadata_only: bool,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            max_slice_size: Some(DEFAULT_MAX_SLICE_SIZE),
            max_slice_records: Some(DEFAULT_MAX_SLICE_RECORDS),
            keep_metadata_only: false,
        }
    }
}
