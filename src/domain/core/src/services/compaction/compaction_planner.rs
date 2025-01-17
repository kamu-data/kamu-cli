// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::{ErrorIntoInternal, InternalError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

use crate::{CompactionListener, ResolvedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_MAX_SLICE_SIZE: u64 = 300_000_000;
pub const DEFAULT_MAX_SLICE_RECORDS: u64 = 10_000;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CompactionPlanner: Send + Sync {
    async fn plan_compaction(
        &self,
        target: ResolvedDataset,
        options: CompactionOptions,
        maybe_listener: Option<Arc<dyn CompactionListener>>,
    ) -> Result<CompactionPlan, CompactionPlanningError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CompactionPlan {
    pub seed: odf::Multihash,
    pub old_head: odf::Multihash,
    pub old_num_blocks: usize,
    pub offset_column_name: String,
    pub data_slice_batches: Vec<CompactionDataSliceBatch>,
}

impl CompactionPlan {
    pub fn has_no_effect(&self) -> bool {
        // slices amount +1(seed block) eq to amount of blocks we should not compact
        self.data_slice_batches.len() + 1 == self.old_num_blocks
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum CompactionDataSliceBatch {
    CompactedBatch(CompactionDataSliceBatchInfo),
    // Hash of block will not be None value in case
    // when we will get only one block in batch
    // and will be used tp not rewriting such blocks
    SingleBlock(odf::Multihash),
}

#[derive(Debug, Default, Clone)]
pub struct CompactionDataSliceBatchInfo {
    pub data_slices_batch: Vec<Url>,
    pub upper_bound: CompactionDataSliceBatchUpperBound,
    pub lower_bound: CompactionDataSliceBatchLowerBound,
}

#[derive(Debug, Default, Clone)]
pub struct CompactionDataSliceBatchUpperBound {
    pub new_source_state: Option<odf::metadata::SourceState>,
    pub new_watermark: Option<DateTime<Utc>>,
    pub new_checkpoint: Option<odf::Checkpoint>,
    pub end_offset: u64,
}

#[derive(Debug, Default, Clone)]
pub struct CompactionDataSliceBatchLowerBound {
    pub prev_offset: Option<u64>,
    pub prev_checkpoint: Option<odf::Multihash>,
    pub start_offset: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum CompactionPlanningError {
    #[error(transparent)]
    InvalidDatasetKind(
        #[from]
        #[backtrace]
        InvalidDatasetKindError,
    ),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Dataset '{dataset_alias}' in not root kind")]
pub struct InvalidDatasetKindError {
    pub dataset_alias: odf::DatasetAlias,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::storage::GetRefError> for CompactionPlanningError {
    fn from(v: odf::storage::GetRefError) -> Self {
        match v {
            odf::storage::GetRefError::NotFound(e) => Self::Internal(e.int_err()),
            odf::storage::GetRefError::Access(e) => Self::Access(e),
            odf::storage::GetRefError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::dataset::IterBlocksError> for CompactionPlanningError {
    fn from(v: odf::dataset::IterBlocksError) -> Self {
        match v {
            odf::dataset::IterBlocksError::Access(e) => Self::Access(e),
            odf::dataset::IterBlocksError::Internal(e) => Self::Internal(e),
            _ => CompactionPlanningError::Internal(v.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
