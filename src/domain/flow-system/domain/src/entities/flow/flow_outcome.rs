// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{CompactionResult, PullResult};
use kamu_task_system::{self as ts, ResetDatasetTaskError, UpdateDatasetTaskError};
use opendatafabric::{DatasetID, Multihash};
use ts::TaskError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowOutcome {
    /// Flow succeeded
    Success(FlowResult),
    /// Flow failed to complete, even after retry logic
    Failed(FlowError),
    /// Flow was aborted by user or by system
    Aborted,
}

impl FlowOutcome {
    pub fn try_result_as_ref(&self) -> Option<&FlowResult> {
        match self {
            Self::Success(flow_result) => Some(flow_result),
            _ => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowResult {
    Empty,
    DatasetUpdate(FlowResultDatasetUpdate),
    DatasetCompact(FlowResultDatasetCompact),
    DatasetReset(FlowResultDatasetReset),
}

impl FlowResult {
    pub fn is_empty(&self) -> bool {
        match self {
            FlowResult::Empty => true,
            FlowResult::DatasetUpdate(_)
            | FlowResult::DatasetCompact(_)
            | FlowResult::DatasetReset(_) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowError {
    Failed,
    RootDatasetCompacted(FlowRootDatasetCompactedError),
    NewHeadHashNotFound(FlowResetNewHeadHashNotFoundError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowRootDatasetCompactedError {
    pub dataset_id: DatasetID,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowResetNewHeadHashNotFoundError {
    pub head_hash: Multihash,
}

impl From<&TaskError> for FlowError {
    fn from(value: &TaskError) -> Self {
        match value {
            TaskError::Empty => Self::Failed,
            TaskError::UpdateDatasetError(update_dataset_error) => match update_dataset_error {
                UpdateDatasetTaskError::RootDatasetCompacted(err) => {
                    Self::RootDatasetCompacted(FlowRootDatasetCompactedError {
                        dataset_id: err.dataset_id.clone(),
                    })
                }
            },
            TaskError::ResetDatasetError(reset_dataset_error) => match reset_dataset_error {
                ResetDatasetTaskError::NewHeadHashNotFound(err) => {
                    Self::NewHeadHashNotFound(FlowResetNewHeadHashNotFoundError {
                        head_hash: err.head_hash.clone(),
                    })
                }
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowResultDatasetUpdate {
    pub old_head: Option<Multihash>,
    pub new_head: Multihash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowResultDatasetCompact {
    pub new_head: Multihash,
    pub old_num_blocks: usize,
    pub new_num_blocks: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowResultDatasetReset {
    pub new_head: Multihash,
}

impl From<ts::TaskResult> for FlowResult {
    fn from(value: ts::TaskResult) -> Self {
        match value {
            ts::TaskResult::Empty => Self::Empty,
            ts::TaskResult::UpdateDatasetResult(task_update_result) => {
                match task_update_result.pull_result {
                    PullResult::UpToDate => Self::Empty,
                    PullResult::Updated { old_head, new_head } => {
                        Self::DatasetUpdate(FlowResultDatasetUpdate { old_head, new_head })
                    }
                }
            }
            ts::TaskResult::ResetDatasetResult(task_reset_result) => {
                Self::DatasetReset(FlowResultDatasetReset {
                    new_head: task_reset_result.new_head,
                })
            }
            ts::TaskResult::CompactionDatasetResult(task_compaction_result) => {
                match task_compaction_result.compaction_result {
                    CompactionResult::NothingToDo => Self::Empty,
                    CompactionResult::Success {
                        new_head,
                        old_num_blocks,
                        new_num_blocks,
                        ..
                    } => Self::DatasetCompact(FlowResultDatasetCompact {
                        new_head,
                        old_num_blocks,
                        new_num_blocks,
                    }),
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
