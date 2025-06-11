// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{CompactionResult, PullResult, PullResultUpToDate};
use kamu_task_system::{self as ts};
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowOutcome {
    /// Flow succeeded
    Success(FlowResult),
    /// Flow failed to complete, even after retry logic
    Failed,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowResultDatasetUpdate {
    Changed(FlowResultDatasetUpdateChanged),
    UpToDate(FlowResultDatasetUpdateUpToDate),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowResultDatasetUpdateChanged {
    pub old_head: Option<odf::Multihash>,
    pub new_head: odf::Multihash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowResultDatasetUpdateUpToDate {
    pub uncacheable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowResultDatasetCompact {
    pub new_head: odf::Multihash,
    pub old_num_blocks: usize,
    pub new_num_blocks: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowResultDatasetReset {
    pub new_head: odf::Multihash,
}

impl From<ts::TaskResult> for FlowResult {
    fn from(value: ts::TaskResult) -> Self {
        match value {
            ts::TaskResult::Empty => Self::Empty,
            ts::TaskResult::UpdateDatasetResult(task_update_result) => {
                match task_update_result.pull_result {
                    PullResult::UpToDate(up_to_date_result) => match up_to_date_result {
                        PullResultUpToDate::Sync | PullResultUpToDate::Transform => Self::Empty,
                        PullResultUpToDate::PollingIngest(result) => Self::DatasetUpdate(
                            FlowResultDatasetUpdate::UpToDate(FlowResultDatasetUpdateUpToDate {
                                uncacheable: result.uncacheable,
                            }),
                        ),
                        PullResultUpToDate::PushIngest(result) => Self::DatasetUpdate(
                            FlowResultDatasetUpdate::UpToDate(FlowResultDatasetUpdateUpToDate {
                                uncacheable: result.uncacheable,
                            }),
                        ),
                    },
                    PullResult::Updated { old_head, new_head } => {
                        Self::DatasetUpdate(FlowResultDatasetUpdate::Changed(
                            FlowResultDatasetUpdateChanged { old_head, new_head },
                        ))
                    }
                }
            }
            ts::TaskResult::ResetDatasetResult(task_reset_result) => {
                Self::DatasetReset(FlowResultDatasetReset {
                    new_head: task_reset_result.reset_result.new_head,
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
