// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_task_system as ts;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowOutcome {
    /// Flow succeeded
    Success(FlowResult),
    /// Flow failed to complete, even after retry logic
    Failed,
    /// Flow was cancelled by a user
    Cancelled,
    /// Flow was aborted by system by force
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

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowResult {
    Empty,
    DatasetUpdate(FlowResultDatasetUpdate),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowResultDatasetUpdate {
    pub num_blocks: u64,
    pub num_records: u64,
    pub watermark_modified: bool,
}

impl From<&ts::TaskResult> for FlowResult {
    fn from(value: &ts::TaskResult) -> Self {
        match value {
            ts::TaskResult::Empty => Self::Empty,
            ts::TaskResult::UpdateDatasetResult(task_pull_result) => {
                Self::DatasetUpdate(FlowResultDatasetUpdate {
                    num_blocks: task_pull_result.num_blocks,
                    num_records: task_pull_result.num_records,
                    watermark_modified: task_pull_result.updated_watermark.is_some(),
                })
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
