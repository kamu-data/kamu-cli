// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::PullResult;
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
}

impl From<&ts::TaskResult> for FlowResult {
    fn from(value: &ts::TaskResult) -> Self {
        match value {
            ts::TaskResult::Empty => Self::Empty,
            ts::TaskResult::PullResult(pull_result) => match **pull_result {
                PullResult::UpToDate => Self::DatasetUpdate(FlowResultDatasetUpdate {
                    num_blocks: 0,
                    num_records: 0,
                }),
                PullResult::Updated {
                    num_blocks,
                    num_records,
                    ..
                } => Self::DatasetUpdate(FlowResultDatasetUpdate {
                    num_blocks,
                    num_records,
                }),
            },
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
