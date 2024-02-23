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
use opendatafabric::Multihash;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowResult {
    Empty,
    DatasetUpdate(FlowResultDatasetUpdate),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowResultDatasetUpdate {
    pub old_head: Option<Multihash>,
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
        }
    }
}

impl std::ops::AddAssign for FlowResult {
    fn add_assign(&mut self, rhs: Self) {
        match self {
            FlowResult::Empty => *self = rhs,
            FlowResult::DatasetUpdate(self_update) => match rhs {
                FlowResult::Empty => {}
                FlowResult::DatasetUpdate(rhs_update) => {
                    assert_eq!(Some(&self_update.new_head), rhs_update.old_head.as_ref());
                    self_update.new_head = rhs_update.new_head;
                }
            },
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
