// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{CompactionResult, PullResult, ResetResult};
use opendatafabric as odf;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "task_status_type", rename_all = "snake_case")]
pub enum TaskStatus {
    /// Task is waiting for capacity to be allocated to it
    Queued,
    /// Task is being executed
    Running,
    /// Task has reached a certain final outcome
    Finished,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskOutcome {
    /// Task succeeded
    Success(TaskResult),
    /// Task failed to complete
    Failed(TaskError),
    /// Task was cancelled by a user
    Cancelled,
    // /// Task was dropped in favor of another task
    // Replaced(TaskID),
}

impl TaskOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success(_))
    }

    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskResult {
    Empty,
    UpdateDatasetResult(TaskUpdateDatasetResult),
    ResetDatasetResult(TaskResetDatasetResult),
    CompactionDatasetResult(TaskCompactionDatasetResult),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskUpdateDatasetResult {
    pub pull_result: PullResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResetDatasetResult {
    pub reset_result: ResetResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCompactionDatasetResult {
    pub compaction_result: CompactionResult,
}

impl From<CompactionResult> for TaskCompactionDatasetResult {
    fn from(value: CompactionResult) -> Self {
        Self {
            compaction_result: value,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskError {
    Empty,
    UpdateDatasetError(UpdateDatasetTaskError),
    ResetDatasetError(ResetDatasetTaskError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateDatasetTaskError {
    InputDatasetCompacted(InputDatasetCompactedError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputDatasetCompactedError {
    pub dataset_id: odf::DatasetID,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResetDatasetTaskError {
    ResetHeadNotFound,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
