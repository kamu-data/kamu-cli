// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::compact_service::CompactResult;
use kamu_core::PullResult;
use opendatafabric::DatasetID;
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    /// Task is waiting for capacity to be allocated to it
    Queued,
    /// Task is being executed
    Running,
    /// Task has reached a certain final outcome
    Finished(TaskOutcome),
}

/////////////////////////////////////////////////////////////////////////////////////////

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
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskResult {
    Empty,
    UpdateDatasetResult(TaskUpdateDatasetResult),
    CompactDatasetResult(TaskCompactDatasetResult),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskUpdateDatasetResult {
    pub pull_result: PullResult,
}

impl From<PullResult> for TaskUpdateDatasetResult {
    fn from(value: PullResult) -> Self {
        Self { pull_result: value }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCompactDatasetResult {
    pub compact_result: CompactResult,
}

impl From<CompactResult> for TaskCompactDatasetResult {
    fn from(value: CompactResult) -> Self {
        Self {
            compact_result: value,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskError {
    Empty,
    RootDatasetWasCompacted(RootDatasetWasCompactedError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootDatasetWasCompactedError {
    pub dataset_id: DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////////////
