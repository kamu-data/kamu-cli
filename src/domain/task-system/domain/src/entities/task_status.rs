// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{CompactionResult, PullResult, UpToDateResult};
use opendatafabric::DatasetID;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStatus {
    /// Task is waiting for capacity to be allocated to it
    Queued,
    /// Task is being executed
    Running,
    /// Task has reached a certain final outcome
    Finished(TaskOutcome),
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskResult {
    Empty,
    UpdateDatasetResult(TaskUpdateDatasetResult),
    CompactionDatasetResult(TaskCompactionDatasetResult),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskUpdateDatasetResult {
    pub pull_result: PullResult,
}

impl TaskUpdateDatasetResult {
    pub fn from_pull_result(pull_result: &PullResult, fetch_uncacheable: bool) -> Self {
        match pull_result {
            PullResult::Updated { .. } => TaskUpdateDatasetResult {
                pull_result: pull_result.clone(),
            },
            PullResult::UpToDate(up_to_date_result) => match up_to_date_result {
                UpToDateResult::UpToDate => TaskUpdateDatasetResult {
                    pull_result: pull_result.clone(),
                },
                UpToDateResult::IngestUpToDate { uncacheable } => TaskUpdateDatasetResult {
                    pull_result: PullResult::UpToDate(UpToDateResult::IngestUpToDate {
                        uncacheable: *uncacheable && fetch_uncacheable,
                    }),
                },
            },
        }
    }
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateDatasetTaskError {
    RootDatasetCompacted(RootDatasetCompactedError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootDatasetCompactedError {
    pub dataset_id: DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
