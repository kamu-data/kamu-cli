// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_task_system as ts;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Life-cycle status of a task
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// Task is waiting for capacity to be allocated to it
    Queued,
    /// Task is being executed
    Running,
    /// Task has failed and is scheduled for retry
    Retrying,
    /// Task has reached a certain final outcome (see [`TaskOutcome`])
    Finished,
}

impl From<&ts::TaskStatus> for TaskStatus {
    fn from(v: &ts::TaskStatus) -> Self {
        match v {
            ts::TaskStatus::Queued => Self::Queued,
            ts::TaskStatus::Running => Self::Running,
            ts::TaskStatus::Retrying => Self::Retrying,
            ts::TaskStatus::Finished => Self::Finished,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a certain final outcome of the task
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcome {
    /// Task succeeded
    Success,
    /// Task failed to complete
    Failed,
    /// Task was cancelled by a user
    Cancelled,
}

impl From<&ts::TaskOutcome> for TaskOutcome {
    fn from(value: &ts::TaskOutcome) -> Self {
        match value {
            ts::TaskOutcome::Success(_) => Self::Success,
            ts::TaskOutcome::Failed(_) => Self::Failed,
            ts::TaskOutcome::Cancelled => Self::Cancelled,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
