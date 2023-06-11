// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use super::*;

/// Represents the state of the task at specific point in time (projection)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskState {
    /// Unique and stable identitfier of this task
    pub task_id: TaskID,
    /// Life-cycle status of a task
    pub status: TaskStatus,
    /// Whether the task was ordered to be cancelled
    pub cancellation_requested: bool,
    /// Execution plan of the task
    pub logical_plan: LogicalPlan,

    /// Time when task was originally created and placed in a queue
    pub created_at: DateTime<Utc>,
    /// Time when task transitioned into a running state
    pub ran_at: Option<DateTime<Utc>>,
    /// Time when cancellation of task was requested
    pub cancellation_requested_at: Option<DateTime<Utc>>,
    /// Time when task has reached a final outcome
    pub finished_at: Option<DateTime<Utc>>,
}
