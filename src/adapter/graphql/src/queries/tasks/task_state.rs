// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use kamu_task_system as ts;

use crate::scalars::*;

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TaskState {
    pub task_id: TaskID,
    pub status: TaskStatus,
    pub outcome: Option<TaskOutcome>,
    //pub logical_plan: LogicalPlan,
}

impl From<ts::TaskState> for TaskState {
    fn from(v: ts::TaskState) -> Self {
        // Unpack so that any update to domain model forces us to update this code
        let ts::TaskState {
            task_id,
            status,
            logical_plan: _,
        } = v;

        // Un-nest enum into a field
        let outcome = match &status {
            ts::TaskStatus::Queued | ts::TaskStatus::Running => None,
            ts::TaskStatus::Finished(outcome) => Some((*outcome).into()),
        };

        Self {
            task_id: task_id.into(),
            status: status.into(),
            outcome,
            //logical_plan: v.logical_plan.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// Task is waiting for capacity to be allocated to it
    Queued,
    /// Task is being executed
    Running,
    /// Task has reached a certain final outcome (see [TaskState::outcome]
    /// field)
    Finished,
}

impl From<ts::TaskStatus> for TaskStatus {
    fn from(v: ts::TaskStatus) -> Self {
        match v {
            ts::TaskStatus::Queued => Self::Queued,
            ts::TaskStatus::Running => Self::Running,
            ts::TaskStatus::Finished(_) => Self::Finished,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcome {
    /// Task succeeded
    Success,
    /// Task failed to complete
    Failed,
    /// Task was cancelled by a user
    Cancelled,
}

impl From<ts::TaskOutcome> for TaskOutcome {
    fn from(v: ts::TaskOutcome) -> Self {
        match v {
            ts::TaskOutcome::Success => Self::Success,
            ts::TaskOutcome::Failed => Self::Failed,
            ts::TaskOutcome::Cancelled => Self::Cancelled,
        }
    }
}
