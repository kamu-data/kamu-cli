// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use enum_variants::*;

use super::*;

/////////////////////////////////////////////////////////////////////////////////////////

/// All events that model life-cycle of a task
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskEvent {
    /// New task entered the queue
    Created(TaskCreated),
    /// Task execution had started
    Running(TaskRunning),
    /// Cancellation of task was requested (this is not immediate and task may
    /// still finish with a different outcome than cancelled)
    Cancelled(TaskCancelled),
    /// Task has reached a final outcome
    Finished(TaskFinished),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCreated {
    pub task_id: TaskID,
    pub logical_plan: LogicalPlan,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRunning {
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCancelled {
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskFinished {
    pub task_id: TaskID,
    pub outcome: TaskOutcome,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl TaskEvent {
    pub fn task_id(&self) -> TaskID {
        match self {
            TaskEvent::Created(e) => e.task_id,
            TaskEvent::Running(e) => e.task_id,
            TaskEvent::Cancelled(e) => e.task_id,
            TaskEvent::Finished(e) => e.task_id,
        }
    }
}

// TODO: Replace with derive macro
impl_enum_with_variants!(TaskEvent);
impl_enum_variant!(TaskEvent::Created(TaskCreated));
impl_enum_variant!(TaskEvent::Running(TaskRunning));
impl_enum_variant!(TaskEvent::Cancelled(TaskCancelled));
impl_enum_variant!(TaskEvent::Finished(TaskFinished));
