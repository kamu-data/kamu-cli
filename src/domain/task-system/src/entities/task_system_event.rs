// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use enum_variants::*;

use super::*;

/////////////////////////////////////////////////////////////////////////////////////////

/// All events that model life-cycle of a task
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskSystemEvent {
    /// New task entered the queue
    TaskCreated(TaskCreated),
    /// Task execution had started
    TaskRunning(TaskRunning),
    /// Cancellation of task was requested (this is not immediate and task may
    /// still finish with a different outcome than cancelled)
    TaskCancelled(TaskCancelled),
    /// Task has reached a final outcome
    TaskFinished(TaskFinished),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCreated {
    pub event_time: DateTime<Utc>,
    pub task_id: TaskID,
    pub logical_plan: LogicalPlan,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRunning {
    pub event_time: DateTime<Utc>,
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCancelled {
    pub event_time: DateTime<Utc>,
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskFinished {
    pub event_time: DateTime<Utc>,
    pub task_id: TaskID,
    pub outcome: TaskOutcome,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl TaskSystemEvent {
    pub fn task_id(&self) -> TaskID {
        match self {
            TaskSystemEvent::TaskCreated(e) => e.task_id,
            TaskSystemEvent::TaskRunning(e) => e.task_id,
            TaskSystemEvent::TaskCancelled(e) => e.task_id,
            TaskSystemEvent::TaskFinished(e) => e.task_id,
        }
    }

    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            TaskSystemEvent::TaskCreated(e) => &e.event_time,
            TaskSystemEvent::TaskRunning(e) => &e.event_time,
            TaskSystemEvent::TaskCancelled(e) => &e.event_time,
            TaskSystemEvent::TaskFinished(e) => &e.event_time,
        }
    }
}

// TODO: Replace with derive macro
impl_enum_with_variants!(TaskSystemEvent);
impl_enum_variant!(TaskSystemEvent::TaskCreated(TaskCreated));
impl_enum_variant!(TaskSystemEvent::TaskRunning(TaskRunning));
impl_enum_variant!(TaskSystemEvent::TaskCancelled(TaskCancelled));
impl_enum_variant!(TaskSystemEvent::TaskFinished(TaskFinished));
