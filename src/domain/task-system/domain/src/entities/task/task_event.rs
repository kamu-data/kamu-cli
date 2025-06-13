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
use serde::{Deserialize, Serialize};

use super::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// All events that model life-cycle of a task
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskEvent {
    /// New task entered the queue
    TaskCreated(TaskEventCreated),
    /// Task execution had started
    TaskRunning(TaskEventRunning),
    /// Task execution has re-queued (switched from Running back to Queued)
    TaskRequeued(TaskEventRequeued),
    /// Cancellation of task was requested (this is not immediate and task may
    /// still finish with a different outcome than cancelled)
    TaskCancelled(TaskEventCancelled),
    /// Task has reached a final outcome
    TaskFinished(TaskEventFinished),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEventCreated {
    pub event_time: DateTime<Utc>,
    pub task_id: TaskID,
    pub logical_plan: LogicalPlan,
    pub metadata: Option<TaskMetadata>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEventRunning {
    pub event_time: DateTime<Utc>,
    pub task_id: TaskID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEventRequeued {
    pub event_time: DateTime<Utc>,
    pub task_id: TaskID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEventCancelled {
    pub event_time: DateTime<Utc>,
    pub task_id: TaskID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskEventFinished {
    pub event_time: DateTime<Utc>,
    pub task_id: TaskID,
    pub outcome: TaskOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TaskEvent {
    pub fn typename(&self) -> &'static str {
        match self {
            TaskEvent::TaskCreated(_) => "TaskEventCreated",
            TaskEvent::TaskRunning(_) => "TaskEventRunning",
            TaskEvent::TaskRequeued(_) => "TaskEventRequeued",
            TaskEvent::TaskCancelled(_) => "TaskEventCancelled",
            TaskEvent::TaskFinished(_) => "TaskEventFinished",
        }
    }

    pub fn task_id(&self) -> TaskID {
        match self {
            TaskEvent::TaskCreated(e) => e.task_id,
            TaskEvent::TaskRunning(e) => e.task_id,
            TaskEvent::TaskRequeued(e) => e.task_id,
            TaskEvent::TaskCancelled(e) => e.task_id,
            TaskEvent::TaskFinished(e) => e.task_id,
        }
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            TaskEvent::TaskCreated(e) => e.event_time,
            TaskEvent::TaskRunning(e) => e.event_time,
            TaskEvent::TaskRequeued(e) => e.event_time,
            TaskEvent::TaskCancelled(e) => e.event_time,
            TaskEvent::TaskFinished(e) => e.event_time,
        }
    }

    pub fn new_status(&self) -> TaskStatus {
        match self {
            TaskEvent::TaskCreated(_) | TaskEvent::TaskRequeued(_) => TaskStatus::Queued,
            TaskEvent::TaskRunning(_) => TaskStatus::Running,
            TaskEvent::TaskCancelled(_) | TaskEvent::TaskFinished(_) => TaskStatus::Finished,
        }
    }
}

// TODO: Replace with derive macro
impl_enum_with_variants!(TaskEvent);
impl_enum_variant!(TaskEvent::TaskCreated(TaskEventCreated));
impl_enum_variant!(TaskEvent::TaskRunning(TaskEventRunning));
impl_enum_variant!(TaskEvent::TaskRequeued(TaskEventRequeued));
impl_enum_variant!(TaskEvent::TaskCancelled(TaskEventCancelled));
impl_enum_variant!(TaskEvent::TaskFinished(TaskEventFinished));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
