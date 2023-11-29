// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::*;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

impl Projection for TaskState {
    type Query = TaskID;
    type Event = TaskEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use TaskEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::TaskCreated(TaskEventCreated {
                    event_time,
                    task_id,
                    logical_plan,
                }) => Ok(Self {
                    task_id,
                    status: TaskStatus::Queued,
                    cancellation_requested: false,
                    logical_plan,
                    created_at: event_time,
                    ran_at: None,
                    cancellation_requested_at: None,
                    finished_at: None,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(s.task_id, event.task_id());

                match event {
                    E::TaskCreated(_) => Err(ProjectionError::new(Some(s), event)),
                    E::TaskRunning(TaskEventRunning {
                        event_time,
                        task_id: _,
                    }) if s.status == TaskStatus::Queued => Ok(Self {
                        status: TaskStatus::Running,
                        ran_at: Some(event_time),
                        ..s
                    }),
                    E::TaskCancelled(TaskEventCancelled {
                        event_time,
                        task_id: _,
                    }) if s.status == TaskStatus::Queued
                        || s.status == TaskStatus::Running && !s.cancellation_requested =>
                    {
                        Ok(Self {
                            cancellation_requested: true,
                            cancellation_requested_at: Some(event_time),
                            ..s
                        })
                    }
                    E::TaskFinished(TaskEventFinished {
                        event_time,
                        task_id: _,
                        outcome,
                    }) if s.status == TaskStatus::Queued || s.status == TaskStatus::Running => {
                        Ok(Self {
                            status: TaskStatus::Finished(outcome),
                            finished_at: Some(event_time),
                            ..s
                        })
                    }
                    E::TaskRunning(_) | E::TaskCancelled(_) | E::TaskFinished(_) => {
                        Err(ProjectionError::new(Some(s), event))
                    }
                }
            }
        }
    }
}
