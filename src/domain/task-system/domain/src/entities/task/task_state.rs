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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents the state of the task at specific point in time (projection)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskState {
    /// Unique and stable identifier of this task
    pub task_id: TaskID,
    /// Outcome of a task
    pub outcome: Option<TaskOutcome>,
    /// Execution plan of the task
    pub logical_plan: LogicalPlan,
    /// Associated metadata
    pub metadata: TaskMetadata,
    /// Timing records
    pub timing: TaskTimingRecords,
}

impl TaskState {
    /// Computes the time when task recently started running, if it did
    pub fn ran_at(&self) -> Option<DateTime<Utc>> {
        self.timing.ran_at
    }

    /// Computes the time when task finished, if it did
    pub fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.timing.finished_at
    }

    /// Computes the outcome of the task
    pub fn outcome(&self) -> Option<&TaskOutcome> {
        self.outcome.as_ref()
    }

    /// Computes status
    pub fn status(&self) -> TaskStatus {
        if self.outcome.is_some() {
            TaskStatus::Finished
        } else if self.timing.ran_at.is_some() {
            TaskStatus::Running
        } else {
            TaskStatus::Queued
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct TaskTimingRecords {
    /// Time when task was originally created and placed in a queue
    pub created_at: DateTime<Utc>,

    /// Time when task transitioned into a running state
    pub ran_at: Option<DateTime<Utc>>,

    /// Time when cancellation of task was requested
    pub cancellation_requested_at: Option<DateTime<Utc>>,

    /// Time when task has reached a final outcome
    pub finished_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
                    metadata,
                }) => Ok(Self {
                    task_id,
                    outcome: None,
                    logical_plan,
                    metadata: metadata.unwrap_or_default(),
                    timing: TaskTimingRecords {
                        created_at: event_time,
                        ran_at: None,
                        finished_at: None,
                        cancellation_requested_at: None,
                    },
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(s.task_id, event.task_id());

                match event {
                    // May run when queued, unless cancellation was requested
                    E::TaskRunning(TaskEventRunning { event_time, .. }) => match s.status() {
                        TaskStatus::Queued if s.timing.cancellation_requested_at.is_none() => {
                            Ok(Self {
                                timing: TaskTimingRecords {
                                    ran_at: Some(event_time),
                                    ..s.timing
                                },
                                ..s
                            })
                        }

                        _ => Err(ProjectionError::new(Some(s), event)),
                    },

                    // Maybe be requeued when running, unless cancellation was requested
                    E::TaskRequeued(TaskEventRequeued { .. })
                        if s.status() == TaskStatus::Running
                            && s.timing.cancellation_requested_at.is_none() =>
                    {
                        Ok(Self {
                            timing: TaskTimingRecords {
                                ran_at: None,
                                ..s.timing
                            },
                            ..s
                        })
                    }

                    // May cancel in all states except finished or already cancelled
                    E::TaskCancelled(TaskEventCancelled { event_time, .. }) => match s.status() {
                        TaskStatus::Queued | TaskStatus::Running
                            if s.timing.cancellation_requested_at.is_none() =>
                        {
                            Ok(Self {
                                timing: TaskTimingRecords {
                                    cancellation_requested_at: Some(event_time),
                                    ..s.timing
                                },
                                ..s
                            })
                        }
                        _ => Err(ProjectionError::new(Some(s), event)),
                    },

                    // May finish only if running
                    E::TaskFinished(TaskEventFinished {
                        event_time,
                        outcome,
                        ..
                    }) if s.status() == TaskStatus::Running => Ok(Self {
                        outcome: Some(outcome),
                        timing: TaskTimingRecords {
                            finished_at: Some(event_time),
                            ..s.timing
                        },
                        ..s
                    }),

                    E::TaskCreated(_) | E::TaskRequeued(_) | E::TaskFinished(_) => {
                        Err(ProjectionError::new(Some(s), event))
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ProjectionEvent<TaskID> for TaskEvent {
    fn matches_query(&self, query: &TaskID) -> bool {
        self.task_id() == *query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
