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
    /// List of attempts to execute this task
    pub attempts: Vec<TaskAttempt>,
    /// Execution plan of the task
    pub logical_plan: LogicalPlan,
    /// Optional associated metadata
    pub metadata: TaskMetadata,
    /// Time when task was originally created and placed in a queue
    pub created_at: DateTime<Utc>,
    /// Time when cancellation of task was requested
    pub cancellation_requested_at: Option<DateTime<Utc>>,
}

impl TaskState {
    /// Computes the time when task recently started running, if it did
    pub fn ran_at(&self) -> Option<DateTime<Utc>> {
        self.attempts.last().map(|a| a.started_at)
    }

    /// Computes the time when task finished, if it did
    pub fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.attempts
            .last()
            .and_then(|attempt| attempt.attempt_result.as_ref().map(|r| r.finished_at))
    }

    /// Computes the outcome of the task
    pub fn outcome(&self) -> Option<&TaskOutcome> {
        self.attempts
            .last()
            .and_then(|attempt| attempt.attempt_result.as_ref().map(|r| &r.outcome))
    }

    /// Computes status
    pub fn status(&self) -> TaskStatus {
        let last_attempt = self.attempts.last();
        match last_attempt {
            Some(attempt) if attempt.attempt_result.is_some() => TaskStatus::Finished,
            Some(_) => TaskStatus::Running,
            None => TaskStatus::Queued,
        }
    }
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
                    attempts: vec![],
                    logical_plan,
                    metadata: metadata.unwrap_or_default(),
                    created_at: event_time,
                    cancellation_requested_at: None,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(s.task_id, event.task_id());

                match event {
                    E::TaskRunning(TaskEventRunning { event_time, .. })
                        if s.status() == TaskStatus::Queued =>
                    {
                        let mut attempts = s.attempts;
                        attempts.push(TaskAttempt {
                            attempt_number: u32::try_from(attempts.len()).unwrap() + 1,
                            started_at: event_time,
                            attempt_result: None,
                        });

                        Ok(Self { attempts, ..s })
                    }
                    E::TaskRequeued(_)
                        if s.status() == TaskStatus::Running
                            && s.cancellation_requested_at.is_none() =>
                    {
                        let mut attempts = s.attempts;
                        attempts.pop();

                        Ok(Self { attempts, ..s })
                    }
                    E::TaskCancelled(TaskEventCancelled { event_time, .. })
                        if s.status() == TaskStatus::Queued
                            || s.status() == TaskStatus::Running
                                && s.cancellation_requested_at.is_none() =>
                    {
                        Ok(Self {
                            cancellation_requested_at: Some(event_time),
                            ..s
                        })
                    }
                    E::TaskFinished(TaskEventFinished {
                        event_time,
                        outcome,
                        ..
                    }) if s.status() == TaskStatus::Running => {
                        let mut attempts = s.attempts;
                        let last_attempt = attempts.last_mut().unwrap();

                        last_attempt.attempt_result = Some(TaskAttemptResult {
                            finished_at: event_time,
                            outcome: if s.cancellation_requested_at.is_some() {
                                TaskOutcome::Cancelled
                            } else {
                                outcome
                            },
                        });
                        Ok(Self { attempts, ..s })
                    }
                    E::TaskCreated(_)
                    | E::TaskRunning(_)
                    | E::TaskRequeued(_)
                    | E::TaskCancelled(_)
                    | E::TaskFinished(_) => Err(ProjectionError::new(Some(s), event)),
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
