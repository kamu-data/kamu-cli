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
    /// Associated metadata
    pub metadata: TaskMetadata,
    /// Retry policy
    pub retry_policy: TaskRetryPolicy,
    /// Timing records
    pub timing: TaskTimingRecords,
}

impl TaskState {
    /// Computes the time when task recently started running, if it did
    pub fn ran_at(&self) -> Option<DateTime<Utc>> {
        self.attempts.last().map(|a| a.started_at)
    }

    /// Computes the time when task finished, if it did
    pub fn finished_at(&self) -> Option<DateTime<Utc>> {
        if self.status() == TaskStatus::Finished {
            self.attempts
                .last()
                .and_then(|a| a.attempt_result.as_ref())
                .map(|r| r.finished_at)
        } else {
            None
        }
    }

    /// Computes the outcome of the task
    pub fn outcome(&self) -> Option<&TaskOutcome> {
        let maybe_last_attempt = self.attempts.last();
        match maybe_last_attempt {
            None => None,
            Some(attempt) => {
                if attempt.attempt_result.is_none() || self.timing.next_attempt_at.is_some() {
                    None
                } else if self.timing.cancellation_requested_at.is_some() {
                    Some(&TaskOutcome::Cancelled)
                } else {
                    attempt.attempt_result.as_ref().map(|r| &r.outcome)
                }
            }
        }
    }

    /// Computes status
    pub fn status(&self) -> TaskStatus {
        let maybe_last_attempt = self.attempts.last();
        match maybe_last_attempt {
            None => TaskStatus::Queued,
            Some(attempt) => {
                if attempt.attempt_result.is_none() {
                    TaskStatus::Running
                } else if self.timing.next_attempt_at.is_some() {
                    TaskStatus::Retrying
                } else {
                    TaskStatus::Finished
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct TaskTimingRecords {
    /// Time when task was originally created and placed in a queue
    pub created_at: DateTime<Utc>,

    /// Time when it's allowed to start running the next attempt
    pub next_attempt_at: Option<DateTime<Utc>>,

    /// Time when cancellation of task was requested
    pub cancellation_requested_at: Option<DateTime<Utc>>,
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
                    retry_policy,
                }) => Ok(Self {
                    task_id,
                    attempts: vec![],
                    logical_plan,
                    metadata: metadata.unwrap_or_default(),
                    retry_policy,
                    timing: TaskTimingRecords {
                        created_at: event_time,
                        next_attempt_at: Some(event_time),
                        cancellation_requested_at: None,
                    },
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(s.task_id, event.task_id());

                match event {
                    // May run when queued or retrying, unless cancellation was requested
                    E::TaskRunning(TaskEventRunning { event_time, .. }) => match s.status() {
                        TaskStatus::Queued | TaskStatus::Retrying
                            if s.timing.cancellation_requested_at.is_none() =>
                        {
                            let mut attempts = s.attempts;
                            attempts.push(TaskAttempt {
                                attempt_number: u32::try_from(attempts.len()).unwrap() + 1,
                                started_at: event_time,
                                attempt_result: None,
                            });

                            Ok(Self {
                                attempts,
                                timing: TaskTimingRecords {
                                    next_attempt_at: None,
                                    ..s.timing
                                },
                                ..s
                            })
                        }

                        _ => Err(ProjectionError::new(Some(s), event)),
                    },

                    // Maybe be requeued when running, unless cancellation was requested
                    E::TaskRequeued(TaskEventRequeued { event_time, .. })
                        if s.status() == TaskStatus::Running
                            && s.timing.cancellation_requested_at.is_none() =>
                    {
                        let mut attempts = s.attempts;
                        attempts.pop();

                        Ok(Self {
                            attempts,
                            timing: TaskTimingRecords {
                                next_attempt_at: Some(event_time),
                                ..s.timing
                            },
                            ..s
                        })
                    }

                    // May cancel in all states except finished or already cancelled
                    E::TaskCancelled(TaskEventCancelled { event_time, .. }) => match s.status() {
                        TaskStatus::Queued | TaskStatus::Retrying | TaskStatus::Running
                            if s.timing.cancellation_requested_at.is_none() =>
                        {
                            Ok(Self {
                                timing: TaskTimingRecords {
                                    cancellation_requested_at: Some(event_time),
                                    next_attempt_at: None,
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
                    }) if s.status() == TaskStatus::Running => {
                        // Record the result of the last attempt
                        let mut attempts = s.attempts;
                        let last_attempt = attempts.last_mut().unwrap();
                        last_attempt.attempt_result = Some(TaskAttemptResult {
                            finished_at: event_time,
                            outcome,
                        });

                        // Compute if there will be a next attempt and when
                        let next_attempt_at = s
                            .retry_policy
                            .next_attempt_at(u32::try_from(attempts.len()).unwrap(), event_time);

                        Ok(Self {
                            attempts,
                            timing: TaskTimingRecords {
                                next_attempt_at,
                                ..s.timing
                            },
                            ..s
                        })
                    }

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
