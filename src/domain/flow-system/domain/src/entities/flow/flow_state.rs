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
use kamu_task_system::{self as ts};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FlowState {
    /// Unique flow identifier
    pub flow_id: FlowID,
    /// Flow binding
    pub flow_binding: FlowBinding,
    /// Activation causes
    pub activation_causes: Vec<FlowActivationCause>,
    /// Start condition (if defined)
    pub start_condition: Option<FlowStartCondition>,
    /// Timing records
    pub timing: FlowTimingRecords,
    /// Associated task IDs
    pub task_ids: Vec<ts::TaskID>,
    /// Flow outcome
    pub outcome: Option<FlowOutcome>,
    /// Flow config snapshot on the moment when flow was initiated
    pub config_snapshot: Option<FlowConfigurationRule>,
    /// Retry policy used
    pub retry_policy: Option<RetryPolicy>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FlowTimingRecords {
    /// Flow scheduled and will be activated at time
    pub scheduled_for_activation_at: Option<DateTime<Utc>>,
    /// Task scheduled and waiting for execution since time
    pub awaiting_executor_since: Option<DateTime<Utc>>,
    /// Started running at time
    pub running_since: Option<DateTime<Utc>>,
    /// Finish time (success or cancel/abort)
    pub last_attempt_finished_at: Option<DateTime<Utc>>,
}

impl FlowState {
    /// Extract primary activation cause
    pub fn primary_activation_cause(&self) -> &FlowActivationCause {
        // At least 1 cause is initially defined for sure
        self.activation_causes.first().unwrap()
    }

    /// Computes status
    pub fn status(&self) -> FlowStatus {
        if self.outcome.is_some() {
            FlowStatus::Finished
        } else if self.timing.last_attempt_finished_at.is_some() {
            FlowStatus::Retrying
        } else if self.timing.running_since.is_some() {
            FlowStatus::Running
        } else {
            FlowStatus::Waiting
        }
    }

    pub fn try_task_result_as_ref(&self) -> Option<&ts::TaskResult> {
        self.outcome
            .as_ref()
            .and_then(|outcome| outcome.try_task_result_as_ref())
    }

    pub fn can_schedule(&self) -> bool {
        matches!(self.status(), FlowStatus::Waiting | FlowStatus::Retrying)
    }
}

impl Projection for FlowState {
    type Query = FlowID;
    type Event = FlowEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use FlowEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::Initiated(FlowEventInitiated {
                    event_time: _,
                    flow_id,
                    flow_binding,
                    activation_cause,
                    config_snapshot,
                    retry_policy,
                }) => Ok(Self {
                    flow_id,
                    flow_binding,
                    activation_causes: vec![activation_cause],
                    start_condition: None,
                    timing: FlowTimingRecords {
                        scheduled_for_activation_at: None,
                        awaiting_executor_since: None,
                        running_since: None,
                        last_attempt_finished_at: None,
                    },
                    task_ids: vec![],
                    config_snapshot,
                    outcome: None,
                    retry_policy,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },

            (Some(s), event) => {
                assert_eq!(s.flow_id, event.flow_id());

                match event {
                    E::Initiated(_) => Err(ProjectionError::new(Some(s), event)),

                    E::StartConditionUpdated(FlowEventStartConditionUpdated {
                        start_condition,
                        ..
                    }) => {
                        if s.outcome.is_some() || s.timing.awaiting_executor_since.is_some() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(FlowState {
                                start_condition: Some(start_condition),
                                ..s
                            })
                        }
                    }

                    E::ConfigSnapshotModified(FlowConfigSnapshotModified {
                        config_snapshot,
                        ..
                    }) => Ok(FlowState {
                        config_snapshot: Some(config_snapshot),
                        ..s
                    }),

                    E::ActivationCauseAdded(FlowEventActivationCauseAdded {
                        ref activation_cause,
                        ..
                    }) => {
                        if s.outcome.is_some() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            let mut activation_causes = s.activation_causes;
                            activation_causes.push(activation_cause.clone());
                            Ok(FlowState {
                                activation_causes,
                                ..s
                            })
                        }
                    }

                    E::ScheduledForActivation(FlowEventScheduledForActivation {
                        scheduled_for_activation_at,
                        ..
                    }) => {
                        if s.outcome.is_some() || s.timing.awaiting_executor_since.is_some() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(FlowState {
                                timing: FlowTimingRecords {
                                    scheduled_for_activation_at: Some(scheduled_for_activation_at),
                                    awaiting_executor_since: None,
                                    running_since: None,
                                    last_attempt_finished_at: None,
                                },
                                ..s
                            })
                        }
                    }

                    E::TaskScheduled(FlowEventTaskScheduled {
                        event_time,
                        task_id,
                        ..
                    }) => {
                        if s.outcome.is_some() || s.timing.scheduled_for_activation_at.is_none() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            let mut task_ids = s.task_ids;
                            task_ids.push(task_id);
                            Ok(FlowState {
                                task_ids,
                                timing: FlowTimingRecords {
                                    awaiting_executor_since: Some(event_time),
                                    ..s.timing
                                },
                                ..s
                            })
                        }
                    }

                    E::TaskRunning(FlowEventTaskRunning {
                        event_time,
                        task_id,
                        ..
                    }) => {
                        if s.outcome.is_some()
                            || !s.task_ids.contains(&task_id)
                            || s.timing.awaiting_executor_since.is_none()
                        {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(FlowState {
                                timing: FlowTimingRecords {
                                    running_since: Some(event_time),
                                    last_attempt_finished_at: None, // Reset for case of retry
                                    ..s.timing
                                },
                                start_condition: None,
                                ..s
                            })
                        }
                    }

                    E::TaskFinished(FlowEventTaskFinished {
                        event_time,
                        task_id,
                        ref task_outcome,
                        next_attempt_at,
                        ..
                    }) => {
                        if !s.task_ids.contains(&task_id)
                            || s.timing.running_since.is_none()
                            || s.start_condition.is_some()
                        {
                            Err(ProjectionError::new(Some(s), event))
                        } else if s.outcome.is_some() {
                            // Ignore for idempotence motivation
                            Ok(s)
                        } else {
                            let timing = FlowTimingRecords {
                                last_attempt_finished_at: Some(event_time),
                                ..s.timing
                            };
                            match task_outcome {
                                ts::TaskOutcome::Success(task_result) => Ok(FlowState {
                                    outcome: Some(FlowOutcome::Success(task_result.clone())),
                                    timing,
                                    ..s
                                }),
                                ts::TaskOutcome::Cancelled => Ok(FlowState {
                                    outcome: Some(FlowOutcome::Aborted),
                                    timing,
                                    ..s
                                }),
                                ts::TaskOutcome::Failed(_) => {
                                    // Retry logic - don't set outcome yet
                                    if let Some(next_attempt_at) = next_attempt_at {
                                        Ok(FlowState {
                                            timing: FlowTimingRecords {
                                                // Next task will have to be scheduled
                                                awaiting_executor_since: None,
                                                // No longer running
                                                running_since: None,
                                                // Keep finished time to distinguish Retrying status
                                                last_attempt_finished_at: timing
                                                    .last_attempt_finished_at,
                                                // The scheduling time is defined via retry policy
                                                scheduled_for_activation_at: Some(next_attempt_at),
                                            },
                                            ..s
                                        })
                                    } else {
                                        Ok(FlowState {
                                            outcome: Some(FlowOutcome::Failed),
                                            timing,
                                            ..s
                                        })
                                    }
                                }
                            }
                        }
                    }

                    E::Aborted(FlowEventAborted { event_time, .. }) => {
                        if let Some(outcome) = &s.outcome {
                            if let FlowOutcome::Success(_) = outcome {
                                Err(ProjectionError::new(Some(s), event))
                            } else {
                                // Ignore for idempotence reasons
                                Ok(s)
                            }
                        } else {
                            Ok(FlowState {
                                outcome: Some(FlowOutcome::Aborted),
                                timing: FlowTimingRecords {
                                    last_attempt_finished_at: Some(event_time),
                                    ..s.timing
                                },
                                start_condition: None,
                                ..s
                            })
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ProjectionEvent<FlowID> for FlowEvent {
    fn matches_query(&self, query: &FlowID) -> bool {
        self.flow_id() == *query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
