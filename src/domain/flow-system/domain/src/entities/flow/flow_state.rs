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
use kamu_task_system as ts;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowState {
    /// Unique flow identifier
    pub flow_id: FlowID,
    /// Flow key
    pub flow_key: FlowKey,
    /// Triggers
    pub triggers: Vec<FlowTriggerInstance>,
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
    pub finished_at: Option<DateTime<Utc>>,
}

impl FlowState {
    /// Extract primary trigger
    pub fn primary_trigger(&self) -> &FlowTriggerInstance {
        // At least 1 trigger is initially defined for sure
        self.triggers.first().unwrap()
    }

    /// Computes status
    pub fn status(&self) -> FlowStatus {
        if self.outcome.is_some() {
            FlowStatus::Finished
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
        matches!(self.status(), FlowStatus::Waiting)
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
                    flow_key,
                    trigger,
                    config_snapshot,
                }) => Ok(Self {
                    flow_id,
                    flow_key,
                    triggers: vec![trigger],
                    start_condition: None,
                    timing: FlowTimingRecords {
                        scheduled_for_activation_at: None,
                        awaiting_executor_since: None,
                        running_since: None,
                        finished_at: None,
                    },
                    task_ids: vec![],
                    config_snapshot,
                    outcome: None,
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
                    E::TriggerAdded(FlowEventTriggerAdded { ref trigger, .. }) => {
                        if s.outcome.is_some() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            let mut triggers = s.triggers;
                            triggers.push(trigger.clone());
                            Ok(FlowState { triggers, ..s })
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
                                    ..s.timing
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
                                finished_at: Some(event_time),
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
                                // TODO: support retries
                                ts::TaskOutcome::Failed(_) => Ok(FlowState {
                                    outcome: Some(FlowOutcome::Failed),
                                    timing,
                                    ..s
                                }),
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
                                    finished_at: Some(event_time),
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
