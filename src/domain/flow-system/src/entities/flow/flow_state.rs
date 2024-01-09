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
use kamu_task_system::{TaskID, TaskOutcome};

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowState {
    /// Unique flow identifier
    pub flow_id: FlowID,
    /// Flow key
    pub flow_key: FlowKey,
    /// Primary flow trigger
    pub primary_trigger: FlowTrigger,
    /// Timing records
    pub timing: FlowTimingRecords,
    /// Associated task IDs
    pub task_ids: Vec<TaskID>,
    /// Flow outcome
    pub outcome: Option<FlowOutcome>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FlowTimingRecords {
    /// Activating at time
    pub activate_at: Option<DateTime<Utc>>,
    /// Started running at time
    pub running_since: Option<DateTime<Utc>>,
    /// Finish time (success or cancel/abort)
    pub finished_at: Option<DateTime<Utc>>,
}

impl FlowState {
    /// Computes status
    pub fn status(&self) -> FlowStatus {
        if self.outcome.is_some() {
            FlowStatus::Finished
        } else if self.timing.running_since.is_some() {
            FlowStatus::Running
        } else if !self.task_ids.is_empty() {
            FlowStatus::Scheduled
        } else if self.timing.activate_at.is_some() {
            FlowStatus::Queued
        } else {
            FlowStatus::Waiting
        }
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
                }) => Ok(Self {
                    flow_id,
                    flow_key,
                    primary_trigger: trigger,
                    timing: FlowTimingRecords {
                        activate_at: None,
                        running_since: None,
                        finished_at: None,
                    },
                    task_ids: vec![],
                    outcome: None,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(s.flow_id, event.flow_id());

                match &event {
                    E::Initiated(_) => Err(ProjectionError::new(Some(s), event)),
                    E::StartConditionDefined(FlowEventStartConditionDefined {
                        event_time: _,
                        flow_id: _,
                        start_condition: _,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.is_empty() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(s)
                        }
                    }
                    E::Queued(FlowEventQueued {
                        event_time: _,
                        flow_id: _,
                        activate_at,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.is_empty() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(FlowState {
                                timing: FlowTimingRecords {
                                    activate_at: Some(*activate_at),
                                    ..s.timing
                                },
                                ..s
                            })
                        }
                    }
                    E::TriggerAdded(FlowEventTriggerAdded {
                        event_time: _,
                        flow_id: _,
                        trigger: _,
                    }) => {
                        if s.outcome.is_some() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(s)
                        }
                    }
                    E::TaskScheduled(FlowEventTaskScheduled {
                        event_time: _,
                        flow_id: _,
                        task_id,
                    }) => {
                        if s.outcome.is_some() || s.timing.activate_at.is_none() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            let mut task_ids = s.task_ids.clone();
                            task_ids.push(*task_id);

                            Ok(FlowState { task_ids, ..s })
                        }
                    }
                    E::TaskRunning(FlowEventTaskRunning {
                        event_time,
                        flow_id: _,
                        task_id,
                    }) => {
                        if s.outcome.is_some()
                            || s.timing.activate_at.is_none()
                            || !s.task_ids.contains(task_id)
                        {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(FlowState {
                                timing: FlowTimingRecords {
                                    running_since: Some(*event_time),
                                    ..s.timing
                                },
                                ..s
                            })
                        }
                    }
                    E::TaskFinished(FlowEventTaskFinished {
                        event_time,
                        flow_id: _,
                        task_id,
                        task_outcome,
                    }) => {
                        if !s.task_ids.contains(task_id) || s.timing.running_since.is_none() {
                            Err(ProjectionError::new(Some(s), event))
                        } else if s.outcome.is_some() {
                            // Ignore for idempotence motivation
                            Ok(s)
                        } else {
                            match task_outcome {
                                TaskOutcome::Success => Ok(FlowState {
                                    outcome: Some(FlowOutcome::Success),
                                    timing: FlowTimingRecords {
                                        finished_at: Some(*event_time),
                                        ..s.timing
                                    },
                                    ..s
                                }),
                                TaskOutcome::Cancelled => Ok(FlowState {
                                    outcome: Some(FlowOutcome::Cancelled),
                                    timing: FlowTimingRecords {
                                        finished_at: Some(*event_time),
                                        ..s.timing
                                    },
                                    ..s
                                }),
                                // TODO: support retries
                                TaskOutcome::Failed => Ok(FlowState {
                                    outcome: Some(FlowOutcome::Failed),
                                    ..s
                                }),
                            }
                        }
                    }
                    E::Aborted(FlowEventAborted {
                        event_time,
                        flow_id: _,
                    }) => {
                        if let Some(outcome) = s.outcome {
                            if outcome == FlowOutcome::Success {
                                Err(ProjectionError::new(Some(s), event))
                            } else {
                                // Ignore for idempotence reasons
                                Ok(s)
                            }
                        } else {
                            Ok(FlowState {
                                outcome: Some(FlowOutcome::Aborted),
                                timing: FlowTimingRecords {
                                    finished_at: Some(*event_time),
                                    ..s.timing
                                },
                                ..s
                            })
                        }
                    }
                }
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl ProjectionEvent<FlowID> for FlowEvent {
    fn matches_query(&self, query: &FlowID) -> bool {
        self.flow_id() == *query
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
