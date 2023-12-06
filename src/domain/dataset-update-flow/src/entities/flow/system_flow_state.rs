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

const MAX_RETRY_TASKS: usize = 3;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemFlowState {
    /// Unique flow identifier
    pub flow_id: SystemFlowID,
    /// Flow type
    pub flow_type: SystemFlowType,
    /// Activating at time
    pub activate_at: Option<DateTime<Utc>>,
    /// Associated task IDs
    pub task_ids: Vec<TaskID>,
    /// Flow outcome
    pub outcome: Option<FlowOutcome>,
    /// Cancellation time
    pub cancelled_at: Option<DateTime<Utc>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Projection for SystemFlowState {
    type Query = SystemFlowID;
    type Event = SystemFlowEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use SystemFlowEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::Initiated(SystemFlowEventInitiated {
                    event_time: _,
                    flow_id,
                    flow_type,
                    trigger: _,
                }) => Ok(Self {
                    flow_id,
                    flow_type,
                    activate_at: None,
                    task_ids: vec![],
                    outcome: None,
                    cancelled_at: None,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(s.flow_id, event.flow_id());

                match &event {
                    E::Initiated(_) => Err(ProjectionError::new(Some(s), event)),
                    E::StartConditionDefined(SystemFlowEventStartConditionDefined {
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
                    E::Queued(SystemFlowEventQueued {
                        event_time: _,
                        flow_id: _,
                        activate_at,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.is_empty() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(SystemFlowState {
                                activate_at: Some(*activate_at),
                                ..s
                            })
                        }
                    }
                    E::TriggerAdded(SystemFlowEventTriggerAdded {
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
                    E::TaskScheduled(SystemFlowEventTaskScheduled {
                        event_time: _,
                        flow_id: _,
                        task_id,
                    }) => {
                        if s.outcome.is_some()
                            || s.activate_at.is_none()
                            || s.task_ids.len() >= MAX_RETRY_TASKS
                        {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            let mut task_ids = s.task_ids.clone();
                            task_ids.push(*task_id);

                            Ok(SystemFlowState { task_ids, ..s })
                        }
                    }
                    E::TaskFinished(SystemFlowEventTaskFinished {
                        event_time,
                        flow_id: _,
                        task_id,
                        task_outcome,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.contains(task_id) {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            match task_outcome {
                                TaskOutcome::Success => Ok(SystemFlowState {
                                    outcome: Some(FlowOutcome::Success),
                                    ..s
                                }),
                                TaskOutcome::Cancelled => Ok(SystemFlowState {
                                    outcome: Some(FlowOutcome::Cancelled),
                                    cancelled_at: Some(event_time.clone()),
                                    ..s
                                }),
                                TaskOutcome::Failed => {
                                    if s.task_ids.len() == MAX_RETRY_TASKS {
                                        Ok(SystemFlowState {
                                            outcome: Some(FlowOutcome::Failed),
                                            ..s
                                        })
                                    } else {
                                        Ok(s)
                                    }
                                }
                            }
                        }
                    }
                    E::Cancelled(SystemFlowEventCancelled {
                        event_time,
                        flow_id: _,
                        by_account_id: _,
                        by_account_name: _,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.is_empty() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(SystemFlowState {
                                outcome: Some(FlowOutcome::Cancelled),
                                cancelled_at: Some(event_time.clone()),
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
