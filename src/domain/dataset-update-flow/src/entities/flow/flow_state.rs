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
pub struct FlowState<TFlowStrategy: FlowStrategy> {
    /// Unique flow identifier
    pub flow_id: TFlowStrategy::FlowID,
    /// Flow key
    pub flow_key: TFlowStrategy::FlowKey,
    /// Activating at time
    pub activate_at: Option<DateTime<Utc>>,
    /// Associated task IDs
    pub task_ids: Vec<TaskID>,
    /// Flow outcome
    pub outcome: Option<FlowOutcome>,
    /// Cancellation time
    pub cancelled_at: Option<DateTime<Utc>>,
}

impl<TFlowStrategy: FlowStrategy> FlowState<TFlowStrategy> {
    /// Checks if flow may be cancelled
    pub fn can_cancel(&self) -> bool {
        !self.outcome.is_some() && self.task_ids.is_empty() && self.cancelled_at.is_none()
    }
}

impl<TFlowStrategy: FlowStrategy + 'static> Projection for FlowState<TFlowStrategy> {
    type Query = TFlowStrategy::FlowID;
    type Event = FlowEvent<TFlowStrategy>;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use FlowEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::Initiated(FlowEventInitiated::<TFlowStrategy> {
                    event_time: _,
                    flow_id,
                    flow_key,
                    trigger: _,
                }) => Ok(Self {
                    flow_id,
                    flow_key,
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
                    E::StartConditionDefined(FlowEventStartConditionDefined::<TFlowStrategy> {
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
                    E::Queued(FlowEventQueued::<TFlowStrategy> {
                        event_time: _,
                        flow_id: _,
                        activate_at,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.is_empty() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(FlowState {
                                activate_at: Some(*activate_at),
                                ..s
                            })
                        }
                    }
                    E::TriggerAdded(FlowEventTriggerAdded::<TFlowStrategy> {
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
                    E::TaskScheduled(FlowEventTaskScheduled::<TFlowStrategy> {
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

                            Ok(FlowState { task_ids, ..s })
                        }
                    }
                    E::TaskFinished(FlowEventTaskFinished::<TFlowStrategy> {
                        event_time,
                        flow_id: _,
                        task_id,
                        task_outcome,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.contains(task_id) {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            match task_outcome {
                                TaskOutcome::Success => Ok(FlowState {
                                    outcome: Some(FlowOutcome::Success),
                                    ..s
                                }),
                                TaskOutcome::Cancelled => Ok(FlowState {
                                    outcome: Some(FlowOutcome::Cancelled),
                                    cancelled_at: Some(event_time.clone()),
                                    ..s
                                }),
                                TaskOutcome::Failed => {
                                    if s.task_ids.len() == MAX_RETRY_TASKS {
                                        Ok(FlowState {
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
                    E::Cancelled(FlowEventCancelled::<TFlowStrategy> {
                        event_time,
                        flow_id: _,
                        by_account_id: _,
                        by_account_name: _,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.is_empty() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(FlowState {
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
