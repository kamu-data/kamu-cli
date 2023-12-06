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
use opendatafabric::DatasetID;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

const MAX_RETRY_TASKS: usize = 3;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFlowState {
    /// Unique flow identifier
    pub flow_id: DatasetFlowID,
    /// Identifier of the related dataset
    pub dataset_id: DatasetID,
    /// Flow type
    pub flow_type: DatasetFlowType,
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

impl Projection for DatasetFlowState {
    type Query = DatasetFlowID;
    type Event = DatasetFlowEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use DatasetFlowEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::Initiated(DatasetFlowEventInitiated {
                    event_time: _,
                    flow_id,
                    dataset_id,
                    flow_type,
                    trigger: _,
                }) => Ok(Self {
                    flow_id,
                    dataset_id,
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
                    E::StartConditionDefined(DatasetFlowEventStartConditionDefined {
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
                    E::Queued(DatasetFlowEventQueued {
                        event_time: _,
                        flow_id: _,
                        activate_at,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.is_empty() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(DatasetFlowState {
                                activate_at: Some(*activate_at),
                                ..s
                            })
                        }
                    }
                    E::TriggerAdded(DatasetFlowEventTriggerAdded {
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
                    E::TaskScheduled(DatasetFlowEventTaskScheduled {
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

                            Ok(DatasetFlowState { task_ids, ..s })
                        }
                    }
                    E::TaskFinished(DatasetFlowEventTaskFinished {
                        event_time,
                        flow_id: _,
                        task_id,
                        task_outcome,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.contains(task_id) {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            match task_outcome {
                                TaskOutcome::Success => Ok(DatasetFlowState {
                                    outcome: Some(FlowOutcome::Success),
                                    ..s
                                }),
                                TaskOutcome::Cancelled => Ok(DatasetFlowState {
                                    outcome: Some(FlowOutcome::Cancelled),
                                    cancelled_at: Some(event_time.clone()),
                                    ..s
                                }),
                                TaskOutcome::Failed => {
                                    if s.task_ids.len() == MAX_RETRY_TASKS {
                                        Ok(DatasetFlowState {
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
                    E::Cancelled(DatasetFlowEventCancelled {
                        event_time,
                        flow_id: _,
                        by_account_id: _,
                        by_account_name: _,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.is_empty() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(DatasetFlowState {
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
