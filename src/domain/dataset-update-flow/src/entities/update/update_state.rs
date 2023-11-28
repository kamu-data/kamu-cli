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

const MAX_UPDATE_TASKS: usize = 3;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateState {
    /// Unique update identifier
    pub update_id: UpdateID,
    /// Identifier of the related dataset
    pub dataset_id: DatasetID,
    /// Activating at time
    pub activate_at: Option<DateTime<Utc>>,
    /// Associated task IDs
    pub task_ids: Vec<TaskID>,
    /// Update outcome
    pub outcome: Option<UpdateOutcome>,
    /// Cancellation time
    pub cancelled_at: Option<DateTime<Utc>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Projection for UpdateState {
    type Query = UpdateID;
    type Event = UpdateEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use UpdateEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::Initiated(UpdateEventInitiated {
                    event_time: _,
                    update_id,
                    dataset_id,
                    trigger: _,
                }) => Ok(Self {
                    update_id,
                    dataset_id,
                    activate_at: None,
                    task_ids: vec![],
                    outcome: None,
                    cancelled_at: None,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(s.update_id, event.update_id());

                match &event {
                    E::Initiated(_) => Err(ProjectionError::new(Some(s), event)),
                    E::StartConditionDefined(UpdateEventStartConditionDefined {
                        event_time: _,
                        update_id: _,
                        start_condition: _,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.is_empty() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(s)
                        }
                    }
                    E::Queued(UpdateEventQueued {
                        event_time: _,
                        update_id: _,
                        activate_at,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.is_empty() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(UpdateState {
                                activate_at: Some(*activate_at),
                                ..s
                            })
                        }
                    }
                    E::TriggerAdded(UpdateEventTriggerAdded {
                        event_time: _,
                        update_id: _,
                        trigger: _,
                    }) => {
                        if s.outcome.is_some() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(s)
                        }
                    }
                    E::TaskScheduled(UpdateEventTaskScheduled {
                        event_time: _,
                        update_id: _,
                        task_id,
                    }) => {
                        if s.outcome.is_some()
                            || s.activate_at.is_none()
                            || s.task_ids.len() >= MAX_UPDATE_TASKS
                        {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            let mut task_ids = s.task_ids.clone();
                            task_ids.push(*task_id);

                            Ok(UpdateState { task_ids, ..s })
                        }
                    }
                    E::TaskFinished(UpdateEventTaskFinished {
                        event_time,
                        update_id: _,
                        task_id,
                        task_outcome,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.contains(task_id) {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            match task_outcome {
                                TaskOutcome::Success => Ok(UpdateState {
                                    outcome: Some(UpdateOutcome::Success),
                                    ..s
                                }),
                                TaskOutcome::Cancelled => Ok(UpdateState {
                                    outcome: Some(UpdateOutcome::Cancelled),
                                    cancelled_at: Some(event_time.clone()),
                                    ..s
                                }),
                                TaskOutcome::Failed => {
                                    if s.task_ids.len() == MAX_UPDATE_TASKS {
                                        Ok(UpdateState {
                                            outcome: Some(UpdateOutcome::Failed),
                                            ..s
                                        })
                                    } else {
                                        Ok(s)
                                    }
                                }
                            }
                        }
                    }
                    E::Cancelled(UpdateEventCancelled {
                        event_time,
                        update_id: _,
                        by_account_id: _,
                        by_account_name: _,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.is_empty() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(UpdateState {
                                outcome: Some(UpdateOutcome::Cancelled),
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
