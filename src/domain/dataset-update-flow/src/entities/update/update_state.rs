// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::*;
use kamu_task_system::TaskID;
use opendatafabric::DatasetID;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

const MAX_UPDATE_TASKS: usize = 3;

/////////////////////////////////////////////////////////////////////////////////////////

/// Represents the state of the task at specific point in time (projection)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateState {
    /// Unique update identifier
    pub update_id: UpdateID,
    /// Identifier of the related dataset
    pub dataset_id: DatasetID,
    /// Queued for time
    pub queued_for: Option<chrono::DateTime<chrono::Utc>>,
    /// Associated task IDs
    pub task_ids: Vec<TaskID>,
    /// Update outcome
    pub outcome: Option<UpdateOutcome>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Projection for UpdateState {
    type Query = DatasetID;
    type Event = UpdateEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use UpdateEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::Initiated(UpdateInitiated {
                    event_time: _,
                    update_id,
                    dataset_id,
                    trigger: _,
                }) => Ok(Self {
                    update_id,
                    dataset_id,
                    queued_for: None,
                    task_ids: vec![],
                    outcome: None,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(s.update_id, event.update_id());

                match &event {
                    E::Initiated(_) => Err(ProjectionError::new(Some(s), event)),
                    E::Postponed(UpdatePostponed {
                        event_time: _,
                        update_id: _,
                        delay_reason: _,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.is_empty() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(s)
                        }
                    }
                    E::Queued(UpdateQueued {
                        event_time: _,
                        update_id: _,
                        queued_for,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.is_empty() {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(UpdateState {
                                queued_for: Some(*queued_for),
                                ..s
                            })
                        }
                    }
                    E::SecondaryTrigger(UpdateSecondaryTrigger {
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
                    E::TaskScheduled(UpdateTaskScheduled {
                        event_time: _,
                        update_id: _,
                        task_id,
                    }) => {
                        if s.outcome.is_some()
                            || s.queued_for.is_none()
                            || s.task_ids.len() >= MAX_UPDATE_TASKS
                        {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            let mut task_ids = s.task_ids.clone();
                            task_ids.push(*task_id);

                            Ok(UpdateState { task_ids, ..s })
                        }
                    }
                    E::TaskSucceeded(UpdateTaskSucceeded {
                        event_time: _,
                        update_id: _,
                        task_id,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.contains(task_id) {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(UpdateState {
                                outcome: Some(UpdateOutcome::Success),
                                ..s
                            })
                        }
                    }
                    E::TaskFailed(UpdateTaskFailed {
                        event_time: _,
                        update_id: _,
                        task_id,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.contains(task_id) {
                            Err(ProjectionError::new(Some(s), event))
                        } else if s.task_ids.len() == MAX_UPDATE_TASKS {
                            Ok(UpdateState {
                                outcome: Some(UpdateOutcome::Failed),
                                ..s
                            })
                        } else {
                            Ok(s)
                        }
                    }
                    E::TaskCancelled(UpdateTaskCancelled {
                        event_time: _,
                        update_id: _,
                        task_id,
                    }) => {
                        if s.outcome.is_some() || !s.task_ids.contains(task_id) {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(UpdateState {
                                outcome: Some(UpdateOutcome::Cancelled),
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
