// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::*;
use opendatafabric::DatasetID;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateScheduleState {
    /// Identifier of the related dataset
    pub dataset_id: DatasetID,
    /// Update schedule
    pub schedule: Schedule,
    /// Schedule status
    pub status: UpdateScheduleStatus,
}

impl UpdateScheduleState {
    pub fn is_active(&self) -> bool {
        match self.status {
            UpdateScheduleStatus::Active => true,
            UpdateScheduleStatus::PausedTemporarily => false,
            UpdateScheduleStatus::StoppedPermanently => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateScheduleStatus {
    Active,
    PausedTemporarily,
    StoppedPermanently,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Projection for UpdateScheduleState {
    type Query = DatasetID;
    type Event = UpdateScheduleEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use UpdateScheduleEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::Created(UpdateScheduleEventCreated {
                    event_time: _,
                    dataset_id,
                    paused,
                    schedule,
                }) => Ok(Self {
                    dataset_id,
                    status: if paused {
                        UpdateScheduleStatus::PausedTemporarily
                    } else {
                        UpdateScheduleStatus::Active
                    },
                    schedule,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(&s.dataset_id, event.dataset_id());

                match &event {
                    E::Created(_) => Err(ProjectionError::new(Some(s), event)),

                    E::Modified(UpdateScheduleEventModified {
                        event_time: _,
                        dataset_id: _,
                        paused,
                        schedule,
                    }) => {
                        // Note: when deleted dataset is re-added with the same id, we have to
                        // gracefully react on this, as if it wasn't a terminal state
                        Ok(UpdateScheduleState {
                            status: if *paused {
                                UpdateScheduleStatus::PausedTemporarily
                            } else {
                                UpdateScheduleStatus::Active
                            },
                            schedule: schedule.clone(),
                            ..s
                        })
                    }

                    E::DatasetRemoved(UpdateScheduleEventDatasetRemoved {
                        event_time: _,
                        dataset_id: _,
                    }) => {
                        if s.status == UpdateScheduleStatus::StoppedPermanently {
                            Ok(s) // idempotent DELETE
                        } else {
                            Ok(UpdateScheduleState {
                                status: UpdateScheduleStatus::StoppedPermanently,
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
