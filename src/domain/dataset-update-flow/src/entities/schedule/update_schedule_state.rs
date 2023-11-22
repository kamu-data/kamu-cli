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

/// Represents the state of the task at specific point in time (projection)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateScheduleState {
    /// Identifier of the related dataset
    pub dataset_id: DatasetID,
    /// Update schedule
    pub schedule: Schedule,
    /// Pause indication
    pub paused: bool,
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
                    schedule,
                }) => Ok(Self {
                    dataset_id,
                    schedule,
                    paused: false,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(&s.dataset_id, event.dataset_id());

                match event {
                    E::Created(_) => Err(ProjectionError::new(Some(s), event)),
                    E::Modified(UpdateScheduleEventModified {
                        event_time: _,
                        dataset_id: _,
                        paused,
                        schedule,
                    }) => Ok(UpdateScheduleState {
                        schedule,
                        paused,
                        ..s
                    }),
                }
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
