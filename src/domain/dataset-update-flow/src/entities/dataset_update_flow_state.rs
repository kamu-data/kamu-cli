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
pub struct DatasetUpdateFlowState {
    /// Identifier of the related dataset
    pub dataset_id: DatasetID,
    /// Update schedule
    pub schedule: UpdateSchedule,
    /// Pause indication
    pub paused: bool,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Projection for DatasetUpdateFlowState {
    type Query = DatasetID;
    type Event = DatasetUpdateFlowEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use DatasetUpdateFlowEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::FlowCreated(DatasetUpdateFlowCreated {
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
                    E::FlowCreated(_) => Err(ProjectionError::new(Some(s), event)),
                    E::FlowPaused(_) => {
                        if s.paused {
                            Err(ProjectionError::new(Some(s), event))
                        } else {
                            Ok(DatasetUpdateFlowState { paused: true, ..s })
                        }
                    }
                    E::FlowResumed(_) => {
                        if s.paused {
                            Ok(DatasetUpdateFlowState { paused: false, ..s })
                        } else {
                            Err(ProjectionError::new(Some(s), event))
                        }
                    }
                    E::ScheduleModified(DatasetUpdateFlowScheduleModified {
                        event_time: _,
                        dataset_id: _,
                        new_schedule,
                    }) => Ok(DatasetUpdateFlowState {
                        schedule: new_schedule,
                        ..s
                    }),
                }
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
