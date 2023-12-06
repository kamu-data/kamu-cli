// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::*;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemFlowConfigurationState {
    /// Flow type
    pub flow_type: SystemFlowType,
    /// Flow schedule
    pub schedule: Schedule,
    /// Configuration status
    pub status: FlowConfigurationStatus,
}

impl SystemFlowConfigurationState {
    pub fn is_active(&self) -> bool {
        self.status.is_active()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Projection for SystemFlowConfigurationState {
    type Query = SystemFlowType;
    type Event = SystemFlowConfigurationEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use SystemFlowConfigurationEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::Created(SystemFlowConfigurationEventCreated {
                    event_time: _,
                    flow_type,
                    paused,
                    schedule,
                }) => Ok(Self {
                    flow_type,
                    status: if paused {
                        FlowConfigurationStatus::PausedTemporarily
                    } else {
                        FlowConfigurationStatus::Active
                    },
                    schedule,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(s.flow_type, event.flow_type());

                match &event {
                    E::Created(_) => Err(ProjectionError::new(Some(s), event)),

                    E::Modified(SystemFlowConfigurationEventModified {
                        event_time: _,
                        flow_type: _,
                        paused,
                        schedule,
                    }) => Ok(SystemFlowConfigurationState {
                        status: if *paused {
                            FlowConfigurationStatus::PausedTemporarily
                        } else {
                            FlowConfigurationStatus::Active
                        },
                        schedule: schedule.clone(),
                        ..s
                    }),
                }
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
