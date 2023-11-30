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
pub struct DatasetFlowConfigurationState {
    /// Identifier of the related dataset
    pub dataset_id: DatasetID,
    /// Flow type
    pub flow_type: DatasetFlowType,
    /// Flow configuration rule
    pub rule: DatasetFlowConfigurationRule,
    /// Configuration status
    pub status: UpdateConfigurationStatus,
}

impl DatasetFlowConfigurationState {
    pub fn is_active(&self) -> bool {
        match self.status {
            UpdateConfigurationStatus::Active => true,
            UpdateConfigurationStatus::PausedTemporarily => false,
            UpdateConfigurationStatus::StoppedPermanently => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateConfigurationStatus {
    Active,
    PausedTemporarily,
    StoppedPermanently,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetFlowConfigurationRule {
    Schedule(Schedule),
    StartCondition(StartCondition),
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Projection for DatasetFlowConfigurationState {
    type Query = (DatasetID, DatasetFlowType);
    type Event = DatasetFlowConfigurationEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use DatasetFlowConfigurationEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::Created(DatasetFlowConfigurationEventCreated {
                    event_time: _,
                    dataset_id,
                    flow_type,
                    paused,
                    rule,
                }) => Ok(Self {
                    dataset_id,
                    flow_type,
                    status: if paused {
                        UpdateConfigurationStatus::PausedTemporarily
                    } else {
                        UpdateConfigurationStatus::Active
                    },
                    rule,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(&s.dataset_id, event.dataset_id());
                assert_eq!(s.flow_type, event.flow_type());

                match &event {
                    E::Created(_) => Err(ProjectionError::new(Some(s), event)),

                    E::Modified(DatasetFlowConfigurationEventModified {
                        event_time: _,
                        dataset_id: _,
                        flow_type: _,
                        paused,
                        rule,
                    }) => {
                        // Note: when deleted dataset is re-added with the same id, we have to
                        // gracefully react on this, as if it wasn't a terminal state
                        Ok(DatasetFlowConfigurationState {
                            status: if *paused {
                                UpdateConfigurationStatus::PausedTemporarily
                            } else {
                                UpdateConfigurationStatus::Active
                            },
                            rule: rule.clone(),
                            ..s
                        })
                    }

                    E::DatasetRemoved(_) => {
                        if s.status == UpdateConfigurationStatus::StoppedPermanently {
                            Ok(s) // idempotent DELETE
                        } else {
                            Ok(DatasetFlowConfigurationState {
                                status: UpdateConfigurationStatus::StoppedPermanently,
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
