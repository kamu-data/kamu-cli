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
pub struct FlowConfigurationState<TFlowKey> {
    /// Flow key
    pub flow_key: TFlowKey,
    /// Flow configuration rule
    pub rule: FlowConfigurationRule,
    /// Configuration status
    pub status: FlowConfigurationStatus,
}

impl<TFlowKey> FlowConfigurationState<TFlowKey> {
    pub fn is_active(&self) -> bool {
        self.status.is_active()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<TFlowKey: std::fmt::Debug + Clone + PartialEq + Eq + Send + Sync + 'static> Projection
    for FlowConfigurationState<TFlowKey>
{
    type Query = TFlowKey;
    type Event = FlowConfigurationEvent<TFlowKey>;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use FlowConfigurationEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::Created(FlowConfigurationEventCreated::<TFlowKey> {
                    event_time: _,
                    flow_key,
                    paused,
                    rule,
                }) => Ok(Self {
                    flow_key,
                    status: if paused {
                        FlowConfigurationStatus::PausedTemporarily
                    } else {
                        FlowConfigurationStatus::Active
                    },
                    rule,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(&s.flow_key, event.flow_key());

                match &event {
                    E::Created(_) => Err(ProjectionError::new(Some(s), event)),

                    E::Modified(FlowConfigurationEventModified::<TFlowKey> {
                        event_time: _,
                        flow_key: _,
                        paused,
                        rule,
                    }) => {
                        // Note: when deleted dataset is re-added with the same id, we have to
                        // gracefully react on this, as if it wasn't a terminal state
                        Ok(FlowConfigurationState {
                            status: if *paused {
                                FlowConfigurationStatus::PausedTemporarily
                            } else {
                                FlowConfigurationStatus::Active
                            },
                            rule: rule.clone(),
                            ..s
                        })
                    }

                    E::DatasetRemoved(_) => {
                        if s.status == FlowConfigurationStatus::StoppedPermanently {
                            Ok(s) // idempotent DELETE
                        } else {
                            Ok(FlowConfigurationState {
                                status: FlowConfigurationStatus::StoppedPermanently,
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
