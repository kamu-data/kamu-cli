// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{Projection, ProjectionError, ProjectionEvent};
use serde::{Deserialize, Serialize};

use super::{FlowTriggerEvent, FlowTriggerRule, FlowTriggerStatus};
use crate::{BatchingRule, FlowKey, FlowTriggerEventCreated, FlowTriggerEventModified, Schedule};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowTriggerState {
    /// Flow key
    pub flow_key: FlowKey,
    /// Trigger rule
    pub rule: FlowTriggerRule,
    /// Trigger status
    pub status: FlowTriggerStatus,
}

impl FlowTriggerState {
    pub fn is_active(&self) -> bool {
        self.status.is_active()
    }

    pub fn try_get_schedule_rule(self) -> Option<Schedule> {
        match self.rule {
            FlowTriggerRule::Schedule(schedule) => Some(schedule),
            FlowTriggerRule::Batching(_) => None,
        }
    }

    pub fn try_get_batching_rule(self) -> Option<BatchingRule> {
        match self.rule {
            FlowTriggerRule::Batching(batching) => Some(batching),
            FlowTriggerRule::Schedule(_) => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Projection for FlowTriggerState {
    type Query = FlowKey;
    type Event = FlowTriggerEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use FlowTriggerEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::Created(FlowTriggerEventCreated {
                    flow_key,
                    paused,
                    rule,
                    ..
                }) => Ok(Self {
                    flow_key,
                    status: if paused {
                        FlowTriggerStatus::PausedTemporarily
                    } else {
                        FlowTriggerStatus::Active
                    },
                    rule,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(&s.flow_key, event.flow_key());

                match &event {
                    E::Created(_) => Err(ProjectionError::new(Some(s), event)),

                    E::Modified(FlowTriggerEventModified { paused, rule, .. }) => {
                        // Note: when deleted dataset is re-added with the same id, we have to
                        // gracefully react on this, as if it wasn't a terminal state
                        Ok(FlowTriggerState {
                            status: if *paused {
                                FlowTriggerStatus::PausedTemporarily
                            } else {
                                FlowTriggerStatus::Active
                            },
                            rule: rule.clone(),
                            ..s
                        })
                    }

                    E::DatasetRemoved(_) => {
                        if let FlowKey::Dataset(_) = &s.flow_key {
                            if s.status == FlowTriggerStatus::StoppedPermanently {
                                Ok(s) // idempotent DELETE
                            } else {
                                Ok(FlowTriggerState {
                                    status: FlowTriggerStatus::StoppedPermanently,
                                    ..s
                                })
                            }
                        } else {
                            Err(ProjectionError::new(Some(s), event))
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ProjectionEvent<FlowKey> for FlowTriggerEvent {
    fn matches_query(&self, query: &FlowKey) -> bool {
        self.flow_key() == query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
