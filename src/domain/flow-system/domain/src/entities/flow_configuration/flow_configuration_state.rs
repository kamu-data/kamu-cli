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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowConfigurationState {
    /// Flow binding
    pub flow_binding: FlowBinding,
    /// Flow configuration rule
    pub rule: FlowConfigurationRule,
    /// Retry policy
    pub retry_policy: Option<RetryPolicy>,
    /// Configuration status
    pub status: FlowConfigurationStatus,
}

impl FlowConfigurationState {
    pub fn is_active(&self) -> bool {
        self.status.is_active()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Projection for FlowConfigurationState {
    type Query = FlowBinding;
    type Event = FlowConfigurationEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use FlowConfigurationEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::Created(FlowConfigurationEventCreated {
                    flow_binding,
                    rule,
                    retry_policy,
                    ..
                }) => Ok(Self {
                    flow_binding,
                    rule,
                    status: FlowConfigurationStatus::Active,
                    retry_policy,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(&s.flow_binding, event.flow_binding());

                match &event {
                    E::Created(_) => Err(ProjectionError::new(Some(s), event)),

                    E::Modified(FlowConfigurationEventModified {
                        rule, retry_policy, ..
                    }) => {
                        // Note: when deleted dataset is re-added with the same id, we have to
                        // gracefully react on this, as if it wasn't a terminal state
                        Ok(FlowConfigurationState {
                            rule: rule.clone(),
                            retry_policy: *retry_policy,
                            ..s
                        })
                    }

                    E::DatasetRemoved(_) => {
                        if s.status == FlowConfigurationStatus::Deleted {
                            Ok(s) // idempotent DELETE
                        } else {
                            Ok(FlowConfigurationState {
                                status: FlowConfigurationStatus::Deleted,
                                ..s
                            })
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ProjectionEvent<FlowBinding> for FlowConfigurationEvent {
    fn matches_query(&self, query: &FlowBinding) -> bool {
        self.flow_binding() == query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
