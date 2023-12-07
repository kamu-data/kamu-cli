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

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub type SystemFlowConfigurationState = FlowConfigurationState<SystemFlowKey>;
pub type SystemFlowConfigurationEvent = FlowConfigurationEvent<SystemFlowKey>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct SystemFlowConfiguration(
    Aggregate<SystemFlowConfigurationState, (dyn SystemFlowConfigurationEventStore + 'static)>,
);

impl SystemFlowConfiguration {
    /// Creates a system flow configuration
    pub fn new(
        now: DateTime<Utc>,
        flow_key: SystemFlowKey,
        paused: bool,
        rule: FlowConfigurationRule,
    ) -> Self {
        Self(
            Aggregate::new(
                flow_key.clone(),
                FlowConfigurationEventCreated::<SystemFlowKey> {
                    event_time: now,
                    flow_key,
                    paused,
                    rule,
                },
            )
            .unwrap(),
        )
    }
}

impl FlowConfiguration<SystemFlowKey> for SystemFlowConfiguration {
    /// Returns assigned flow key
    fn flow_key(&self) -> &SystemFlowKey {
        &self.flow_key
    }

    /// Applies an event on the flow configuration
    fn apply_event(
        &mut self,
        event: FlowConfigurationEvent<SystemFlowKey>,
    ) -> Result<(), ProjectionError<SystemFlowConfigurationState>> {
        self.0.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
