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
        flow_type: SystemFlowType,
        paused: bool,
        schedule: Schedule,
    ) -> Self {
        Self(
            Aggregate::new(
                SystemFlowKey::new(flow_type),
                FlowConfigurationEventCreated::<SystemFlowKey> {
                    event_time: now,
                    flow_key: SystemFlowKey::new(flow_type),
                    paused,
                    rule: FlowConfigurationRule::Schedule(schedule),
                },
            )
            .unwrap(),
        )
    }

    /// Modify configuration
    pub fn modify_configuration(
        &mut self,
        now: DateTime<Utc>,
        paused: bool,
        new_schedule: Schedule,
    ) -> Result<(), ProjectionError<SystemFlowConfigurationState>> {
        let event = FlowConfigurationEventModified::<SystemFlowKey> {
            event_time: now,
            flow_key: self.flow_key.clone(),
            paused,
            rule: FlowConfigurationRule::Schedule(new_schedule),
        };
        self.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
