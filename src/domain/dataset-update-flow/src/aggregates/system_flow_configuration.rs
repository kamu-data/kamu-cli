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
                flow_type,
                SystemFlowConfigurationEventCreated {
                    event_time: now,
                    flow_type,
                    paused,
                    schedule,
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
        let event = SystemFlowConfigurationEventModified {
            event_time: now,
            flow_type: self.flow_type,
            paused,
            schedule: new_schedule,
        };
        self.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
