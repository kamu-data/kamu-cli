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
pub struct FlowConfiguration(
    Aggregate<FlowConfigurationState, (dyn FlowConfigurationEventStore + 'static)>,
);

impl FlowConfiguration {
    /// Creates a flow configuration
    pub fn new(
        now: DateTime<Utc>,
        flow_key: FlowKey,
        paused: bool,
        rule: FlowConfigurationRule,
    ) -> Self {
        Self(
            Aggregate::new(
                flow_key.clone(),
                FlowConfigurationEventCreated {
                    event_time: now,
                    flow_key,
                    paused,
                    rule,
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
        new_rule: FlowConfigurationRule,
    ) -> Result<(), ProjectionError<FlowConfigurationState>> {
        let event = FlowConfigurationEventModified {
            event_time: now,
            flow_key: self.flow_key.clone(),
            paused,
            rule: new_rule,
        };
        self.apply(event)
    }

    /// Pause configuration
    pub fn pause(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), ProjectionError<FlowConfigurationState>> {
        if self.is_active() {
            let event = FlowConfigurationEventModified {
                event_time: now,
                flow_key: self.flow_key.clone(),
                paused: true,
                rule: self.rule.clone(),
            };
            self.apply(event)
        } else {
            Ok(())
        }
    }

    /// Resume configuration
    pub fn resume(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), ProjectionError<FlowConfigurationState>> {
        if self.is_active() {
            Ok(())
        } else {
            let event = FlowConfigurationEventModified {
                event_time: now,
                flow_key: self.flow_key.clone(),
                paused: false,
                rule: self.rule.clone(),
            };
            self.apply(event)
        }
    }

    /// Handle dataset removal
    pub fn notify_dataset_removed(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), ProjectionError<FlowConfigurationState>> {
        let event = FlowConfigurationEventDatasetRemoved {
            event_time: now,
            flow_key: self.flow_key.clone(),
        };
        self.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
