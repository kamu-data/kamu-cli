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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct FlowTrigger(Aggregate<FlowTriggerState, (dyn FlowTriggerEventStore + 'static)>);

impl FlowTrigger {
    /// Creates a flow trigger rule
    pub fn new(now: DateTime<Utc>, flow_key: FlowKey, paused: bool, rule: FlowTriggerRule) -> Self {
        Self(
            Aggregate::new(
                flow_key.clone(),
                FlowTriggerEventCreated {
                    event_time: now,
                    flow_key,
                    paused,
                    rule,
                },
            )
            .unwrap(),
        )
    }

    /// Modify trigger rule
    pub fn modify_rule(
        &mut self,
        now: DateTime<Utc>,
        paused: bool,
        new_rule: FlowTriggerRule,
    ) -> Result<(), ProjectionError<FlowTriggerState>> {
        let event = FlowTriggerEventModified {
            event_time: now,
            flow_key: self.flow_key.clone(),
            paused,
            rule: new_rule,
        };
        self.apply(event)
    }

    /// Pause trigger
    pub fn pause(&mut self, now: DateTime<Utc>) -> Result<(), ProjectionError<FlowTriggerState>> {
        if self.is_active() {
            let event = FlowTriggerEventModified {
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

    /// Resume trigger
    pub fn resume(&mut self, now: DateTime<Utc>) -> Result<(), ProjectionError<FlowTriggerState>> {
        if self.is_active() {
            Ok(())
        } else {
            let event = FlowTriggerEventModified {
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
    ) -> Result<(), ProjectionError<FlowTriggerState>> {
        let event = FlowTriggerEventDatasetRemoved {
            event_time: now,
            flow_key: self.flow_key.clone(),
        };
        self.apply(event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
