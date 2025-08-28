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
    pub fn new(
        now: DateTime<Utc>,
        flow_binding: FlowBinding,
        rule: FlowTriggerRule,
        stop_policy: FlowTriggerStopPolicy,
    ) -> Self {
        Self(
            Aggregate::new(
                flow_binding.clone(),
                FlowTriggerEventCreated {
                    event_time: now,
                    flow_binding,
                    paused: false,
                    rule,
                    stop_policy,
                },
            )
            .unwrap(),
        )
    }

    /// Modify trigger rule
    pub fn modify_rule(
        &mut self,
        now: DateTime<Utc>,
        new_rule: FlowTriggerRule,
        stop_policy: FlowTriggerStopPolicy,
    ) -> Result<(), ProjectionError<FlowTriggerState>> {
        // Determine which value parts got modified
        let pause_affected = !self.is_active();
        let rule_affected = self.rule != new_rule;
        let stop_policy_affected = self.stop_policy != stop_policy;

        // Ignore if nothing changed
        if !pause_affected && !rule_affected && !stop_policy_affected {
            return Ok(());
        }

        let event = FlowTriggerEventModified {
            event_time: now,
            flow_binding: self.flow_binding.clone(),
            paused: false,
            rule: new_rule,
            stop_policy,
        };
        self.apply(event)
    }

    /// Pause trigger (user initiative)
    pub fn pause(&mut self, now: DateTime<Utc>) -> Result<(), ProjectionError<FlowTriggerState>> {
        if self.is_active() {
            let event = FlowTriggerEventModified {
                event_time: now,
                flow_binding: self.flow_binding.clone(),
                paused: true,
                rule: self.rule.clone(),
                stop_policy: self.stop_policy,
            };
            self.apply(event)
        } else {
            Ok(())
        }
    }

    /// Stop trigger (system initiative)
    pub fn stop(&mut self, now: DateTime<Utc>) -> Result<(), ProjectionError<FlowTriggerState>> {
        if self.is_active() {
            let event = FlowTriggerEventAutoStopped {
                event_time: now,
                flow_binding: self.flow_binding.clone(),
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
                flow_binding: self.flow_binding.clone(),
                paused: false,
                rule: self.rule.clone(),
                stop_policy: self.stop_policy,
            };
            self.apply(event)
        }
    }

    /// Handle scope removal
    pub fn notify_scope_removed(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), ProjectionError<FlowTriggerState>> {
        let event = FlowTriggerEventScopeRemoved {
            event_time: now,
            flow_binding: self.flow_binding.clone(),
        };
        self.apply(event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
