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
pub struct FlowConfiguration(
    Aggregate<FlowConfigurationState, (dyn FlowConfigurationEventStore + 'static)>,
);

impl FlowConfiguration {
    /// Creates a flow configuration
    pub fn new(
        now: DateTime<Utc>,
        flow_binding: FlowBinding,
        rule: FlowConfigurationRule,
        retry_policy: Option<RetryPolicy>,
    ) -> Self {
        Self(
            Aggregate::new(
                flow_binding.clone(),
                FlowConfigurationEventCreated {
                    event_time: now,
                    flow_binding,
                    rule,
                    retry_policy,
                },
            )
            .unwrap(),
        )
    }

    /// Modify configuration
    pub fn modify_configuration(
        &mut self,
        now: DateTime<Utc>,
        new_rule: FlowConfigurationRule,
        new_retry_policy: Option<RetryPolicy>,
    ) -> Result<(), ProjectionError<FlowConfigurationState>> {
        let event = FlowConfigurationEventModified {
            event_time: now,
            flow_binding: self.flow_binding.clone(),
            rule: new_rule,
            retry_policy: new_retry_policy,
        };
        self.apply(event)
    }

    /// Handle scope removal
    pub fn notify_scope_removed(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), ProjectionError<FlowConfigurationState>> {
        let event = FlowConfigurationEventScopeRemoved {
            event_time: now,
            flow_binding: self.flow_binding.clone(),
        };
        self.apply(event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
