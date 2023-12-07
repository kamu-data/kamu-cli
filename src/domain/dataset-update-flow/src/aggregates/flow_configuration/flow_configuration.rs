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

pub trait FlowConfiguration<
    TFlowKey: std::fmt::Debug + Clone + Eq + PartialEq + Send + Sync + 'static,
>
{
    /// Returns assigned flow key
    fn flow_key(&self) -> &TFlowKey;

    /// Applies an event on the flow configuration
    fn apply_event(
        &mut self,
        event: <FlowConfigurationState<TFlowKey> as event_sourcing::Projection>::Event,
    ) -> Result<(), ProjectionError<FlowConfigurationState<TFlowKey>>>;

    /// Modify configuration
    fn modify_configuration(
        &mut self,
        now: DateTime<Utc>,
        paused: bool,
        new_rule: FlowConfigurationRule,
    ) -> Result<(), ProjectionError<FlowConfigurationState<TFlowKey>>> {
        let event = FlowConfigurationEventModified::<TFlowKey> {
            event_time: now,
            flow_key: self.flow_key().clone(),
            paused,
            rule: new_rule,
        };
        self.apply_event(event.into())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
