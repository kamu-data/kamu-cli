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

pub type SystemFlowState = FlowState<SystemFlowStrategy>;
pub type SystemFlowEvent = FlowEvent<SystemFlowStrategy>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct SystemFlow(Aggregate<SystemFlowState, (dyn SystemFlowEventStore + 'static)>);

impl SystemFlow {
    /// Creates a system flow
    pub fn new(
        now: DateTime<Utc>,
        flow_id: SystemFlowID,
        flow_key: SystemFlowKey,
        trigger: FlowTrigger,
    ) -> Self {
        Self(
            Aggregate::new(
                flow_id,
                FlowEventInitiated::<SystemFlowStrategy> {
                    event_time: now,
                    flow_id,
                    flow_key,
                    trigger,
                },
            )
            .unwrap(),
        )
    }
}

impl Flow<SystemFlowStrategy> for SystemFlow {
    /// Returns assigned flow ID
    fn flow_id(&self) -> SystemFlowID {
        self.flow_id
    }

    /// Returns flow state reference
    fn as_state(&self) -> &SystemFlowState {
        self.0.as_state()
    }

    /// Applies an event on the flow
    fn apply_event(
        &mut self,
        event: SystemFlowEvent,
    ) -> Result<(), ProjectionError<SystemFlowState>> {
        self.0.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
