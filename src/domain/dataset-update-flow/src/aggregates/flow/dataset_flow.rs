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

pub type DatasetFlowState = FlowState<DatasetFlowStrategy>;
pub type DatasetFlowEvent = FlowEvent<DatasetFlowStrategy>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct DatasetFlow(Aggregate<DatasetFlowState, (dyn DatasetFlowEventStore + 'static)>);

impl DatasetFlow {
    /// Creates a dataset flow
    pub fn new(
        now: DateTime<Utc>,
        flow_id: DatasetFlowID,
        flow_key: DatasetFlowKey,
        trigger: FlowTrigger,
    ) -> Self {
        Self(
            Aggregate::new(
                flow_id,
                FlowEventInitiated::<DatasetFlowStrategy> {
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

impl Flow<DatasetFlowStrategy> for DatasetFlow {
    /// Returns assigned flow ID
    fn flow_id(&self) -> DatasetFlowID {
        self.flow_id
    }

    /// Returns flow state reference
    fn as_state(&self) -> &DatasetFlowState {
        self.0.as_state()
    }

    /// Applies an event on the flow
    fn apply_event(
        &mut self,
        event: DatasetFlowEvent,
    ) -> Result<(), ProjectionError<DatasetFlowState>> {
        self.0.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
