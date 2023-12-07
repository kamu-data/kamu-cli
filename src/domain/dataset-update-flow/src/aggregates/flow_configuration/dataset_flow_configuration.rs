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

pub type DatasetFlowConfigurationState = FlowConfigurationState<DatasetFlowKey>;
pub type DatasetFlowConfigurationEvent = FlowConfigurationEvent<DatasetFlowKey>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct DatasetFlowConfiguration(
    Aggregate<DatasetFlowConfigurationState, (dyn DatasetFlowConfigurationEventStore + 'static)>,
);

impl DatasetFlowConfiguration {
    /// Creates a dataset flow configuration
    pub fn new(
        now: DateTime<Utc>,
        flow_key: DatasetFlowKey,
        paused: bool,
        rule: FlowConfigurationRule,
    ) -> Self {
        Self(
            Aggregate::new(
                flow_key.clone(),
                FlowConfigurationEventCreated::<DatasetFlowKey> {
                    event_time: now,
                    flow_key,
                    paused,
                    rule,
                },
            )
            .unwrap(),
        )
    }

    /// Handle dataset removal
    pub fn notify_dataset_removed(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), ProjectionError<DatasetFlowConfigurationState>> {
        let event = FlowConfigurationEventDatasetRemoved::<DatasetFlowKey> {
            event_time: now,
            flow_key: self.flow_key.clone(),
        };
        self.apply(event)
    }
}

impl FlowConfiguration<DatasetFlowKey> for DatasetFlowConfiguration {
    /// Returns assigned flow key
    fn flow_key(&self) -> &DatasetFlowKey {
        &self.flow_key
    }

    /// Applies an event on the flow configuration
    fn apply_event(
        &mut self,
        event: FlowConfigurationEvent<DatasetFlowKey>,
    ) -> Result<(), ProjectionError<DatasetFlowConfigurationState>> {
        self.0.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
