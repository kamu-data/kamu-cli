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
use opendatafabric::DatasetID;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct DatasetFlowConfiguration(
    Aggregate<DatasetFlowConfigurationState, (dyn DatasetFlowConfigurationEventStore + 'static)>,
);

impl DatasetFlowConfiguration {
    /// Creates a dataset flow configuration
    pub fn new(
        now: DateTime<Utc>,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
        paused: bool,
        rule: DatasetFlowConfigurationRule,
    ) -> Self {
        Self(
            Aggregate::new(
                (dataset_id.clone(), flow_type),
                DatasetFlowConfigurationEventCreated {
                    event_time: now,
                    dataset_id,
                    flow_type,
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
        new_rule: DatasetFlowConfigurationRule,
    ) -> Result<(), ProjectionError<DatasetFlowConfigurationState>> {
        let event = DatasetFlowConfigurationEventModified {
            event_time: now,
            flow_type: self.flow_type,
            dataset_id: self.dataset_id.clone(),
            paused,
            rule: new_rule,
        };
        self.apply(event)
    }

    /// Handle dataset removal
    pub fn notify_dataset_removed(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), ProjectionError<DatasetFlowConfigurationState>> {
        let event = DatasetFlowConfigurationEventDatasetRemoved {
            event_time: now,
            dataset_id: self.dataset_id.clone(),
            flow_type: self.flow_type,
        };
        self.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
