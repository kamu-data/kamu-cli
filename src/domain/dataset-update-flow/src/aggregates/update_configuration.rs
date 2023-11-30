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
pub struct UpdateConfiguration(
    Aggregate<UpdateConfigurationState, (dyn UpdateConfigurationEventStore + 'static)>,
);

impl UpdateConfiguration {
    /// Creates a dataset update configuration
    pub fn new(
        now: DateTime<Utc>,
        dataset_id: DatasetID,
        paused: bool,
        rule: UpdateConfigurationRule,
    ) -> Self {
        Self(
            Aggregate::new(
                dataset_id.clone(),
                UpdateConfigurationEventCreated {
                    event_time: now,
                    dataset_id,
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
        new_rule: UpdateConfigurationRule,
    ) -> Result<(), ProjectionError<UpdateConfigurationState>> {
        let event = UpdateConfigurationEventModified {
            event_time: now,
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
    ) -> Result<(), ProjectionError<UpdateConfigurationState>> {
        let event = UpdateConfigurationEventDatasetRemoved {
            event_time: now,
            dataset_id: self.dataset_id.clone(),
        };
        self.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
