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
        schedule: Schedule,
    ) -> Self {
        Self(
            Aggregate::new(
                dataset_id.clone(),
                UpdateConfigurationEventCreated {
                    event_time: now,
                    dataset_id,
                    paused,
                    schedule,
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
        new_schedule: Schedule,
    ) -> Result<(), ProjectionError<UpdateConfigurationState>> {
        let event = UpdateConfigurationEventModified {
            event_time: now,
            dataset_id: self.dataset_id.clone(),
            paused,
            schedule: new_schedule,
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
