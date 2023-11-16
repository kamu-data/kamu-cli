// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use event_sourcing::*;
use opendatafabric::DatasetID;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct DatasetUpdateFlow(
    Aggregate<DatasetUpdateFlowState, (dyn DatasetUpdateFlowEventStore + 'static)>,
);

impl DatasetUpdateFlow {
    /// Creates a dataset update flow
    pub fn new(dataset_id: DatasetID, schedule: UpdateSchedule) -> Self {
        Self(
            Aggregate::new(
                dataset_id.clone(),
                DatasetUpdateFlowCreated {
                    event_time: Utc::now(),
                    dataset_id,
                    schedule,
                },
            )
            .unwrap(),
        )
    }

    /// Pause flow
    pub fn pause(&mut self) -> Result<(), ProjectionError<DatasetUpdateFlowState>> {
        let event = DatasetUpdateFlowPaused {
            event_time: Utc::now(),
            dataset_id: self.dataset_id.clone(),
        };
        self.apply(event)
    }

    /// Resume flow
    pub fn resume(&mut self) -> Result<(), ProjectionError<DatasetUpdateFlowState>> {
        let event = DatasetUpdateFlowResumed {
            event_time: Utc::now(),
            dataset_id: self.dataset_id.clone(),
        };
        self.apply(event)
    }

    /// Modifie schedule
    pub fn modify_schedule(
        &mut self,
        new_schedule: UpdateSchedule,
    ) -> Result<(), ProjectionError<DatasetUpdateFlowState>> {
        let event = DatasetUpdateFlowScheduleModified {
            event_time: Utc::now(),
            dataset_id: self.dataset_id.clone(),
            new_schedule,
        };
        self.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
