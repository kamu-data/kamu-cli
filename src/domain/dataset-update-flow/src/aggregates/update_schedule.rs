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
pub struct UpdateSchedule(Aggregate<UpdateScheduleState, (dyn UpdateScheduleEventStore + 'static)>);

impl UpdateSchedule {
    /// Creates a dataset update flow
    pub fn new(now: DateTime<Utc>, dataset_id: DatasetID, schedule: Schedule) -> Self {
        Self(
            Aggregate::new(
                dataset_id.clone(),
                UpdateScheduleEventCreated {
                    event_time: now,
                    dataset_id,
                    schedule,
                },
            )
            .unwrap(),
        )
    }

    /// Pause flow
    pub fn pause(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), ProjectionError<UpdateScheduleState>> {
        let event = UpdateScheduleEventModified {
            event_time: now,
            dataset_id: self.dataset_id.clone(),
            paused: true,
            schedule: self.schedule.clone(),
        };
        self.apply(event)
    }

    /// Resume flow
    pub fn resume(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), ProjectionError<UpdateScheduleState>> {
        let event = UpdateScheduleEventModified {
            event_time: now,
            dataset_id: self.dataset_id.clone(),
            paused: false,
            schedule: self.schedule.clone(),
        };
        self.apply(event)
    }

    /// Modify schedule
    pub fn modify_schedule(
        &mut self,
        now: DateTime<Utc>,
        new_schedule: Schedule,
    ) -> Result<(), ProjectionError<UpdateScheduleState>> {
        let event = UpdateScheduleEventModified {
            event_time: now,
            dataset_id: self.dataset_id.clone(),
            paused: self.is_active(),
            schedule: new_schedule,
        };
        self.apply(event)
    }

    /// Handle dataset removal
    pub fn notify_dataset_removed(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), ProjectionError<UpdateScheduleState>> {
        let event = UpdateScheduleEventDatasetRemoved {
            event_time: now,
            dataset_id: self.dataset_id.clone(),
        };
        self.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
