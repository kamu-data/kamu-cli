// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
use kamu_task_system::TaskID;
use opendatafabric::DatasetID;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct Update(Aggregate<UpdateState, (dyn UpdateEventStore + 'static)>);

impl Update {
    /// Creates a dataset update process
    pub fn new(update_id: UpdateID, dataset_id: DatasetID, trigger: UpdateTrigger) -> Self {
        Self(
            Aggregate::new(
                dataset_id.clone(),
                UpdateEventInitiated {
                    event_time: Utc::now(),
                    update_id,
                    dataset_id,
                    trigger,
                },
            )
            .unwrap(),
        )
    }

    /// Postpone launching for now
    pub fn postpone(
        &mut self,
        delay_reason: UpdateDelayReason,
    ) -> Result<(), ProjectionError<UpdateState>> {
        let event = UpdateEventPostponed {
            event_time: Utc::now(),
            update_id: self.update_id.clone(),
            delay_reason,
        };
        self.apply(event)
    }

    /// Queue for time
    pub fn queue_for_time(
        &mut self,
        queued_for: DateTime<Utc>,
    ) -> Result<(), ProjectionError<UpdateState>> {
        let event = UpdateEventQueued {
            event_time: Utc::now(),
            update_id: self.update_id.clone(),
            queued_for,
        };
        self.apply(event)
    }

    /// Extra trigger
    pub fn retrigger(
        &mut self,
        trigger: UpdateTrigger,
    ) -> Result<(), ProjectionError<UpdateState>> {
        let event = UpdateEventSecondaryTrigger {
            event_time: Utc::now(),
            update_id: self.update_id.clone(),
            trigger,
        };
        self.apply(event)
    }

    /// Attaches a scheduled task
    pub fn on_task_scheduled(
        &mut self,
        task_id: TaskID,
    ) -> Result<(), ProjectionError<UpdateState>> {
        let event = UpdateEventTaskScheduled {
            event_time: Utc::now(),
            update_id: self.update_id.clone(),
            task_id,
        };
        self.apply(event)
    }

    /// Task succeeded
    pub fn on_task_success(&mut self, task_id: TaskID) -> Result<(), ProjectionError<UpdateState>> {
        let event = UpdateEventTaskSucceeded {
            event_time: Utc::now(),
            update_id: self.update_id.clone(),
            task_id,
        };
        self.apply(event)
    }

    /// Task cancelled
    pub fn on_task_cancelled(
        &mut self,
        task_id: TaskID,
    ) -> Result<(), ProjectionError<UpdateState>> {
        let event = UpdateEventTaskCancelled {
            event_time: Utc::now(),
            update_id: self.update_id.clone(),
            task_id,
        };
        self.apply(event)
    }

    /// Task failed
    pub fn on_task_failed(&mut self, task_id: TaskID) -> Result<(), ProjectionError<UpdateState>> {
        let event = UpdateEventTaskFailed {
            event_time: Utc::now(),
            update_id: self.update_id.clone(),
            task_id,
        };
        self.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
