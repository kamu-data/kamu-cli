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
use kamu_task_system::{TaskID, TaskOutcome};
use opendatafabric::{AccountID, AccountName, DatasetID};

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct Update(Aggregate<UpdateState, (dyn UpdateEventStore + 'static)>);

impl Update {
    /// Creates a dataset update process
    pub fn new(
        now: DateTime<Utc>,
        update_id: UpdateID,
        dataset_id: DatasetID,
        trigger: UpdateTrigger,
    ) -> Self {
        Self(
            Aggregate::new(
                dataset_id.clone(),
                UpdateEventInitiated {
                    event_time: now,
                    update_id,
                    dataset_id,
                    trigger,
                },
            )
            .unwrap(),
        )
    }

    /// Define start condition for the history
    pub fn define_start_condition(
        &mut self,
        now: DateTime<Utc>,
        start_condition: UpdateStartCondition,
    ) -> Result<(), ProjectionError<UpdateState>> {
        let event = UpdateEventStartConditionDefined {
            event_time: now,
            update_id: self.update_id.clone(),
            start_condition,
        };
        self.apply(event)
    }

    /// Queue for time
    pub fn queue_for_time(
        &mut self,
        now: DateTime<Utc>,
        queued_for: DateTime<Utc>,
    ) -> Result<(), ProjectionError<UpdateState>> {
        let event = UpdateEventQueued {
            event_time: now,
            update_id: self.update_id.clone(),
            queued_for,
        };
        self.apply(event)
    }

    /// Extra trigger
    pub fn add_trigger(
        &mut self,
        now: DateTime<Utc>,
        trigger: UpdateTrigger,
    ) -> Result<(), ProjectionError<UpdateState>> {
        let event = UpdateEventTriggerAdded {
            event_time: now,
            update_id: self.update_id.clone(),
            trigger,
        };
        self.apply(event)
    }

    /// Attaches a scheduled task
    pub fn on_task_scheduled(
        &mut self,
        now: DateTime<Utc>,
        task_id: TaskID,
    ) -> Result<(), ProjectionError<UpdateState>> {
        let event = UpdateEventTaskScheduled {
            event_time: now,
            update_id: self.update_id.clone(),
            task_id,
        };
        self.apply(event)
    }

    /// Task finished
    pub fn on_task_finished(
        &mut self,
        now: DateTime<Utc>,
        task_id: TaskID,
        task_outcome: TaskOutcome,
    ) -> Result<(), ProjectionError<UpdateState>> {
        let event = UpdateEventTaskFinished {
            event_time: now,
            update_id: self.update_id.clone(),
            task_id,
            task_outcome,
        };
        self.apply(event)
    }

    /// Cancel update before task started
    pub fn cancel(
        &mut self,
        now: DateTime<Utc>,
        by_account_id: AccountID,
        by_account_name: AccountName,
    ) -> Result<(), ProjectionError<UpdateState>> {
        let event = UpdateEventCancelled {
            event_time: now,
            update_id: self.update_id.clone(),
            by_account_id,
            by_account_name,
        };
        self.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
