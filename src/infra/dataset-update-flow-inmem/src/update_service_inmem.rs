// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use dill::{component, scope, Singleton};
use futures::TryStreamExt;
use kamu_core::{InternalError, SystemTimeSource};
use kamu_dataset_update_flow::*;
use kamu_task_system::*;
use opendatafabric::{AccountID, AccountName, DatasetID};

use crate::ActivityTimeWheel;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct UpdateServiceInMemory {
    state: Arc<Mutex<State>>,
    event_store: Arc<dyn UpdateEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    task_scheduler: Arc<dyn TaskScheduler>,
    update_schedule_service: Arc<dyn UpdateScheduleService>,
}

/////////////////////////////////////////////////////////////////////////////////////////

struct State {
    active_schedules: HashMap<DatasetID, Schedule>,
    pending_updates_by_dataset: HashMap<DatasetID, UpdateID>,
    pending_updates_by_tasks: HashMap<TaskID, UpdateID>,
    time_wheel: ActivityTimeWheel,
}

impl State {
    fn new() -> Self {
        Self {
            active_schedules: HashMap::new(),
            pending_updates_by_dataset: HashMap::new(),
            pending_updates_by_tasks: HashMap::new(),
            time_wheel: ActivityTimeWheel::new(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[scope(Singleton)]
impl UpdateServiceInMemory {
    pub fn new(
        event_store: Arc<dyn UpdateEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        task_scheduler: Arc<dyn TaskScheduler>,
        update_schedule_service: Arc<dyn UpdateScheduleService>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
            event_store,
            time_source,
            task_scheduler,
            update_schedule_service,
        }
    }

    async fn read_initial_schedules(&self) -> Result<(), InternalError> {
        let enabled_schedules: Vec<_> = self
            .update_schedule_service
            .list_enabled_schedules()
            .try_collect()
            .await
            .int_err()?;

        for enabled_schedule in enabled_schedules {
            if enabled_schedule.schedule.is_active() {
                self.queue_auto_polling_update(
                    &enabled_schedule.dataset_id,
                    &enabled_schedule.schedule,
                )
                .await?;
            }

            let mut state = self.state.lock().unwrap();
            state.active_schedules.insert(
                enabled_schedule.dataset_id.clone(),
                enabled_schedule.schedule,
            );
        }

        Ok(())
    }

    async fn queue_auto_polling_update(
        &self,
        dataset_id: &DatasetID,
        schedule: &Schedule,
    ) -> Result<UpdateState, InternalError> {
        let trigger = UpdateTrigger::AutoPolling(UpdateTriggerAutoPolling {});

        match self.find_pending_update(&dataset_id) {
            // If update is already pending for this dataset, simply merge triggers
            Some(update_id) => self.merge_secondary_trigger(update_id, trigger).await,

            // Otherwise, initiate a new update, and enqueue it in the time wheel
            None => {
                let mut update = self.make_new_update(dataset_id.clone(), trigger).await?;

                if let Some(next_activation_time) =
                    schedule.next_activation_time(self.time_source.now())
                {
                    self.queue_update(update.update_id, next_activation_time)?;
                    update
                        .queued_for_time(self.time_source.now(), next_activation_time)
                        .int_err()?;
                }

                update.save(self.event_store.as_ref()).await.int_err()?;
                Ok(update.into())
            }
        }
    }

    fn find_pending_update(&self, dataset_id: &DatasetID) -> Option<UpdateID> {
        let state = self.state.lock().unwrap();
        state
            .pending_updates_by_dataset
            .get(&dataset_id)
            .map(|update_id| update_id.to_owned())
    }

    async fn make_new_update(
        &self,
        dataset_id: DatasetID,
        trigger: UpdateTrigger,
    ) -> Result<Update, InternalError> {
        let update = Update::new(
            self.time_source.now(),
            self.event_store.new_update_id(),
            dataset_id,
            trigger,
        );

        {
            let mut state = self.state.lock().unwrap();
            state
                .pending_updates_by_dataset
                .insert(update.dataset_id.clone(), update.update_id);
        }

        Ok(update)
    }

    async fn merge_secondary_trigger(
        &self,
        update_id: UpdateID,
        trigger: UpdateTrigger,
    ) -> Result<UpdateState, InternalError> {
        let mut update = Update::load(update_id, self.event_store.as_ref())
            .await
            .int_err()?;
        update
            .add_trigger(self.time_source.now(), trigger)
            .int_err()?;
        update.save(self.event_store.as_ref()).await.int_err()?;
        Ok(update.into())
    }

    fn queue_update(
        &self,
        update_id: UpdateID,
        activation_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        self.state
            .lock()
            .unwrap()
            .time_wheel
            .activate_at(activation_time, update_id.into())?;
        Ok(())
    }

    async fn schedule_update_task(&self, update: &mut Update) -> Result<(), InternalError> {
        let task = self
            .task_scheduler
            .create_task(LogicalPlan::UpdateDataset(UpdateDataset {
                dataset_id: update.dataset_id.clone(),
            }))
            .await
            .int_err()?;

        update
            .on_task_scheduled(self.time_source.now(), task.task_id)
            .int_err()?;

        let mut state = self.state.lock().unwrap();
        state
            .pending_updates_by_tasks
            .insert(task.task_id, update.update_id);

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl UpdateService for UpdateServiceInMemory {
    /// Runs the update main loop
    async fn run(&self) -> Result<(), InternalError> {
        self.read_initial_schedules().await?;

        loop {
            let maybe_nearest_activation_time = {
                let state = self.state.lock().unwrap();
                state.time_wheel.nearest_activation_moment()
            };

            if let Some(nearest_activation_time) = maybe_nearest_activation_time
                && nearest_activation_time <= self.time_source.now()
            {
                let planned_updates: Vec<_> = {
                    let state = self.state.lock().unwrap();
                    state.time_wheel.nearest_planned_activities().collect()
                };

                for update_id in planned_updates {
                    let mut update =
                        Update::load(UpdateID::new(update_id), self.event_store.as_ref())
                            .await
                            .int_err()?;
                    self.schedule_update_task(&mut update).await?;
                }

                let mut state = self.state.lock().unwrap();
                state.time_wheel.spin();
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            continue;
        }
    }

    /// Creates a new manual update request
    async fn request_manual_update(
        &self,
        dataset_id: DatasetID,
        initiator_account_id: AccountID,
        initiator_account_name: AccountName,
    ) -> Result<UpdateState, RequestUpdateError> {
        let trigger = UpdateTrigger::Manual(UpdateTriggerManual {
            initiator_account_id,
            initiator_account_name,
        });

        match self.find_pending_update(&dataset_id) {
            // If update is already pending for this dataset, simply merge triggers
            Some(update_id) => self
                .merge_secondary_trigger(update_id, trigger)
                .await
                .map_err(|e| RequestUpdateError::Internal(e)),

            // Otherwise, initiate a new update and schedule immediate task
            None => {
                let mut update = self.make_new_update(dataset_id, trigger).await?;
                self.schedule_update_task(&mut update).await?;
                update.save(self.event_store.as_ref()).await.int_err()?;
                Ok(update.into())
            }
        }
    }

    /// Returns states of updates associated with a given dataset ordered by
    /// creation time from newest to oldest
    fn list_updates_by_dataset(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<UpdateStateStream, ListUpdatesByDatasetError> {
        let dataset_id = dataset_id.clone();

        Ok(Box::pin(async_stream::try_stream! {
            let relevant_updates: Vec<_> = self
                .event_store
                .get_updates_by_dataset(&dataset_id)
                .try_collect()
                .await?;

            for update_id in relevant_updates.into_iter() {
                let update = Update::load(update_id, self.event_store.as_ref()).await.int_err()?;

                yield update.into();
            }
        }))
    }

    /// Returns current state of a given update
    async fn get_update(&self, update_id: UpdateID) -> Result<UpdateState, GetUpdateError> {
        let update = Update::load(update_id, self.event_store.as_ref()).await?;
        Ok(update.into())
    }

    /// Attempts to cancel the given update
    async fn cancel_update(
        &self,
        update_id: UpdateID,
        by_account_id: AccountID,
        by_account_name: AccountName,
    ) -> Result<UpdateState, CancelUpdateError> {
        let mut update = Update::load(update_id, self.event_store.as_ref()).await?;

        if update.can_cancel() {
            update
                .cancel(self.time_source.now(), by_account_id, by_account_name)
                .int_err()?;
            update.save(self.event_store.as_ref()).await.int_err()?;

            {
                let mut state = self.state.lock().unwrap();
                if state
                    .time_wheel
                    .is_activation_planned(update.update_id.into())
                {
                    state
                        .time_wheel
                        .cancel_activation(update.update_id.into())
                        .map_err(|e| CancelUpdateError::Internal(e.int_err()))?;
                }
                state.pending_updates_by_dataset.remove(&update.dataset_id);
            }
        }

        Ok(update.into())
    }

    /// Handles task execution outcome.
    /// Reacts correspondingly if the task is related to updates
    ///
    /// TODO: connect to event bus
    async fn on_task_finished(
        &self,
        task_id: TaskID,
        task_outcome: TaskOutcome,
    ) -> Result<Option<UpdateState>, InternalError> {
        let maybe_update_id = {
            let state = self.state.lock().unwrap();
            state
                .pending_updates_by_tasks
                .get(&task_id)
                .map(|update_id| update_id.clone())
        };

        if let Some(update_id) = maybe_update_id {
            let mut update = Update::load(update_id, self.event_store.as_ref())
                .await
                .int_err()?;
            update
                .on_task_finished(self.time_source.now(), task_id, task_outcome)
                .int_err()?;

            update.save(self.event_store.as_ref()).await.int_err()?;

            {
                let mut state = self.state.lock().unwrap();
                state.pending_updates_by_tasks.remove(&task_id);
            }

            Ok(Some(update.into()))
        } else {
            Ok(None)
        }
    }

    /// Notifies about changes in dataset update schedule
    ///
    /// TODO: connect to event bus
    async fn update_schedule_modified(
        &self,
        update_schedule_state: UpdateScheduleState,
    ) -> Result<(), InternalError> {
        let dataset_id = update_schedule_state.dataset_id;
        let schedule = update_schedule_state.schedule;

        if schedule.is_active() {
            self.queue_auto_polling_update(&dataset_id, &schedule)
                .await?;
        }

        let mut state = self.state.lock().unwrap();
        state
            .active_schedules
            .entry(dataset_id)
            .and_modify(|e| *e = schedule.clone())
            .or_insert(schedule);

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
