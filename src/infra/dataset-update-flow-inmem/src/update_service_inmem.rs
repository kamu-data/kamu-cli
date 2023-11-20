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

use dill::{component, scope, Singleton};
use futures::TryStreamExt;
use kamu_core::{InternalError, SystemTimeSource};
use kamu_dataset_update_flow::*;
use kamu_task_system::*;
use opendatafabric::{AccountID, AccountName, DatasetID};

use crate::update_time_wheel::UpdateTimeWheel;

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
    update_time_wheel: UpdateTimeWheel,
}

impl State {
    fn new() -> Self {
        Self {
            active_schedules: HashMap::new(),
            pending_updates_by_dataset: HashMap::new(),
            pending_updates_by_tasks: HashMap::new(),
            update_time_wheel: UpdateTimeWheel::new(),
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

    async fn read_initial_schedule(&self) -> Result<(), InternalError> {
        let active_schedules: Vec<_> = self
            .update_schedule_service
            .list_active_schedules()
            .try_collect()
            .await
            .int_err()?;

        let mut state = self.state.lock().unwrap();
        for active_schedule in active_schedules {
            state
                .active_schedules
                .insert(active_schedule.dataset_id.clone(), active_schedule.schedule);
        }

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
        self.read_initial_schedule().await?;

        loop {
            // TODO: implement real scheduling
            tracing::info!("Update service not implemented yet..");
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
    ) -> Result<UpdateState, RequestManualUpdateError> {
        let trigger = UpdateTrigger::Manual(UpdateTriggerManual {
            initiator_account_id,
            initiator_account_name,
        });

        let maybe_update_id = {
            let state = self.state.lock().unwrap();
            state
                .pending_updates_by_dataset
                .get(&dataset_id)
                .map(|update_id| update_id.clone())
        };

        match maybe_update_id {
            // If update is already pending for this dataset, simply merge triggers
            Some(update_id) => {
                let mut update = Update::load(update_id, self.event_store.as_ref())
                    .await
                    .int_err()?;
                update
                    .add_trigger(self.time_source.now(), trigger)
                    .int_err()?;
                update.save(self.event_store.as_ref()).await.int_err()?;
                Ok(update.into())
            }

            // Otherwise, initiate a new update
            None => {
                let mut update = Update::new(
                    self.time_source.now(),
                    self.event_store.new_update_id(),
                    dataset_id,
                    trigger,
                );
                self.schedule_update_task(&mut update).await?;
                update.save(self.event_store.as_ref()).await.int_err()?;

                {
                    let mut state = self.state.lock().unwrap();
                    state
                        .pending_updates_by_dataset
                        .insert(update.dataset_id.clone(), update.update_id);
                    Ok(update.into())
                }
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
