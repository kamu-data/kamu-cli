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
use dill::*;
use event_bus::AsyncEventHandler;
use futures::TryStreamExt;
use kamu_core::events::DatasetEventDeleted;
use kamu_core::{InternalError, SystemTimeSource};
use kamu_dataset_update_flow::*;
use kamu_task_system::*;
use opendatafabric::{AccountID, AccountName, DatasetID};
use tokio_stream::StreamExt;

use crate::ActivityTimeWheel;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct UpdateServiceInMemory {
    state: Arc<Mutex<State>>,
    event_store: Arc<dyn UpdateEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    task_scheduler: Arc<dyn TaskScheduler>,
    update_configuration_service: Arc<dyn UpdateConfigurationService>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
}

/////////////////////////////////////////////////////////////////////////////////////////

struct State {
    active_schedules: HashMap<DatasetID, Schedule>,
    active_start_conditions: HashMap<DatasetID, StartCondition>,
    pending_updates_by_dataset: HashMap<DatasetID, UpdateID>,
    pending_updates_by_tasks: HashMap<TaskID, UpdateID>,
    time_wheel: ActivityTimeWheel,
}

impl State {
    fn new() -> Self {
        Self {
            active_schedules: HashMap::new(),
            active_start_conditions: HashMap::new(),
            pending_updates_by_dataset: HashMap::new(),
            pending_updates_by_tasks: HashMap::new(),
            time_wheel: ActivityTimeWheel::new(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn UpdateService)]
#[interface(dyn AsyncEventHandler<TaskEventFinished>)]
#[interface(dyn AsyncEventHandler<DatasetEventDeleted>)]
#[interface(dyn AsyncEventHandler<UpdateConfigurationEventModified>)]
#[scope(Singleton)]
impl UpdateServiceInMemory {
    pub fn new(
        event_store: Arc<dyn UpdateEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        task_scheduler: Arc<dyn TaskScheduler>,
        update_configuration_service: Arc<dyn UpdateConfigurationService>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
            event_store,
            time_source,
            task_scheduler,
            update_configuration_service,
            dependency_graph_service,
        }
    }

    async fn run_current_timeslot(&self) {
        let planned_updates: Vec<_> = {
            let mut state = self.state.lock().unwrap();
            state.time_wheel.take_nearest_planned_activities()
        };

        let update_task_futures: Vec<_> = planned_updates
            .iter()
            .map(async move |update_id| {
                let mut update = Update::load(UpdateID::new(*update_id), self.event_store.as_ref())
                    .await
                    .int_err()?;
                self.schedule_update_task(&mut update).await?;
                Ok(())
            })
            .collect();

        let results = futures::future::join_all(update_task_futures).await;
        results
            .into_iter()
            .filter(|res| res.is_err())
            .map(|e| e.err().unwrap())
            .for_each(|e: InternalError| {
                tracing::error!("Scheduling update failed: {:?}", e);
            });
    }

    async fn initialize_enabled_configurations(&self) -> Result<(), InternalError> {
        let enabled_configurations: Vec<_> = self
            .update_configuration_service
            .list_enabled_configurations()
            .try_collect()
            .await
            .int_err()?;

        for enabled_config in enabled_configurations {
            match enabled_config.rule {
                UpdateConfigurationRule::Schedule(schedule) => {
                    self.enqueue_auto_polling_update(&enabled_config.dataset_id, &schedule)
                        .await?;

                    let mut state = self.state.lock().unwrap();
                    state
                        .active_schedules
                        .insert(enabled_config.dataset_id.clone(), schedule);
                }
                UpdateConfigurationRule::StartCondition(start_condition) => {
                    let mut state = self.state.lock().unwrap();
                    state
                        .active_start_conditions
                        .insert(enabled_config.dataset_id.clone(), start_condition);
                }
            }
        }

        Ok(())
    }

    async fn try_enqueue_auto_polling_update_if_enabled(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<(), InternalError> {
        let maybe_active_schedule = {
            let state = self.state.lock().unwrap();
            if let Some(schedule) = state.active_schedules.get(&dataset_id) {
                Some(schedule.clone())
            } else {
                None
            }
        };

        if let Some(active_schedule) = maybe_active_schedule {
            self.enqueue_auto_polling_update(&dataset_id, &active_schedule)
                .await?;
        }

        Ok(())
    }

    async fn enqueue_auto_polling_update(
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

                let next_activation_time = schedule.next_activation_time(self.time_source.now());
                self.enqueue_update(update.update_id, next_activation_time)?;

                update
                    .activate_at_time(self.time_source.now(), next_activation_time)
                    .int_err()?;

                update.save(self.event_store.as_ref()).await.int_err()?;
                Ok(update.into())
            }
        }
    }

    async fn enqueue_dependent_updates(
        &self,
        dataset_id: &DatasetID,
        update_id: &UpdateID,
    ) -> Result<(), InternalError> {
        // Extract list of downstream 1 level datasets
        let dependent_dataset_ids: Vec<_> = self
            .dependency_graph_service
            .get_downstream_dependencies(dataset_id)
            .await
            .int_err()?
            .collect()
            .await;

        // For each, scan if updates are on
        for dependent_dataset_id in dependent_dataset_ids {
            let maybe_dependent_start_condition = self
                .state
                .lock()
                .unwrap()
                .active_start_conditions
                .get(&dependent_dataset_id)
                .map(|schedule| schedule.clone());

            if let Some(start_condition) = maybe_dependent_start_condition {
                let trigger = UpdateTrigger::InputUpdated(UpdateTriggerInputUpdated {
                    input_dataset_id: dataset_id.clone(),
                    input_update_id: update_id.clone(),
                });

                match self.find_pending_update(&dependent_dataset_id) {
                    // If update is already pending for this dataset, simply merge triggers
                    Some(dependent_update_id) => {
                        self.merge_secondary_trigger(dependent_update_id, trigger)
                            .await?;
                    }

                    // Otherwise, initiate a new update accordingly to start condition rules
                    None => {
                        let mut dependent_update = self
                            .make_new_update(dependent_dataset_id.clone(), trigger)
                            .await?;

                        if start_condition.minimal_data_batch.is_some() {
                            unimplemented!("Data batching not supported yet in scheduler")
                        }

                        if let Some(throttling_period) = start_condition.throttling_period {
                            // TODO: throttle not from NOW, but from last update of the dependent
                            // daataset
                            let now = self.time_source.now();
                            self.enqueue_update(
                                dependent_update.update_id,
                                now + throttling_period,
                            )?;

                            dependent_update
                                .define_start_condition(
                                    now,
                                    UpdateStartCondition::Throttling(
                                        UpdateStardConditionThrottling {
                                            interval: throttling_period,
                                        },
                                    ),
                                )
                                .int_err()?;
                        }

                        dependent_update
                            .save(self.event_store.as_ref())
                            .await
                            .int_err()?;
                    }
                }
            }
        }

        Ok(())
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

    fn enqueue_update(
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
        // Initial scheduling
        self.initialize_enabled_configurations().await?;

        // Main scanning loop
        loop {
            // Do we have a timeslot scheduled?
            let maybe_nearest_activation_time = {
                let state = self.state.lock().unwrap();
                state.time_wheel.nearest_activation_moment()
            };

            // Is it time to execute it yet?
            if let Some(nearest_activation_time) = maybe_nearest_activation_time
                && nearest_activation_time <= self.time_source.now()
            {
                // Run scheduling for current time slot. Should not throw any errors
                self.run_current_timeslot().await;
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

    /// Returns state of the latest update created for the given dataset
    async fn get_last_update(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Option<UpdateState>, GetLastUpdateError> {
        let res = match self.event_store.get_last_dataset_update(dataset_id) {
            Some(update_id) => Some(self.get_update(update_id).await.int_err()?),
            None => None,
        };
        Ok(res)
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
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<TaskEventFinished> for UpdateServiceInMemory {
    async fn handle(&self, event: &TaskEventFinished) -> Result<(), InternalError> {
        let maybe_update_id = {
            let state = self.state.lock().unwrap();
            state
                .pending_updates_by_tasks
                .get(&event.task_id)
                .map(|update_id| update_id.clone())
        };

        // Is this a task associated with updates?
        if let Some(update_id) = maybe_update_id {
            let mut update = Update::load(update_id, self.event_store.as_ref())
                .await
                .int_err()?;
            update
                .on_task_finished(self.time_source.now(), event.task_id, event.outcome)
                .int_err()?;

            update.save(self.event_store.as_ref()).await.int_err()?;

            {
                let mut state = self.state.lock().unwrap();
                state.pending_updates_by_tasks.remove(&event.task_id);
            }

            // In case of success:
            //  - enqueue next auto-polling update cycle
            //  - enqueue dependent datasets
            if event.outcome == TaskOutcome::Success {
                self.try_enqueue_auto_polling_update_if_enabled(&update.dataset_id)
                    .await?;

                self.enqueue_dependent_updates(&update.dataset_id, &update.update_id)
                    .await?;
            }

            // TODO: retry logic in case of failed outcome
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<UpdateConfigurationEventModified> for UpdateServiceInMemory {
    async fn handle(&self, event: &UpdateConfigurationEventModified) -> Result<(), InternalError> {
        if event.paused {
            let mut state = self.state.lock().unwrap();
            state.active_schedules.remove(&event.dataset_id);
        } else {
            match &event.rule {
                UpdateConfigurationRule::Schedule(schedule) => {
                    self.enqueue_auto_polling_update(&event.dataset_id, &schedule)
                        .await?;

                    let mut state = self.state.lock().unwrap();
                    state
                        .active_schedules
                        .entry(event.dataset_id.clone())
                        .and_modify(|e| *e = schedule.clone())
                        .or_insert(schedule.clone());
                }
                UpdateConfigurationRule::StartCondition(start_condition) => {
                    let mut state = self.state.lock().unwrap();
                    state
                        .active_start_conditions
                        .entry(event.dataset_id.clone())
                        .and_modify(|e| *e = start_condition.clone())
                        .or_insert_with(|| start_condition.clone());
                }
            }
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<DatasetEventDeleted> for UpdateServiceInMemory {
    async fn handle(&self, event: &DatasetEventDeleted) -> Result<(), InternalError> {
        let mut state = self.state.lock().unwrap();

        state.active_schedules.remove(&event.dataset_id);
        if let Some(update_id) = state.pending_updates_by_dataset.remove(&event.dataset_id) {
            state
                .time_wheel
                .cancel_activation(update_id.into())
                .int_err()?;
        }

        // Not deleting task->update association, it should be safe.
        // Most of the time the outcome of the task will be "Cancelled".
        // Even if task squeezes to succeed in between cancellations, it's safe:
        //   - we will record a successful update, no consequence
        //   - no further updates will be attempted (schedule deactivated above)
        //   - no dependent tasks will be launched (dependency graph erases neighbors)

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
