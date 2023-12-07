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

use crate::dataset_flow_key::{BorrowedDatasetFlowKey, OwnedDatasetFlowKey};
use crate::ActivityTimeWheel;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowServiceInMemory {
    state: Arc<Mutex<State>>,
    dataset_flow_event_store: Arc<dyn DatasetFlowEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    task_scheduler: Arc<dyn TaskScheduler>,
    dataset_flow_configuration_service: Arc<dyn DatasetFlowConfigurationService>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
}

/////////////////////////////////////////////////////////////////////////////////////////

struct State {
    active_schedules: HashMap<OwnedDatasetFlowKey, Schedule>,
    active_start_conditions: HashMap<OwnedDatasetFlowKey, StartConditionConfiguration>,
    pending_dataset_flows_by_dataset: HashMap<OwnedDatasetFlowKey, DatasetFlowID>,
    pending_dataset_flows_by_tasks: HashMap<TaskID, DatasetFlowID>,
    time_wheel: ActivityTimeWheel,
}

impl State {
    fn new() -> Self {
        Self {
            active_schedules: HashMap::new(),
            active_start_conditions: HashMap::new(),
            pending_dataset_flows_by_dataset: HashMap::new(),
            pending_dataset_flows_by_tasks: HashMap::new(),
            time_wheel: ActivityTimeWheel::new(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowService)]
#[interface(dyn AsyncEventHandler<TaskEventFinished>)]
#[interface(dyn AsyncEventHandler<DatasetEventDeleted>)]
#[interface(dyn AsyncEventHandler<FlowConfigurationEventModified<DatasetFlowKey>>)]
#[scope(Singleton)]
impl FlowServiceInMemory {
    pub fn new(
        dataset_flow_event_store: Arc<dyn DatasetFlowEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        task_scheduler: Arc<dyn TaskScheduler>,
        dataset_flow_configuration_service: Arc<dyn DatasetFlowConfigurationService>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
            dataset_flow_event_store,
            time_source,
            task_scheduler,
            dataset_flow_configuration_service,
            dependency_graph_service,
        }
    }

    async fn run_current_timeslot(&self) {
        let planned_flows: Vec<_> = {
            let mut state = self.state.lock().unwrap();
            state.time_wheel.take_nearest_planned_activities()
        };

        let planned_task_futures: Vec<_> = planned_flows
            .iter()
            .map(async move |flow_id| {
                // TODO: distinguish system flows
                let mut dataset_flow = DatasetFlow::load(
                    DatasetFlowID::new(*flow_id),
                    self.dataset_flow_event_store.as_ref(),
                )
                .await
                .int_err()?;
                self.schedule_dataset_flow_task(&mut dataset_flow).await?;
                Ok(())
            })
            .collect();

        let results = futures::future::join_all(planned_task_futures).await;
        results
            .into_iter()
            .filter(|res| res.is_err())
            .map(|e| e.err().unwrap())
            .for_each(|e: InternalError| {
                tracing::error!("Scheduling flow failed: {:?}", e);
            });
    }

    async fn initialize_enabled_dataset_configurations(
        &self,
        flow_type: DatasetFlowType,
    ) -> Result<(), InternalError> {
        let enabled_configurations: Vec<_> = self
            .dataset_flow_configuration_service
            .list_enabled_configurations(flow_type)
            .try_collect()
            .await
            .int_err()?;

        for enabled_config in enabled_configurations {
            match enabled_config.rule {
                FlowConfigurationRule::Schedule(schedule) => {
                    self.enqueue_auto_polling_dataset_flow(
                        &enabled_config.flow_key.dataset_id,
                        flow_type,
                        &schedule,
                    )
                    .await?;

                    let mut state = self.state.lock().unwrap();
                    state.active_schedules.insert(
                        OwnedDatasetFlowKey::new(
                            enabled_config.flow_key.dataset_id.clone(),
                            flow_type,
                        ),
                        schedule,
                    );
                }
                FlowConfigurationRule::StartCondition(start_condition) => {
                    let mut state = self.state.lock().unwrap();
                    state.active_start_conditions.insert(
                        OwnedDatasetFlowKey::new(
                            enabled_config.flow_key.dataset_id.clone(),
                            flow_type,
                        ),
                        start_condition,
                    );
                }
            }
        }

        Ok(())
    }

    async fn try_enqueue_auto_polling_dataset_flow_if_enabled(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<(), InternalError> {
        let maybe_active_schedule = {
            let state = self.state.lock().unwrap();
            if let Some(schedule) = state
                .active_schedules
                .get(BorrowedDatasetFlowKey::new(&dataset_id, flow_type).as_trait())
            {
                Some(schedule.clone())
            } else {
                None
            }
        };

        if let Some(active_schedule) = maybe_active_schedule {
            self.enqueue_auto_polling_dataset_flow(&dataset_id, flow_type, &active_schedule)
                .await?;
        }

        Ok(())
    }

    async fn enqueue_auto_polling_dataset_flow(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
        schedule: &Schedule,
    ) -> Result<DatasetFlowState, InternalError> {
        let trigger = FlowTrigger::AutoPolling(FlowTriggerAutoPolling {});

        match self.find_pending_dataset_flow(&dataset_id, flow_type) {
            // If flow is already pending for this dataset, simply merge triggers
            Some(flow_id) => self.merge_secondary_trigger(flow_id, trigger).await,

            // Otherwise, initiate a new flow, and enqueue it in the time wheel
            None => {
                let mut dataset_flow = self
                    .make_new_dataset_flow(dataset_id.clone(), flow_type, trigger)
                    .await?;

                let next_activation_time = schedule.next_activation_time(self.time_source.now());
                self.enqueue_dataset_flow(dataset_flow.flow_id, next_activation_time)?;

                dataset_flow
                    .activate_at_time(self.time_source.now(), next_activation_time)
                    .int_err()?;

                dataset_flow
                    .save(self.dataset_flow_event_store.as_ref())
                    .await
                    .int_err()?;
                Ok(dataset_flow.into())
            }
        }
    }

    async fn enqueue_dependent_dataset_flows(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
        flow_id: DatasetFlowID,
    ) -> Result<(), InternalError> {
        // TODO: this is applicable to updates only

        // Extract list of downstream 1 level datasets
        let dependent_dataset_ids: Vec<_> = self
            .dependency_graph_service
            .get_downstream_dependencies(dataset_id)
            .await
            .int_err()?
            .collect()
            .await;

        // For each, scan if flows configurations are on
        for dependent_dataset_id in dependent_dataset_ids {
            let maybe_dependent_start_condition = self
                .state
                .lock()
                .unwrap()
                .active_start_conditions
                .get(BorrowedDatasetFlowKey::new(&dependent_dataset_id, flow_type).as_trait())
                .map(|schedule| schedule.clone());

            if let Some(start_condition) = maybe_dependent_start_condition {
                let trigger = FlowTrigger::InputDatasetFlow(FlowTriggerInputDatasetFlow {
                    input_dataset_id: dataset_id.clone(),
                    input_flow_type: flow_type,
                    input_flow_id: flow_id,
                });

                match self.find_pending_dataset_flow(&dependent_dataset_id, flow_type) {
                    // If flow is already pending for this dataset, simply merge triggers
                    Some(dependent_flow_id) => {
                        self.merge_secondary_trigger(dependent_flow_id, trigger)
                            .await?;
                    }

                    // Otherwise, initiate a new update accordingly to start condition rules
                    None => {
                        let mut dependent_dataset_flow = self
                            .make_new_dataset_flow(dependent_dataset_id.clone(), flow_type, trigger)
                            .await?;

                        if start_condition.minimal_data_batch.is_some() {
                            unimplemented!("Data batching not supported yet in scheduler")
                        }

                        if let Some(throttling_period) = start_condition.throttling_period {
                            // TODO: throttle not from NOW,
                            //  but from last flow of the dependent daataset
                            let now = self.time_source.now();
                            self.enqueue_dataset_flow(
                                dependent_dataset_flow.flow_id,
                                now + throttling_period,
                            )?;

                            dependent_dataset_flow
                                .define_start_condition(
                                    now,
                                    FlowStartCondition::Throttling(FlowStartConditionThrottling {
                                        interval: throttling_period,
                                    }),
                                )
                                .int_err()?;
                        }

                        dependent_dataset_flow
                            .save(self.dataset_flow_event_store.as_ref())
                            .await
                            .int_err()?;
                    }
                }
            }
        }

        Ok(())
    }

    fn find_pending_dataset_flow(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<DatasetFlowID> {
        let state = self.state.lock().unwrap();
        state
            .pending_dataset_flows_by_dataset
            .get(BorrowedDatasetFlowKey::new(&dataset_id, flow_type).as_trait())
            .map(|flow_id| *flow_id)
    }

    async fn make_new_dataset_flow(
        &self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
        trigger: FlowTrigger,
    ) -> Result<DatasetFlow, InternalError> {
        let dataset_flow = DatasetFlow::new(
            self.time_source.now(),
            self.dataset_flow_event_store.new_flow_id(),
            DatasetFlowKey::new(dataset_id, flow_type),
            trigger,
        );

        {
            let mut state = self.state.lock().unwrap();
            state.pending_dataset_flows_by_dataset.insert(
                OwnedDatasetFlowKey::new(dataset_flow.flow_key.dataset_id.clone(), flow_type),
                dataset_flow.flow_id,
            );
        }

        Ok(dataset_flow)
    }

    async fn merge_secondary_trigger(
        &self,
        flow_id: DatasetFlowID,
        trigger: FlowTrigger,
    ) -> Result<DatasetFlowState, InternalError> {
        let mut dataset_flow = DatasetFlow::load(flow_id, self.dataset_flow_event_store.as_ref())
            .await
            .int_err()?;
        dataset_flow
            .add_trigger(self.time_source.now(), trigger)
            .int_err()?;
        dataset_flow
            .save(self.dataset_flow_event_store.as_ref())
            .await
            .int_err()?;
        Ok(dataset_flow.into())
    }

    fn enqueue_dataset_flow(
        &self,
        flow_id: DatasetFlowID,
        activation_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        self.state
            .lock()
            .unwrap()
            .time_wheel
            .activate_at(activation_time, flow_id.into())?;
        Ok(())
    }

    async fn schedule_dataset_flow_task(
        &self,
        dataset_flow: &mut DatasetFlow,
    ) -> Result<(), InternalError> {
        let logical_plan = match dataset_flow.flow_key.flow_type {
            DatasetFlowType::Update => LogicalPlan::UpdateDataset(UpdateDataset {
                dataset_id: dataset_flow.flow_key.dataset_id.clone(),
            }),
            DatasetFlowType::Compacting => unimplemented!(),
        };

        let task = self
            .task_scheduler
            .create_task(logical_plan)
            .await
            .int_err()?;

        dataset_flow
            .on_task_scheduled(self.time_source.now(), task.task_id)
            .int_err()?;

        let mut state = self.state.lock().unwrap();
        state
            .pending_dataset_flows_by_tasks
            .insert(task.task_id, dataset_flow.flow_id);

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowService for FlowServiceInMemory {
    /// Runs the update main loop
    async fn run(&self) -> Result<(), InternalError> {
        // Initial scheduling
        // TODO: other dataset flow types, system flows
        self.initialize_enabled_dataset_configurations(DatasetFlowType::Update)
            .await?;

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

    /// Creates a new manual dataset flow request
    async fn request_manual_dataset_flow(
        &self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
        initiator_account_id: AccountID,
        initiator_account_name: AccountName,
    ) -> Result<DatasetFlowState, RequestFlowError> {
        let trigger = FlowTrigger::Manual(FlowTriggerManual {
            initiator_account_id,
            initiator_account_name,
        });

        match self.find_pending_dataset_flow(&dataset_id, flow_type) {
            // If flow is already pending for this dataset, simply merge triggers
            Some(flow_id) => self
                .merge_secondary_trigger(flow_id, trigger)
                .await
                .map_err(|e| RequestFlowError::Internal(e)),

            // Otherwise, initiate a new flow and schedule immediate task
            None => {
                let mut dataset_flow = self
                    .make_new_dataset_flow(dataset_id, flow_type, trigger)
                    .await?;
                self.schedule_dataset_flow_task(&mut dataset_flow).await?;
                dataset_flow
                    .save(self.dataset_flow_event_store.as_ref())
                    .await
                    .int_err()?;
                Ok(dataset_flow.into())
            }
        }
    }

    async fn request_manual_system_flow(
        &self,
        flow_type: SystemFlowType,
        initiator_account_id: AccountID,
        initiator_account_name: AccountName,
    ) -> Result<SystemFlowState, RequestFlowError> {
        unimplemented!()
    }

    /// Returns states of flows of certian type associated with a given dataset
    /// ordered by creation time from newest to oldest
    fn list_specific_flows_by_dataset(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<DatasetFlowStateStream, ListFlowsByDatasetError> {
        let dataset_id = dataset_id.clone();

        Ok(Box::pin(async_stream::try_stream! {
            let relevant_updates: Vec<_> = self
                .dataset_flow_event_store
                .get_specific_flows_by_dataset(&dataset_id, flow_type)
                .try_collect()
                .await?;

            for update_id in relevant_updates.into_iter() {
                let update = DatasetFlow::load(update_id, self.dataset_flow_event_store.as_ref()).await.int_err()?;

                yield update.into();
            }
        }))
    }

    /// Returns states of system flows of certian type
    /// ordered by creation time from newest to oldest
    fn list_specific_system_flows(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<SystemFlowStateStream, ListSystemFlowsError> {
        unimplemented!()
    }

    /// Returns states of flows of any type associated with a given dataset
    /// ordered by creation time from newest to oldest
    fn list_all_flows_by_dataset(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetFlowStateStream, ListFlowsByDatasetError> {
        unimplemented!()
    }

    /// Returns states of system flows of any type
    /// ordered by creation time from newest to oldest
    fn list_all_system_flows(&self) -> Result<SystemFlowStateStream, ListSystemFlowsError> {
        unimplemented!()
    }

    /// Returns state of the latest flow of certain type created for the given
    /// dataset
    async fn get_last_specific_flow_by_dataset(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<DatasetFlowState>, GetLastDatasetFlowError> {
        let res = match self
            .dataset_flow_event_store
            .get_last_specific_dataset_flow(dataset_id, flow_type)
        {
            Some(flow_id) => Some(self.get_dataset_flow(flow_id).await.int_err()?),
            None => None,
        };
        Ok(res)
    }

    /// Returns state of the latest sstem flow of certain type
    async fn get_last_specific_system_flow(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<Option<SystemFlowState>, GetLastSystemtFlowError> {
        unimplemented!()
    }

    /// Returns current state of a given dataset flow
    async fn get_dataset_flow(
        &self,
        flow_id: DatasetFlowID,
    ) -> Result<DatasetFlowState, GetDatasetFlowError> {
        let dataset_flow =
            DatasetFlow::load(flow_id, self.dataset_flow_event_store.as_ref()).await?;
        Ok(dataset_flow.into())
    }

    /// Returns current state of a given system flow
    async fn get_system_flow(
        &self,
        flow_id: SystemFlowID,
    ) -> Result<SystemFlowState, GetSystemFlowError> {
        unimplemented!()
    }

    /// Attempts to cancel the given dataset flow
    async fn cancel_dataset_flow(
        &self,
        flow_id: DatasetFlowID,
        by_account_id: AccountID,
        by_account_name: AccountName,
    ) -> Result<DatasetFlowState, CancelDatasetFlowError> {
        let mut dataset_flow =
            DatasetFlow::load(flow_id, self.dataset_flow_event_store.as_ref()).await?;

        if dataset_flow.can_cancel() {
            dataset_flow
                .cancel(self.time_source.now(), by_account_id, by_account_name)
                .int_err()?;
            dataset_flow
                .save(self.dataset_flow_event_store.as_ref())
                .await
                .int_err()?;

            {
                let mut state = self.state.lock().unwrap();
                if state
                    .time_wheel
                    .is_activation_planned(dataset_flow.flow_id.into())
                {
                    state
                        .time_wheel
                        .cancel_activation(dataset_flow.flow_id.into())
                        .map_err(|e| CancelDatasetFlowError::Internal(e.int_err()))?;
                }
                state.pending_dataset_flows_by_dataset.remove(
                    BorrowedDatasetFlowKey::new(
                        &dataset_flow.flow_key.dataset_id,
                        dataset_flow.flow_key.flow_type,
                    )
                    .as_trait(),
                );
            }
        }

        Ok(dataset_flow.into())
    }

    /// Attempts to cancel the given system flow
    async fn cancel_system_flow(
        &self,
        flow_id: SystemFlowID,
        by_account_id: AccountID,
        by_account_name: AccountName,
    ) -> Result<SystemFlowState, CancelSystemFlowError> {
        unimplemented!()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<TaskEventFinished> for FlowServiceInMemory {
    async fn handle(&self, event: &TaskEventFinished) -> Result<(), InternalError> {
        let maybe_dataset_flow_id = {
            let state = self.state.lock().unwrap();
            state
                .pending_dataset_flows_by_tasks
                .get(&event.task_id)
                .map(|flow_id: &DatasetFlowID| *flow_id)
        };

        // Is this a task associated with flows?
        if let Some(flow_id) = maybe_dataset_flow_id {
            // TODO: distinguish system flows
            let mut dataset_flow =
                DatasetFlow::load(flow_id, self.dataset_flow_event_store.as_ref())
                    .await
                    .int_err()?;
            dataset_flow
                .on_task_finished(self.time_source.now(), event.task_id, event.outcome)
                .int_err()?;

            dataset_flow
                .save(self.dataset_flow_event_store.as_ref())
                .await
                .int_err()?;

            {
                let mut state = self.state.lock().unwrap();
                state.pending_dataset_flows_by_tasks.remove(&event.task_id);
            }

            // In case of success:
            //  - enqueue next auto-polling flow cycle
            //  - enqueue dependent datasets
            if event.outcome == TaskOutcome::Success {
                self.try_enqueue_auto_polling_dataset_flow_if_enabled(
                    &dataset_flow.flow_key.dataset_id,
                    dataset_flow.flow_key.flow_type,
                )
                .await?;

                self.enqueue_dependent_dataset_flows(
                    &dataset_flow.flow_key.dataset_id,
                    dataset_flow.flow_key.flow_type,
                    dataset_flow.flow_id,
                )
                .await?;
            }

            // TODO: retry logic in case of failed outcome
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<FlowConfigurationEventModified<DatasetFlowKey>> for FlowServiceInMemory {
    async fn handle(
        &self,
        event: &FlowConfigurationEventModified<DatasetFlowKey>,
    ) -> Result<(), InternalError> {
        if event.paused {
            let mut state = self.state.lock().unwrap();
            state.active_schedules.remove(
                BorrowedDatasetFlowKey::new(&event.flow_key.dataset_id, event.flow_key.flow_type)
                    .as_trait(),
            );
        } else {
            match &event.rule {
                FlowConfigurationRule::Schedule(schedule) => {
                    self.enqueue_auto_polling_dataset_flow(
                        &event.flow_key.dataset_id,
                        event.flow_key.flow_type,
                        &schedule,
                    )
                    .await?;

                    let mut state = self.state.lock().unwrap();
                    state.active_schedules.insert(
                        OwnedDatasetFlowKey::new(
                            event.flow_key.dataset_id.clone(),
                            event.flow_key.flow_type,
                        ),
                        schedule.clone(),
                    );
                }
                FlowConfigurationRule::StartCondition(start_condition) => {
                    let mut state = self.state.lock().unwrap();
                    state.active_start_conditions.insert(
                        OwnedDatasetFlowKey::new(
                            event.flow_key.dataset_id.clone(),
                            event.flow_key.flow_type,
                        ),
                        start_condition.clone(),
                    );
                }
            }
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<DatasetEventDeleted> for FlowServiceInMemory {
    async fn handle(&self, event: &DatasetEventDeleted) -> Result<(), InternalError> {
        let mut state = self.state.lock().unwrap();

        // TODO: other dataset flow types
        state.active_schedules.remove(
            BorrowedDatasetFlowKey::new(&event.dataset_id, DatasetFlowType::Update).as_trait(),
        );

        // TODO: other dataset flow types
        if let Some(update_id) = state.pending_dataset_flows_by_dataset.remove(
            BorrowedDatasetFlowKey::new(&event.dataset_id, DatasetFlowType::Update).as_trait(),
        ) {
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
