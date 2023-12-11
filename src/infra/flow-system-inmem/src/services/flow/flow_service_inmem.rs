// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////

use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use dill::*;
use event_bus::AsyncEventHandler;
use futures::TryStreamExt;
use kamu_core::events::DatasetEventDeleted;
use kamu_core::{InternalError, SystemTimeSource};
use kamu_flow_system::*;
use kamu_task_system::*;
use opendatafabric::{AccountID, AccountName, DatasetID};
use tokio_stream::StreamExt;

use super::active_configs_state::ActiveConfigsState;
use super::flow_time_wheel::FlowTimeWheel;
use super::pending_flows_state::PendingFlowsState;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowServiceInMemory {
    state: Arc<Mutex<State>>,
    flow_event_store: Arc<dyn FlowEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    task_scheduler: Arc<dyn TaskScheduler>,
    flow_configuration_service: Arc<dyn FlowConfigurationService>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    active_configs: ActiveConfigsState,
    pending_flows: PendingFlowsState,
    time_wheel: FlowTimeWheel,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowService)]
#[interface(dyn AsyncEventHandler<TaskEventFinished>)]
#[interface(dyn AsyncEventHandler<DatasetEventDeleted>)]
#[interface(dyn AsyncEventHandler<FlowConfigurationEventModified>)]
#[scope(Singleton)]
impl FlowServiceInMemory {
    pub fn new(
        flow_event_store: Arc<dyn FlowEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        task_scheduler: Arc<dyn TaskScheduler>,
        flow_configuration_service: Arc<dyn FlowConfigurationService>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            flow_event_store,
            time_source,
            task_scheduler,
            flow_configuration_service,
            dependency_graph_service,
        }
    }

    async fn run_current_timeslot(&self) {
        let planned_flows: Vec<_> = {
            let mut state = self.state.lock().unwrap();
            state.time_wheel.take_nearest_planned_flows()
        };

        let planned_task_futures: Vec<_> = planned_flows
            .iter()
            .map(async move |flow_id| {
                let mut flow = Flow::load(*flow_id, self.flow_event_store.as_ref())
                    .await
                    .int_err()?;

                self.schedule_flow_task(&mut flow).await?;
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

    async fn initialize_auto_polling_flows_from_configurations(&self) -> Result<(), InternalError> {
        for dataset_flow_type in DatasetFlowType::all() {
            self.initialize_enabled_dataset_configurations(*dataset_flow_type)
                .await?;
        }

        for system_flow_type in SystemFlowType::all() {
            self.initialize_system_flow_configuration(*system_flow_type)
                .await?;
        }

        Ok(())
    }

    async fn initialize_enabled_dataset_configurations(
        &self,
        flow_type: DatasetFlowType,
    ) -> Result<(), InternalError> {
        let enabled_configurations: Vec<_> = self
            .flow_configuration_service
            .list_enabled_dataset_flow_configurations(flow_type)
            .try_collect()
            .await
            .int_err()?;

        for enabled_config in enabled_configurations {
            match &enabled_config.flow_key {
                FlowKey::Dataset(_) => {
                    self.activate_flow_configuration(enabled_config.flow_key, enabled_config.rule)
                        .await?;
                }
                FlowKey::System(_) => unreachable!(),
            }
        }

        Ok(())
    }

    async fn initialize_system_flow_configuration(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<(), InternalError> {
        let maybe_system_flow_config = self
            .flow_configuration_service
            .find_configuration(FlowKey::System(FlowKeySystem::new(flow_type)))
            .await
            .map_err(|e| match e {
                FindFlowConfigurationError::Internal(e) => e,
            })?;

        if let Some(system_flow_config) = maybe_system_flow_config {
            let flow_key = FlowKey::System(FlowKeySystem::new(flow_type));
            self.activate_flow_configuration(flow_key, system_flow_config.rule)
                .await?;
        }

        Ok(())
    }

    async fn activate_flow_configuration(
        &self,
        flow_key: FlowKey,
        rule: FlowConfigurationRule,
    ) -> Result<(), InternalError> {
        match &flow_key {
            FlowKey::Dataset(dataset_flow_key) => {
                if let FlowConfigurationRule::Schedule(schedule) = &rule {
                    self.enqueue_auto_polling_flow(&flow_key, schedule).await?;
                }

                let mut state = self.state.lock().unwrap();
                state.active_configs.add_dataset_flow_config(
                    &dataset_flow_key.dataset_id,
                    dataset_flow_key.flow_type,
                    rule,
                );
            }
            FlowKey::System(system_flow_key) => {
                if let FlowConfigurationRule::Schedule(schedule) = &rule {
                    self.enqueue_auto_polling_flow(&flow_key, schedule).await?;

                    let mut state = self.state.lock().unwrap();
                    state
                        .active_configs
                        .add_system_flow_config(system_flow_key.flow_type, schedule.clone());
                } else {
                    unimplemented!("Doubt will ever need to schedule system flows via conditions")
                }
            }
        }

        Ok(())
    }

    async fn try_enqueue_auto_polling_flow_if_enabled(
        &self,
        flow_key: &FlowKey,
    ) -> Result<(), InternalError> {
        let maybe_active_schedule = self
            .state
            .lock()
            .unwrap()
            .active_configs
            .try_get_flow_schedule(flow_key);

        if let Some(active_schedule) = maybe_active_schedule {
            self.enqueue_auto_polling_flow(flow_key, &active_schedule)
                .await?;
        }

        Ok(())
    }

    async fn enqueue_auto_polling_flow(
        &self,
        flow_key: &FlowKey,
        schedule: &Schedule,
    ) -> Result<FlowState, InternalError> {
        let trigger = FlowTrigger::AutoPolling(FlowTriggerAutoPolling {});

        match self.find_pending_flow(flow_key) {
            // If flow is already pending, simply merge triggers
            Some(flow_id) => self.merge_secondary_flow_trigger(flow_id, trigger).await,

            // Otherwise, initiate a new flow, and enqueue it in the time wheel
            None => {
                let mut flow = self.make_new_flow(flow_key.clone(), trigger).await?;

                let next_activation_time = schedule.next_activation_time(self.time_source.now());
                self.enqueue_flow(flow.flow_id, next_activation_time)?;

                flow.activate_at_time(self.time_source.now(), next_activation_time)
                    .int_err()?;

                flow.save(self.flow_event_store.as_ref()).await.int_err()?;

                Ok(flow.into())
            }
        }
    }

    async fn enqueue_dependent_dataset_flows(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
        flow_id: FlowID,
    ) -> Result<(), InternalError> {
        // Note: this is applicable to dataset updates only
        assert_eq!(flow_type, DatasetFlowType::Update);

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
                .active_configs
                .try_get_dataset_start_condition(&dependent_dataset_id, flow_type);

            if let Some(start_condition) = maybe_dependent_start_condition {
                let trigger = FlowTrigger::InputDatasetFlow(FlowTriggerInputDatasetFlow {
                    input_dataset_id: dataset_id.clone(),
                    input_flow_type: flow_type,
                    input_flow_id: flow_id,
                });

                let flow_key =
                    FlowKey::Dataset(FlowKeyDataset::new(dependent_dataset_id.clone(), flow_type));
                match self.find_pending_flow(&flow_key) {
                    // If flow is already pending for this dataset, simply merge triggers
                    Some(dependent_flow_id) => {
                        self.merge_secondary_flow_trigger(dependent_flow_id, trigger)
                            .await?;
                    }

                    // Otherwise, initiate a new update accordingly to start condition rules
                    None => {
                        let mut dependent_dataset_flow =
                            self.make_new_flow(flow_key, trigger).await?;

                        if start_condition.minimal_data_batch.is_some() {
                            unimplemented!("Data batching not supported yet in scheduler")
                        }

                        if let Some(throttling_period) = start_condition.throttling_period {
                            // TODO: throttle not from NOW,
                            //  but from last flow of the dependent daataset
                            let now = self.time_source.now();
                            self.enqueue_flow(
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
                            .save(self.flow_event_store.as_ref())
                            .await
                            .int_err()?;
                    }
                }
            }
        }

        Ok(())
    }

    fn find_pending_flow(&self, flow_key: &FlowKey) -> Option<FlowID> {
        let state = self.state.lock().unwrap();
        state.pending_flows.try_get_pending_flow(flow_key)
    }

    async fn make_new_flow(
        &self,
        flow_key: FlowKey,
        trigger: FlowTrigger,
    ) -> Result<Flow, InternalError> {
        let flow = Flow::new(
            self.time_source.now(),
            self.flow_event_store.new_flow_id(),
            flow_key,
            trigger,
        );

        let mut state = self.state.lock().unwrap();
        state
            .pending_flows
            .add_pending_flow(flow.flow_key.clone(), flow.flow_id);

        Ok(flow)
    }

    async fn merge_secondary_flow_trigger(
        &self,
        flow_id: FlowID,
        trigger: FlowTrigger,
    ) -> Result<FlowState, InternalError> {
        let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
            .await
            .int_err()?;
        flow.add_trigger(self.time_source.now(), trigger)
            .int_err()?;
        flow.save(self.flow_event_store.as_ref()).await.int_err()?;
        Ok(flow.into())
    }

    fn enqueue_flow(
        &self,
        flow_id: FlowID,
        activation_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        self.state
            .lock()
            .unwrap()
            .time_wheel
            .activate_at(activation_time, flow_id)?;
        Ok(())
    }

    async fn schedule_flow_task(&self, flow: &mut Flow) -> Result<(), InternalError> {
        let logical_plan = match &flow.flow_key {
            FlowKey::Dataset(flow_key) => match flow_key.flow_type {
                DatasetFlowType::Update => LogicalPlan::UpdateDataset(UpdateDataset {
                    dataset_id: flow_key.dataset_id.clone(),
                }),
                DatasetFlowType::Compacting => unimplemented!(),
            },
            FlowKey::System(flow_key) => {
                match flow_key.flow_type {
                    // TODO: replace on correct logical plan
                    SystemFlowType::GC => LogicalPlan::Probe(Probe {
                        dataset_id: None,
                        busy_time: Some(std::time::Duration::from_secs(20)),
                        end_with_outcome: Some(TaskOutcome::Success),
                    }),
                }
            }
        };

        let task = self
            .task_scheduler
            .create_task(logical_plan)
            .await
            .int_err()?;

        flow.on_task_scheduled(self.time_source.now(), task.task_id)
            .int_err()?;
        flow.save(self.flow_event_store.as_ref()).await.int_err()?;

        let mut state = self.state.lock().unwrap();
        state
            .pending_flows
            .track_flow_task(flow.flow_id, task.task_id);

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowService for FlowServiceInMemory {
    /// Runs the update main loop
    async fn run(&self) -> Result<(), InternalError> {
        // Initial scheduling
        self.initialize_auto_polling_flows_from_configurations()
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

    /// Creates a new manual flow request
    async fn request_manual_flow(
        &self,
        flow_key: FlowKey,
        initiator_account_id: AccountID,
        initiator_account_name: AccountName,
    ) -> Result<FlowState, RequestFlowError> {
        let trigger = FlowTrigger::Manual(FlowTriggerManual {
            initiator_account_id,
            initiator_account_name,
        });

        match self.find_pending_flow(&flow_key) {
            // If flow is already pending, simply merge triggers
            Some(flow_id) => self
                .merge_secondary_flow_trigger(flow_id, trigger)
                .await
                .map_err(|e| RequestFlowError::Internal(e)),

            // Otherwise, initiate a new flow and schedule immediate task
            None => {
                let mut flow = self.make_new_flow(flow_key, trigger).await?;
                self.schedule_flow_task(&mut flow).await?;
                flow.save(self.flow_event_store.as_ref()).await.int_err()?;
                Ok(flow.into())
            }
        }
    }

    /// Returns states of flows of certian type associated with a given dataset
    /// ordered by creation time from newest to oldest
    fn list_flows_by_dataset_of_type(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<FlowStateStream, ListFlowsByDatasetError> {
        let dataset_id = dataset_id.clone();

        Ok(Box::pin(async_stream::try_stream! {
            let relevant_flows: Vec<_> = self
                .flow_event_store
                .get_specific_flows_by_dataset(&dataset_id, flow_type)
                .try_collect()
                .await?;

            for flow_id in relevant_flows.into_iter() {
                let flow = Flow::load(flow_id, self.flow_event_store.as_ref()).await.int_err()?;

                yield flow.into();
            }
        }))
    }

    /// Returns states of system flows of certian type
    /// ordered by creation time from newest to oldest
    fn list_system_flows_of_type(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<FlowStateStream, ListSystemFlowsError> {
        Ok(Box::pin(async_stream::try_stream! {
            let relevant_flows: Vec<_> = self
                .flow_event_store
                .get_specific_system_flows(flow_type)
                .try_collect()
                .await?;

            for flow_id in relevant_flows.into_iter() {
                let flow = Flow::load(flow_id, self.flow_event_store.as_ref()).await.int_err()?;

                yield flow.into();
            }
        }))
    }

    /// Returns states of flows of any type associated with a given dataset
    /// ordered by creation time from newest to oldest
    fn list_all_flows_by_dataset(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<FlowStateStream, ListFlowsByDatasetError> {
        let dataset_id = dataset_id.clone();

        Ok(Box::pin(async_stream::try_stream! {
            let relevant_flows: Vec<_> = self
                .flow_event_store
                .get_all_flows_by_dataset(&dataset_id)
                .try_collect()
                .await?;

            for flow_id in relevant_flows.into_iter() {
                let flow = Flow::load(flow_id, self.flow_event_store.as_ref()).await.int_err()?;

                yield flow.into();
            }
        }))
    }

    /// Returns states of system flows of any type
    /// ordered by creation time from newest to oldest
    fn list_all_system_flows(&self) -> Result<FlowStateStream, ListSystemFlowsError> {
        Ok(Box::pin(async_stream::try_stream! {
            let all_flows: Vec<_> = self
                .flow_event_store
                .get_all_system_flows()
                .try_collect()
                .await?;

            for flow_id in all_flows.into_iter() {
                let flow = Flow::load(flow_id, self.flow_event_store.as_ref()).await.int_err()?;

                yield flow.into();
            }
        }))
    }

    /// Returns state of the latest flow of certain type created for the given
    /// dataset
    async fn get_last_flow_by_dataset_of_type(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<FlowState>, GetLastDatasetFlowError> {
        let res = match self
            .flow_event_store
            .get_last_specific_dataset_flow(dataset_id, flow_type)
        {
            Some(flow_id) => Some(self.get_flow(flow_id).await.int_err()?),
            None => None,
        };
        Ok(res)
    }

    /// Returns state of the latest system flow of certain type
    async fn get_last_system_flow_of_type(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<Option<FlowState>, GetLastSystemtFlowError> {
        let res = match self
            .flow_event_store
            .get_last_specific_system_flow(flow_type)
        {
            Some(flow_id) => Some(self.get_flow(flow_id).await.int_err()?),
            None => None,
        };
        Ok(res)
    }

    /// Returns current state of a given flow
    async fn get_flow(&self, flow_id: FlowID) -> Result<FlowState, GetFlowError> {
        let flow = Flow::load(flow_id, self.flow_event_store.as_ref()).await?;
        Ok(flow.into())
    }

    /// Attempts to cancel the given flow
    async fn cancel_flow(
        &self,
        flow_id: FlowID,
        by_account_id: AccountID,
        by_account_name: AccountName,
    ) -> Result<FlowState, CancelFlowError> {
        let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref()).await?;

        if flow.can_cancel() {
            flow.cancel(self.time_source.now(), by_account_id, by_account_name)
                .int_err()?;
            flow.save(self.flow_event_store.as_ref()).await.int_err()?;

            let mut state = self.state.lock().unwrap();
            if state.time_wheel.is_flow_activation_planned(flow_id) {
                state
                    .time_wheel
                    .cancel_flow_activation(flow_id)
                    .map_err(|e| CancelFlowError::Internal(e.int_err()))?;
            }

            state.pending_flows.drop_pending_flow(&flow.flow_key);
        }

        Ok(flow.into())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<TaskEventFinished> for FlowServiceInMemory {
    async fn handle(&self, event: &TaskEventFinished) -> Result<(), InternalError> {
        // Is this a task associated with flows?
        let maybe_flow_id = self
            .state
            .lock()
            .unwrap()
            .pending_flows
            .try_get_flow_id_by_task(event.task_id);

        if let Some(flow_id) = maybe_flow_id {
            let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
                .await
                .int_err()?;
            flow.on_task_finished(self.time_source.now(), event.task_id, event.outcome)
                .int_err()?;
            flow.save(self.flow_event_store.as_ref()).await.int_err()?;

            // In case of success:
            //  - enqueue updates of dependent datasets
            if event.outcome == TaskOutcome::Success {
                if let FlowKey::Dataset(flow_key) = &flow.flow_key
                    && flow_key.flow_type == DatasetFlowType::Update
                {
                    self.enqueue_dependent_dataset_flows(
                        &flow_key.dataset_id,
                        flow_key.flow_type,
                        flow.flow_id,
                    )
                    .await?;
                }
            }

            {
                let mut state = self.state.lock().unwrap();
                state.pending_flows.untrack_flow_by_task(event.task_id);
                state.pending_flows.drop_pending_flow(&flow.flow_key);
            }

            // In case of success:
            //  - enqueue next auto-polling flow cycle
            if event.outcome == TaskOutcome::Success {
                self.try_enqueue_auto_polling_flow_if_enabled(&flow.flow_key)
                    .await?;
            }

            // TODO: retry logic in case of failed outcome
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<FlowConfigurationEventModified> for FlowServiceInMemory {
    async fn handle(&self, event: &FlowConfigurationEventModified) -> Result<(), InternalError> {
        if event.paused {
            let mut state = self.state.lock().unwrap();
            state.active_configs.drop_flow_config(&event.flow_key);
            // TODO: should we unqueue pending flows / abort scheduled tasks?
        } else {
            self.activate_flow_configuration(event.flow_key.clone(), event.rule.clone())
                .await?
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<DatasetEventDeleted> for FlowServiceInMemory {
    async fn handle(&self, event: &DatasetEventDeleted) -> Result<(), InternalError> {
        let mut state = self.state.lock().unwrap();
        state.active_configs.drop_dataset_configs(&event.dataset_id);

        for flow_type in DatasetFlowType::all() {
            if let Some(flow_id) = state
                .pending_flows
                .drop_dataset_pending_flow(&event.dataset_id, *flow_type)
            {
                state.time_wheel.cancel_flow_activation(flow_id).int_err()?;
            }
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
