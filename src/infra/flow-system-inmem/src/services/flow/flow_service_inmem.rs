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

use chrono::{DateTime, DurationRound, Utc};
use dill::*;
use event_bus::{AsyncEventHandler, EventBus};
use futures::TryStreamExt;
use kamu_core::events::DatasetEventDeleted;
use kamu_core::{DependencyGraphService, InternalError, SystemTimeSource};
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
    run_config: Arc<FlowServiceRunConfig>,
    event_bus: Arc<EventBus>,
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
    running: bool,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowService)]
#[interface(dyn FlowServiceTestDriver)]
#[interface(dyn AsyncEventHandler<TaskEventRunning>)]
#[interface(dyn AsyncEventHandler<TaskEventFinished>)]
#[interface(dyn AsyncEventHandler<DatasetEventDeleted>)]
#[interface(dyn AsyncEventHandler<FlowConfigurationEventModified>)]
#[scope(Singleton)]
impl FlowServiceInMemory {
    pub fn new(
        run_config: Arc<FlowServiceRunConfig>,
        event_bus: Arc<EventBus>,
        flow_event_store: Arc<dyn FlowEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        task_scheduler: Arc<dyn TaskScheduler>,
        flow_configuration_service: Arc<dyn FlowConfigurationService>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            run_config,
            event_bus,
            flow_event_store,
            time_source,
            task_scheduler,
            flow_configuration_service,
            dependency_graph_service,
        }
    }

    fn round_time(&self, time: DateTime<Utc>) -> Result<DateTime<Utc>, InternalError> {
        let rounded_time = time
            .duration_round(self.run_config.awaiting_step)
            .int_err()?;
        Ok(rounded_time)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run_current_timeslot(&self) -> Result<(), InternalError> {
        let planned_flow_ids: Vec<_> = {
            let mut state = self.state.lock().unwrap();
            state.time_wheel.take_nearest_planned_flows()
        };

        let mut planned_task_futures = Vec::new();
        for planned_flow_id in planned_flow_ids {
            planned_task_futures.push(async move {
                let mut flow = Flow::load(planned_flow_id, self.flow_event_store.as_ref())
                    .await
                    .int_err()?;
                self.schedule_flow_task(&mut flow).await?;
                Ok(())
            });
        }

        let results = futures::future::join_all(planned_task_futures).await;
        results
            .into_iter()
            .filter(Result::is_err)
            .map(|e| e.err().unwrap())
            .for_each(|e: InternalError| {
                tracing::error!(error=?e, "Scheduling flow failed");
            });

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn initialize_auto_polling_flows_from_configurations(
        &self,
        start_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let enabled_configurations: Vec<_> = self
            .flow_configuration_service
            .list_enabled_configurations()
            .try_collect()
            .await
            .int_err()?;

        for enabled_config in enabled_configurations {
            self.activate_flow_configuration(
                start_time,
                enabled_config.flow_key,
                enabled_config.rule,
            )
            .await?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(?flow_key, ?rule))]
    async fn activate_flow_configuration(
        &self,
        start_time: DateTime<Utc>,
        flow_key: FlowKey,
        rule: FlowConfigurationRule,
    ) -> Result<(), InternalError> {
        match &flow_key {
            FlowKey::Dataset(dataset_flow_key) => {
                match &rule {
                    FlowConfigurationRule::Schedule(schedule) => {
                        self.enqueue_scheduled_auto_polling_flow(start_time, &flow_key, schedule)
                            .await?;
                    }
                    FlowConfigurationRule::BatchingRule(_) => {
                        self.enqueue_auto_polling_flow_unconditionally(start_time, &flow_key)
                            .await?;
                    }
                }

                let mut state = self.state.lock().unwrap();
                state
                    .active_configs
                    .add_dataset_flow_config(dataset_flow_key, rule);
            }
            FlowKey::System(system_flow_key) => {
                if let FlowConfigurationRule::Schedule(schedule) = &rule {
                    self.enqueue_scheduled_auto_polling_flow(start_time, &flow_key, schedule)
                        .await?;

                    let mut state = self.state.lock().unwrap();
                    state
                        .active_configs
                        .add_system_flow_config(system_flow_key.flow_type, schedule.clone());
                } else {
                    unimplemented!(
                        "Doubt will ever need to schedule system flows via batching rules"
                    )
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(?flow_key))]
    async fn try_enqueue_scheduled_auto_polling_flow_if_enabled(
        &self,
        start_time: DateTime<Utc>,
        flow_key: &FlowKey,
    ) -> Result<(), InternalError> {
        let maybe_active_schedule = self
            .state
            .lock()
            .unwrap()
            .active_configs
            .try_get_flow_schedule(flow_key);

        if let Some(active_schedule) = maybe_active_schedule {
            self.enqueue_scheduled_auto_polling_flow(start_time, flow_key, &active_schedule)
                .await?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(?flow_key, ?schedule))]
    async fn enqueue_scheduled_auto_polling_flow(
        &self,
        start_time: DateTime<Utc>,
        flow_key: &FlowKey,
        schedule: &Schedule,
    ) -> Result<FlowState, InternalError> {
        self.trigger_flow_common(
            start_time,
            flow_key,
            FlowTrigger::AutoPolling(FlowTriggerAutoPolling {}),
            None,
            |flow_run_stats: &FlowRunStats| {
                schedule.next_activation_time(start_time, flow_run_stats.last_success_time)
            },
        )
        .await
    }

    #[tracing::instrument(level = "trace", skip_all, fields(?flow_key))]
    async fn enqueue_auto_polling_flow_unconditionally(
        &self,
        start_time: DateTime<Utc>,
        flow_key: &FlowKey,
    ) -> Result<FlowState, InternalError> {
        // Very similar to manual trigger, but automatic reasons
        self.trigger_flow_common(
            start_time,
            flow_key,
            FlowTrigger::AutoPolling(FlowTriggerAutoPolling {}),
            None,
            |_: &FlowRunStats| start_time,
        )
        .await
    }

    async fn trigger_flow_common(
        &self,
        trigger_time: DateTime<Utc>,
        flow_key: &FlowKey,
        trigger: FlowTrigger,
        maybe_batching_rule: Option<&BatchingRule>,
        new_activation_time_fn: impl FnOnce(&FlowRunStats) -> DateTime<Utc>,
    ) -> Result<FlowState, InternalError> {
        // Query previous runs stats to determine activation time
        let flow_run_stats = self.flow_run_stats(flow_key).await?;

        // Flows may not be attempted more frequent than mandatory throttling period.
        // If flow has never run before, let it go without restriction
        let throttling_boundary_time = flow_run_stats.last_attempt_time.map_or(trigger_time, |t| {
            t + self.run_config.mandatory_throttling_period
        });

        // Is a pending flow present for this config?
        match self.find_pending_flow(flow_key) {
            // Already pending flow
            Some(flow_id) => {
                // Load, merge triggers, update activation time
                let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
                    .await
                    .int_err()?;
                flow.add_trigger(trigger_time, trigger).int_err()?;

                // Evaluate batching rule, if defined, and still waiting
                let batching_gate_pass = if flow.status() == FlowStatus::Waiting
                    && let Some(batching_rule) = maybe_batching_rule
                {
                    // Stop if batching condition not satisfied
                    self.evaluate_flow_batching_rule(trigger_time, &mut flow, batching_rule)?
                } else {
                    true
                };

                // Evaluate throttling condition, if batching condition passed
                if batching_gate_pass {
                    // Is new time earlier than previously planned?
                    if throttling_boundary_time
                        < flow
                            .timing
                            .activate_at
                            .expect("Flow expected to be queued by now")
                    {
                        // If so, enqueue the flow earlier
                        self.enqueue_flow(flow.flow_id, throttling_boundary_time)?;
                        flow.activate_at_time(self.time_source.now(), throttling_boundary_time)
                            .int_err()?;

                        // Indicate throttling activity
                        if throttling_boundary_time > trigger_time {
                            flow.define_start_condition(
                                self.time_source.now(),
                                FlowStartCondition::Throttling(FlowStartConditionThrottling {
                                    interval: self.run_config.mandatory_throttling_period,
                                }),
                            )
                            .int_err()?;
                        }
                    }
                }

                flow.save(self.flow_event_store.as_ref()).await.int_err()?;
                Ok(flow.into())
            }

            // Otherwise, initiate a new flow, and enqueue it in the time wheel
            None => {
                // Initiate new flow
                let mut flow = self.make_new_flow(flow_key.clone(), trigger).await?;

                // Evaluate batching rule, if defined
                let batching_gate_pass = if let Some(batching_rule) = maybe_batching_rule {
                    // Stop if batching condition not satisfied
                    self.evaluate_flow_batching_rule(trigger_time, &mut flow, batching_rule)?
                } else {
                    true
                };

                // Evaluate throttling condition, if batching condition passed
                if batching_gate_pass {
                    // Next activation time dependds on:
                    //  - last success time, if ever launched
                    //  - schedule, if defined
                    // but in any case, may not be less than throttling boundary
                    let naive_next_activation_time = new_activation_time_fn(&flow_run_stats);
                    let next_activation_time =
                        std::cmp::max(throttling_boundary_time, naive_next_activation_time);
                    self.enqueue_flow(flow.flow_id, next_activation_time)?;
                    flow.activate_at_time(self.time_source.now(), next_activation_time)
                        .int_err()?;

                    // Indicate throttling activity
                    if throttling_boundary_time > naive_next_activation_time {
                        flow.define_start_condition(
                            self.time_source.now(),
                            FlowStartCondition::Throttling(FlowStartConditionThrottling {
                                interval: self.run_config.mandatory_throttling_period,
                            }),
                        )
                        .int_err()?;
                    }
                }

                flow.save(self.flow_event_store.as_ref()).await.int_err()?;
                Ok(flow.into())
            }
        }
    }

    fn evaluate_flow_batching_rule(
        &self,
        evaluation_time: DateTime<Utc>,
        flow: &mut Flow,
        batching_rule: &BatchingRule,
    ) -> Result<bool, InternalError> {
        // Run evaluation
        let result = batching_rule.evaluate(
            evaluation_time - flow.timing.created_at, // how much time flow waited
            &flow.triggers,
        );

        // Update batching condition data
        flow.define_start_condition(
            evaluation_time,
            FlowStartCondition::Batching(FlowStartConditionBatching {
                active_batching_rule: *batching_rule,
                awaited_by_now: result.awaited_by_now,
                accumulated_records_count: result.accumulated_records_count,
                watermark_modified: result.watermark_modified,
            }),
        )
        .int_err()?;

        // Stop if batching condition not satisfied
        Ok(result.satisfied)
    }

    #[tracing::instrument(level = "trace", skip_all, fields(%dataset_id, ?flow_type, %flow_id))]
    async fn enqueue_dependent_dataset_flows(
        &self,
        start_time: DateTime<Utc>,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
        flow_id: FlowID,
        flow_result: &FlowResult,
    ) -> Result<(), InternalError> {
        // Note: this is applicable to dataset updates only
        assert!(flow_type.is_dataset_update());

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
            let maybe_batching_rule = self
                .state
                .lock()
                .unwrap()
                .active_configs
                .try_get_dataset_batching_rule(&dependent_dataset_id, flow_type);

            if let Some(batching_rule) = maybe_batching_rule {
                let dependent_flow_key =
                    FlowKeyDataset::new(dependent_dataset_id.clone(), flow_type).into();

                let trigger = FlowTrigger::InputDatasetFlow(FlowTriggerInputDatasetFlow {
                    dataset_id: dataset_id.clone(),
                    flow_type,
                    flow_id,
                    flow_result: *flow_result,
                });

                self.trigger_flow_common(
                    start_time,
                    &dependent_flow_key,
                    trigger,
                    Some(&batching_rule),
                    |_| start_time,
                )
                .await
                .int_err()?;
            }
        }

        Ok(())
    }

    fn find_pending_flow(&self, flow_key: &FlowKey) -> Option<FlowID> {
        let state = self.state.lock().unwrap();
        state.pending_flows.try_get_pending_flow(flow_key)
    }

    #[tracing::instrument(level = "trace", skip_all, fields(?flow_key, ?trigger))]
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

    async fn flow_run_stats(&self, flow_key: &FlowKey) -> Result<FlowRunStats, InternalError> {
        match flow_key {
            FlowKey::Dataset(fk_dataset) => {
                self.flow_event_store
                    .get_dataset_flow_run_stats(&fk_dataset.dataset_id, fk_dataset.flow_type)
                    .await
            }
            FlowKey::System(fk_system) => {
                self.flow_event_store
                    .get_system_flow_run_stats(fk_system.flow_type)
                    .await
            }
        }
    }

    #[tracing::instrument(level = "trace", skip_all, fields(%flow_id, %activation_time))]
    fn enqueue_flow(
        &self,
        flow_id: FlowID,
        activation_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        self.state
            .lock()
            .unwrap()
            .time_wheel
            .activate_at(activation_time, flow_id);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(flow_id = %flow.flow_id))]
    async fn schedule_flow_task(&self, flow: &mut Flow) -> Result<TaskID, InternalError> {
        let logical_plan = flow.make_task_logical_plan();

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

        Ok(task.task_id)
    }

    async fn abort_flow(&self, flow_id: FlowID) -> Result<(), InternalError> {
        // Mark flow as aborted
        let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
            .await
            .int_err()?;

        self.abort_flow_impl(&mut flow).await
    }

    async fn abort_flow_impl(&self, flow: &mut Flow) -> Result<(), InternalError> {
        // Abort flow itself
        flow.abort(self.time_source.now()).int_err()?;
        flow.save(self.flow_event_store.as_ref()).await.int_err()?;

        // Cancel associated tasks, but first drop task -> flow associations
        {
            let mut state = self.state.lock().unwrap();
            for task_id in &flow.task_ids {
                state.pending_flows.untrack_flow_by_task(*task_id);
            }
        }
        for task_id in &flow.task_ids {
            self.task_scheduler.cancel_task(*task_id).await.int_err()?;
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowService for FlowServiceInMemory {
    /// Runs the update main loop
    #[tracing::instrument(level = "info", skip_all)]
    async fn run(&self, planned_start_time: DateTime<Utc>) -> Result<(), InternalError> {
        // Mark running started
        self.state.lock().unwrap().running = true;

        // Initial scheduling
        let start_time = self.round_time(planned_start_time)?;
        self.initialize_auto_polling_flows_from_configurations(start_time)
            .await?;

        // Publish progress event
        self.event_bus
            .dispatch_event(FlowServiceEvent::ConfigurationLoaded(
                FlowServiceEventConfigurationLoaded {
                    event_time: start_time,
                },
            ))
            .await
            .int_err()?;

        // Main scanning loop
        let main_loop_span = tracing::debug_span!("FlowService main loop");
        let _ = main_loop_span.enter();

        loop {
            let current_time = self.time_source.now();

            // Do we have a timeslot scheduled?
            let maybe_nearest_activation_time = {
                let state = self.state.lock().unwrap();
                state.time_wheel.nearest_activation_moment()
            };

            // Is it time to execute it yet?
            if let Some(nearest_activation_time) = maybe_nearest_activation_time
                && nearest_activation_time <= current_time
            {
                // Run scheduling for current time slot. Should not throw any errors
                self.run_current_timeslot().await.int_err()?;

                // Publish progress event
                self.event_bus
                    .dispatch_event(FlowServiceEvent::ExecutedTimeSlot(
                        FlowServiceEventExecutedTimeSlot {
                            event_time: nearest_activation_time,
                        },
                    ))
                    .await
                    .int_err()?;
            }

            self.time_source.sleep(self.run_config.awaiting_step).await;
        }
    }

    /// Triggers the specified flow manually, unless it's already waiting
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(?flow_key, %initiator_account_id, %initiator_account_name)
    )]
    async fn trigger_manual_flow(
        &self,
        trigger_time: DateTime<Utc>,
        flow_key: FlowKey,
        initiator_account_id: AccountID,
        initiator_account_name: AccountName,
    ) -> Result<FlowState, RequestFlowError> {
        let activation_time = self.round_time(trigger_time).int_err()?;

        self.trigger_flow_common(
            activation_time,
            &flow_key,
            FlowTrigger::Manual(FlowTriggerManual {
                initiator_account_id,
                initiator_account_name,
            }),
            None, // Ignore batching condition when triggering manually, even if it is defined
            |_| activation_time,
        )
        .await
        .map_err(RequestFlowError::Internal)
    }

    /// Returns states of flows associated with a given dataset
    /// ordered by creation time from newest to oldest
    /// Applies specified filters
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id, ?filters, ?pagination))]
    async fn list_all_flows_by_dataset(
        &self,
        dataset_id: &DatasetID,
        filters: DatasetFlowFilters,
        pagination: FlowPaginationOpts,
    ) -> Result<FlowStateListing, ListFlowsByDatasetError> {
        let total_count = self
            .flow_event_store
            .get_count_flows_by_dataset(dataset_id, &filters)
            .await
            .int_err()?;

        let dataset_id = dataset_id.clone();

        let matched_stream = Box::pin(async_stream::try_stream! {
            let relevant_flow_ids: Vec<_> = self
                .flow_event_store
                .get_all_flow_ids_by_dataset(&dataset_id, filters, pagination)
                .try_collect()
                .await
                .int_err()?;

            // TODO: implement batch loading
            for flow_id in relevant_flow_ids {
                let flow = Flow::load(flow_id, self.flow_event_store.as_ref()).await.int_err()?;
                yield flow.into();
            }
        });

        Ok(FlowStateListing {
            matched_stream,
            total_count,
        })
    }

    /// Returns states of system flows
    /// ordered by creation time from newest to oldest
    /// Applies specified filters
    #[tracing::instrument(level = "debug", skip_all, fields(?filters, ?pagination))]
    async fn list_all_system_flows(
        &self,
        filters: SystemFlowFilters,
        pagination: FlowPaginationOpts,
    ) -> Result<FlowStateListing, ListSystemFlowsError> {
        let total_count = self
            .flow_event_store
            .get_count_system_flows(&filters)
            .await
            .int_err()?;

        let matched_stream = Box::pin(async_stream::try_stream! {
            let relevant_flow_ids: Vec<_> = self
                .flow_event_store
                .get_all_system_flow_ids(filters, pagination)
                .try_collect()
                .await
                .int_err()?;

            // TODO: implement batch loading
            for flow_id in relevant_flow_ids {
                let flow = Flow::load(flow_id, self.flow_event_store.as_ref()).await.int_err()?;
                yield flow.into();
            }
        });

        Ok(FlowStateListing {
            matched_stream,
            total_count,
        })
    }

    /// Returns state of all flows, whether they are system-level or
    /// dataset-bound, ordered by creation time from newest to oldest
    #[tracing::instrument(level = "debug", skip_all, fields(?pagination))]
    async fn list_all_flows(
        &self,
        pagination: FlowPaginationOpts,
    ) -> Result<FlowStateListing, ListFlowsError> {
        let total_count = self
            .flow_event_store
            .get_count_all_flows()
            .await
            .int_err()?;

        let matched_stream = Box::pin(async_stream::try_stream! {
            let all_flows: Vec<_> = self
                .flow_event_store
                .get_all_flow_ids(pagination)
                .try_collect()
                .await
                .int_err()?;

            // TODO: implement batch loading
            for flow_id in all_flows {
                let flow = Flow::load(flow_id, self.flow_event_store.as_ref()).await.int_err()?;
                yield flow.into();
            }
        });

        Ok(FlowStateListing {
            matched_stream,
            total_count,
        })
    }

    /// Returns current state of a given flow
    #[tracing::instrument(level = "debug", skip_all, fields(%flow_id))]
    async fn get_flow(&self, flow_id: FlowID) -> Result<FlowState, GetFlowError> {
        let flow = Flow::load(flow_id, self.flow_event_store.as_ref()).await?;
        Ok(flow.into())
    }

    /// Attempts to cancel the tasks already scheduled for the given flow
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(%flow_id)
    )]
    async fn cancel_scheduled_tasks(
        &self,
        flow_id: FlowID,
    ) -> Result<FlowState, CancelScheduledTasksError> {
        let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref()).await?;

        // May not be called for Draft or Queued flows.
        // Cancel tasks for flows in Scheduled/Running state.
        // Ignore in Finished state
        match flow.status() {
            FlowStatus::Waiting | FlowStatus::Queued => {
                return Err(CancelScheduledTasksError::NotScheduled(
                    FlowNotScheduledError { flow_id },
                ))
            }
            FlowStatus::Scheduled | FlowStatus::Running => {
                // Abort current flow and it's scheduled tasks
                self.abort_flow_impl(&mut flow).await?;

                // Schedule next period
                let abort_time = self.round_time(self.time_source.now())?;
                self.try_enqueue_scheduled_auto_polling_flow_if_enabled(abort_time, &flow.flow_key)
                    .await?;
            }
            FlowStatus::Finished => { /* Skip, idempotence */ }
        }

        Ok(flow.into())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowServiceTestDriver for FlowServiceInMemory {
    /// Pretends running started
    fn mimic_running_started(&self) {
        let mut state = self.state.lock().unwrap();
        state.running = true;
    }

    /// Pretends it is time to schedule the given flow that was in Queued state
    async fn mimic_flow_scheduled(&self, flow_id: FlowID) -> Result<TaskID, InternalError> {
        {
            let mut state = self.state.lock().unwrap();
            state.time_wheel.cancel_flow_activation(flow_id).int_err()?;
        }

        let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
            .await
            .int_err()?;
        let task_id = self.schedule_flow_task(&mut flow).await.int_err()?;
        Ok(task_id)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<TaskEventRunning> for FlowServiceInMemory {
    #[tracing::instrument(level = "debug", skip_all, fields(?event))]
    async fn handle(&self, event: &TaskEventRunning) -> Result<(), InternalError> {
        // Is this a task associated with flows?
        let maybe_flow_id = {
            let state = self.state.lock().unwrap();
            if !state.running {
                // Abort if running hasn't started yet
                return Ok(());
            }
            state.pending_flows.try_get_flow_id_by_task(event.task_id)
        };

        let running_time = self.round_time(event.event_time)?;

        if let Some(flow_id) = maybe_flow_id {
            let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
                .await
                .int_err()?;
            flow.on_task_running(running_time, event.task_id)
                .int_err()?;
            flow.save(self.flow_event_store.as_ref()).await.int_err()?;
        }

        // Publish progress event
        self.event_bus
            .dispatch_event(FlowServiceEvent::FlowRunning(FlowServiceEventFlowRunning {
                event_time: event.event_time,
            }))
            .await
            .int_err()?;

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<TaskEventFinished> for FlowServiceInMemory {
    #[tracing::instrument(level = "debug", skip_all, fields(?event))]
    async fn handle(&self, event: &TaskEventFinished) -> Result<(), InternalError> {
        // Is this a task associated with flows?
        let maybe_flow_id = {
            let state = self.state.lock().unwrap();
            if !state.running {
                // Abort if running hasn't started yet
                return Ok(());
            }
            state.pending_flows.try_get_flow_id_by_task(event.task_id)
        };

        let finish_time = self.round_time(event.event_time)?;

        if let Some(flow_id) = maybe_flow_id {
            let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
                .await
                .int_err()?;
            flow.on_task_finished(finish_time, event.task_id, event.outcome.clone())
                .int_err()?;
            flow.save(self.flow_event_store.as_ref()).await.int_err()?;

            // In case of success:
            //  - enqueue updates of dependent datasets
            if let Some(flow_result) = flow.try_result_as_ref()
                && let FlowKey::Dataset(flow_key) = &flow.flow_key
                && flow_key.flow_type.is_dataset_update()
            {
                self.enqueue_dependent_dataset_flows(
                    finish_time,
                    &flow_key.dataset_id,
                    DatasetFlowType::ExecuteTransform,
                    flow.flow_id,
                    flow_result,
                )
                .await?;
            }

            {
                let mut state = self.state.lock().unwrap();
                state.pending_flows.untrack_flow_by_task(event.task_id);
                state.pending_flows.drop_pending_flow(&flow.flow_key);
            }

            // In case of success:
            //  - enqueue next auto-polling flow cycle
            if event.outcome.is_success() {
                self.try_enqueue_scheduled_auto_polling_flow_if_enabled(
                    finish_time,
                    &flow.flow_key,
                )
                .await?;
            }

            // Publish progress event
            self.event_bus
                .dispatch_event(FlowServiceEvent::FlowFinished(
                    FlowServiceEventFlowFinished {
                        event_time: finish_time,
                    },
                ))
                .await
                .int_err()?;

            // TODO: retry logic in case of failed outcome
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<FlowConfigurationEventModified> for FlowServiceInMemory {
    #[tracing::instrument(level = "debug", skip_all, fields(?event))]
    async fn handle(&self, event: &FlowConfigurationEventModified) -> Result<(), InternalError> {
        if event.paused {
            let maybe_pending_flow_id = {
                let mut state = self.state.lock().unwrap();
                if !state.running {
                    // Abort if running hasn't started yet
                    return Ok(());
                };

                state.active_configs.drop_flow_config(&event.flow_key);

                let maybe_pending_flow_id = state.pending_flows.drop_pending_flow(&event.flow_key);
                if let Some(flow_id) = &maybe_pending_flow_id {
                    state
                        .time_wheel
                        .cancel_flow_activation(*flow_id)
                        .int_err()?;
                }
                maybe_pending_flow_id
            };

            if let Some(flow_id) = maybe_pending_flow_id {
                self.abort_flow(flow_id).await?;
            }
        } else {
            {
                let state = self.state.lock().unwrap();
                if !state.running {
                    // Abort if running hasn't started yet
                    return Ok(());
                };
            }

            let activation_time = self.round_time(event.event_time)?;
            self.activate_flow_configuration(
                activation_time,
                event.flow_key.clone(),
                event.rule.clone(),
            )
            .await?;
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<DatasetEventDeleted> for FlowServiceInMemory {
    #[tracing::instrument(level = "debug", skip_all, fields(?event))]
    async fn handle(&self, event: &DatasetEventDeleted) -> Result<(), InternalError> {
        let flow_ids_2_abort = {
            let mut state = self.state.lock().unwrap();
            if !state.running {
                // Abort if running hasn't started yet
                return Ok(());
            };

            state.active_configs.drop_dataset_configs(&event.dataset_id);

            // For every possible dataset flow:
            //  - drop it from pending state
            //  - drop queued activations
            //  - collect ID of aborted flow
            let mut flow_ids_2_abort: Vec<_> = Vec::new();
            for flow_type in DatasetFlowType::all() {
                if let Some(flow_id) = state
                    .pending_flows
                    .drop_dataset_pending_flow(&event.dataset_id, *flow_type)
                {
                    flow_ids_2_abort.push(flow_id);
                    state.time_wheel.cancel_flow_activation(flow_id).int_err()?;
                }
            }
            flow_ids_2_abort
        };

        // Abort matched flows
        for flow_id in flow_ids_2_abort {
            let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
                .await
                .int_err()?;
            flow.abort(self.time_source.now()).int_err()?;
            flow.save(self.flow_event_store.as_ref()).await.int_err()?;
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
