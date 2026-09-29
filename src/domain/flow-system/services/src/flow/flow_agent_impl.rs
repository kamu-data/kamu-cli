// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use std::sync::{Arc, Mutex};

use async_utils::BackgroundAgent;
use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use database_common_macros::{transactional_method, transactional_method1};
use dill::*;
use futures::{StreamExt, TryStreamExt};
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::InternalError;
use kamu_datasets::JOB_KAMU_DATASETS_DEPENDENCY_GRAPH_INDEXER;
use kamu_flow_system::*;
use kamu_task_system::*;
use messaging_outbox::*;
use time_source::SystemTimeSource;
use tracing::Instrument as _;
use wakeup_listener::{WakeHint, WakeupListener, WakeupListenerConfig};

use crate::{FlowAbortHelper, FlowAgentMetrics, FlowCompletionMetrics, FlowSchedulingServiceImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowAgentImpl {
    catalog: CatalogWeakRef,
    time_source: Arc<dyn SystemTimeSource>,
    agent_config: Arc<FlowAgentConfig>,
    activation_config: Arc<FlowAgentActivationConfig>,
    wakeup_config: Arc<WakeupListenerConfig>,
    flow_activation_wakeup_source: Arc<dyn FlowActivationWakeupSource>,
    metrics: Arc<FlowAgentMetrics>,
    completion_metrics: Arc<FlowCompletionMetrics>,
    state: Arc<Mutex<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    agent_started: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn BackgroundAgent)]
#[interface(dyn FlowAgent)]
#[interface(dyn FlowAgentTestDriver)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<TaskProgressMessage>)]
#[interface(dyn MessageConsumerT<FlowTriggerUpdatedMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_FLOW_AGENT,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
        MESSAGE_PRODUCER_KAMU_TASK_AGENT,
    ],
    consumption_mode: MessageConsumptionMode::TransactionalWrapped,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_FLOW_AGENT_RECOVERY,
    depends_on: &[
        JOB_KAMU_DATASETS_DEPENDENCY_GRAPH_INDEXER
    ],
    requires_transaction: false,
})]
#[scope(Singleton)]
impl FlowAgentImpl {
    pub fn new(
        catalog: CatalogWeakRef,
        time_source: Arc<dyn SystemTimeSource>,
        agent_config: Arc<FlowAgentConfig>,
        activation_config: Arc<FlowAgentActivationConfig>,
        wakeup_config: Arc<WakeupListenerConfig>,
        flow_activation_wakeup_source: Arc<dyn FlowActivationWakeupSource>,
        metrics: Arc<FlowAgentMetrics>,
        completion_metrics: Arc<FlowCompletionMetrics>,
    ) -> Self {
        Self {
            catalog,
            time_source,
            agent_config,
            activation_config,
            wakeup_config,
            flow_activation_wakeup_source,
            metrics,
            completion_metrics,
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    fn has_agent_started(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.agent_started
    }

    fn mark_engine_as_started(&self) {
        let mut state = self.state.lock().unwrap();
        state.agent_started = true;
    }

    #[transactional_method]
    #[tracing::instrument(level = "info", skip_all)]
    async fn recover_initial_flows_state(
        &self,
        start_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        // Recover already scheduled flows after server restart
        self.recover_waiting_flows(&transaction_catalog, start_time)
            .await?;

        // Restore auto polling flows:
        //   - read active triggers
        //   - automatically trigger flows, if they are not waiting already
        self.restore_auto_polling_flows_from_triggers(&transaction_catalog, start_time)
            .await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn recover_waiting_flows(
        &self,
        target_catalog: &Catalog,
        start_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        // Extract necessary dependencies
        let flow_event_store = target_catalog.get_one::<dyn FlowEventStore>().unwrap();
        let scheduling_service = target_catalog
            .get_one::<FlowSchedulingServiceImpl>()
            .unwrap();

        // How many waiting flows do we have?
        let waiting_filters = FlowFilters {
            by_flow_types: None,
            by_flow_statuses: Some(vec![FlowStatus::Waiting]),
            by_initiator: None,
        };
        let total_waiting_flows = flow_event_store
            .get_count_all_flows(&waiting_filters)
            .await?;

        // For each waiting flow, check if it should contribute to time wheel
        // Load them in pages
        let mut processed_waiting_flows = 0;
        while processed_waiting_flows < total_waiting_flows {
            // Another page
            let waiting_flow_ids: Vec<_> = flow_event_store
                .get_all_flow_ids(
                    &waiting_filters,
                    &FlowOrder::creation_time_desc(),
                    PaginationOpts {
                        offset: processed_waiting_flows,
                        limit: 100,
                    },
                )
                .try_collect()
                .await?;

            processed_waiting_flows += waiting_flow_ids.len();
            let mut state_stream = flow_event_store.get_stream(waiting_flow_ids);

            while let Some(flow) = state_stream.try_next().await? {
                // We need to re-evaluate reactive conditions only
                if let Some(FlowStartCondition::Reactive(b)) = &flow.start_condition {
                    scheduling_service
                        .trigger_flow_common(
                            start_time,
                            &flow.flow_binding,
                            Some(FlowTriggerRule::Reactive(b.active_rule)),
                            vec![FlowActivationCause::AutoPolling(
                                FlowActivationCauseAutoPolling {
                                    activation_time: start_time,
                                },
                            )],
                            None,
                        )
                        .await?;
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn restore_auto_polling_flows_from_triggers(
        &self,
        target_catalog: &Catalog,
        start_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let flow_trigger_service = target_catalog.get_one::<dyn FlowTriggerService>().unwrap();
        let flow_event_store = target_catalog.get_one::<dyn FlowEventStore>().unwrap();

        // Query all enabled flow triggers
        let enabled_triggers: Vec<_> = flow_trigger_service
            .list_enabled_triggers()
            .try_collect()
            .await?;

        // Split triggers by those which have a schedule or different rules
        let (schedule_triggers, non_schedule_triggers): (Vec<_>, Vec<_>) = enabled_triggers
            .into_iter()
            .partition(|config| matches!(config.rule, FlowTriggerRule::Schedule(_)));

        let scheduling_service = target_catalog
            .get_one::<FlowSchedulingServiceImpl>()
            .unwrap();

        // Activate all configs, ensuring schedule configs precedes non-schedule configs
        // (this i.e. forces all root datasets to be updated earlier than the derived)
        //
        // Thought: maybe we need topological sorting by derived relations as well to
        // optimize the initial execution order, but reactive rules may work just fine
        for enabled_trigger in schedule_triggers
            .into_iter()
            .chain(non_schedule_triggers.into_iter())
        {
            // Do not re-trigger the flow that has already triggered
            let maybe_pending_flow_id = flow_event_store
                .try_get_pending_flow(&enabled_trigger.flow_binding)
                .await?;
            if maybe_pending_flow_id.is_none() {
                scheduling_service
                    .activate_flow_trigger(
                        target_catalog,
                        start_time,
                        &enabled_trigger.flow_binding,
                        enabled_trigger.rule,
                    )
                    .await?;
            }
        }

        Ok(())
    }

    async fn activate_due_flows(&self) -> Result<(), InternalError> {
        // Fixed for the pass, so that it ends even while more flows become due
        let current_time = self.time_source.now();

        let mut page_after = None;
        loop {
            let page = self
                .load_flows_due_for_activation(current_time, page_after)
                .await?;
            self.activate_flows(page.flows).await;

            let Some(next_page_after) = page.next_page_after else {
                return Ok(());
            };
            page_after = Some(next_page_after);
        }
    }

    /// Each flow is activated in its own transaction, so that a failure of one
    /// flow rolls back only its own changes. Activations start in the order of
    /// activation moments, and run concurrently on separate connections
    async fn activate_flows(&self, flows: Vec<(Flow, DateTime<Utc>)>) {
        futures::stream::iter(flows)
            .for_each_concurrent(
                self.activation_config.concurrency.get(),
                |(flow, activation_time)| {
                    let flow_id = flow.flow_id;
                    let flow_type = flow.flow_binding.flow_type.clone();
                    async move {
                        let result = self.activate_flow(flow, activation_time).await;
                        self.metrics.on_activation(
                            &flow_type,
                            &result,
                            activation_time,
                            self.time_source.now(),
                        );
                        match result {
                            Ok(()) => {}
                            Err(ActivateFlowError::ConcurrentModification) => {
                                // The store is re-read on the next iteration
                                tracing::info!(
                                    %flow_id,
                                    "Flow activation skipped as the flow changed concurrently"
                                );
                            }
                            Err(ActivateFlowError::Internal(e)) => {
                                tracing::error!(
                                    %flow_id,
                                    error = ?e,
                                    error_msg = %e,
                                    "Flow activation failed"
                                );
                            }
                        }
                    }
                    .instrument(observability::tracing::root_span!(
                        "FlowAgent::activation",
                        %flow_id,
                        %activation_time
                    ))
                },
            )
            .await;
    }

    /// Loads a page of flows due for activation in one transaction.
    /// Pages by the activation key rather than restarting from the first due
    /// flow: flows whose activation failed stay due, and would come back first
    #[transactional_method1(flow_event_store: Arc<dyn FlowEventStore>)]
    async fn load_flows_due_for_activation(
        &self,
        current_time: DateTime<Utc>,
        page_after: Option<DueFlowActivation>,
    ) -> Result<DueFlowsPage, InternalError> {
        let batch_size = self.activation_config.batch_size.get();
        let due_flows = flow_event_store
            .get_flows_due_for_activation(current_time, page_after, batch_size)
            .await?;

        let next_page_after = if due_flows.len() == batch_size {
            due_flows.last().copied()
        } else {
            None
        };

        let flow_ids: Vec<_> = due_flows.iter().map(|due_flow| due_flow.flow_id).collect();
        let load_results = Flow::load_multi(&flow_ids, flow_event_store.as_ref())
            .await
            .int_err()?;

        // A flow failing to load must not hold back the others
        let flows = load_results
            .into_iter()
            .zip(due_flows)
            .filter_map(|(load_result, due_flow)| match load_result {
                Ok(flow) => Some((flow, due_flow.activation_time)),
                Err(e) => {
                    tracing::error!(
                        flow_id = %due_flow.flow_id,
                        error = ?e,
                        error_msg = %e,
                        "Flow due for activation failed to load"
                    );
                    None
                }
            })
            .collect();

        Ok(DueFlowsPage {
            flows,
            next_page_after,
        })
    }

    #[transactional_method1(flow_event_store: Arc<dyn FlowEventStore>)]
    async fn nearest_flow_activation_moment(&self) -> Result<Option<DateTime<Utc>>, InternalError> {
        flow_event_store.nearest_flow_activation_moment().await
    }

    /// Sleeps until the nearest activation moment, or until a flow might have
    /// been scheduled for activation, whichever comes first
    async fn wait_for_next_activation(
        &self,
        wakeup_listener: &dyn WakeupListener,
    ) -> Result<(), InternalError> {
        let maybe_nearest_activation_moment = self.nearest_flow_activation_moment().await?;

        let wait_signal = wakeup_listener.wait_wake(
            self.wakeup_config.max_listening_timeout,
            self.wakeup_config.min_debounce_interval,
        );

        let hint = match maybe_nearest_activation_moment {
            None => wait_signal.await?,
            Some(nearest_activation_moment) => {
                let time_left = nearest_activation_moment - self.time_source.now();
                let sleep_duration = if time_left > chrono::Duration::zero() {
                    time_left
                } else {
                    // Still due right after activating due flows means their activation failed:
                    // retry them later rather than spin
                    self.agent_config.awaiting_step
                };

                // Dropping the signal wait is safe: whatever it could have consumed was
                // committed before, so the next iteration sees it in the store anyway.
                // The deadline is measured by the time source, which tests control
                tokio::select! {
                    hint = wait_signal => hint?,
                    () = self.time_source.sleep(sleep_duration) => WakeHint::Timeout,
                }
            }
        };

        tracing::debug!(
            ?hint,
            ?maybe_nearest_activation_moment,
            "Flow agent woke up with a hint"
        );

        Ok(())
    }

    /// Activates a flow loaded beforehand. If the flow changed since then,
    /// saving it fails as a concurrent modification, and the transaction is
    /// rolled back
    #[transactional_method]
    async fn activate_flow(
        &self,
        mut flow: Flow,
        activation_time: DateTime<Utc>,
    ) -> Result<(), ActivateFlowError> {
        self.schedule_flow_task(transaction_catalog, &mut flow, activation_time)
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(flow_id = %flow.flow_id))]
    async fn schedule_flow_task(
        &self,
        target_catalog: Catalog,
        flow: &mut Flow,
        schedule_time: DateTime<Utc>,
    ) -> Result<TaskID, ActivateFlowError> {
        // Find a controller for this flow type
        let flow_controller =
            get_flow_controller_from_catalog(&target_catalog, &flow.flow_binding.flow_type)?;

        // Controller should create a logical plan that corresponds to the flow type
        let logical_plan = flow_controller
            .build_task_logical_plan(flow)
            .await
            .int_err()?;

        let task_scheduler = target_catalog.get_one::<dyn TaskScheduler>().unwrap();
        let task = task_scheduler
            .create_task(
                logical_plan,
                Some(TaskMetadata::from(vec![(
                    METADATA_TASK_FLOW_ID,
                    flow.flow_id.to_string(),
                )])),
            )
            .await
            .int_err()?;

        flow.set_relevant_start_condition(
            schedule_time,
            FlowStartCondition::Executor(FlowStartConditionExecutor {
                task_id: task.task_id,
            }),
        )
        .int_err()?;

        flow.on_task_scheduled(schedule_time, task.task_id)
            .int_err()?;

        let flow_event_store = target_catalog.get_one::<dyn FlowEventStore>().unwrap();
        flow.save(flow_event_store.as_ref())
            .await
            .map_err(|e| match e {
                SaveError::ConcurrentModification(_) => ActivateFlowError::ConcurrentModification,
                e => ActivateFlowError::Internal(e.int_err()),
            })?;

        Ok(task.task_id)
    }

    fn flow_id_from_task_metadata(
        task_metadata: &TaskMetadata,
    ) -> Result<Option<FlowID>, InternalError> {
        let maybe_flow_id_property = task_metadata.try_get_property(METADATA_TASK_FLOW_ID);
        Ok(match maybe_flow_id_property {
            Some(flow_id_property) => Some(FlowID::from(&flow_id_property).int_err()?),
            None => None,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for FlowAgentImpl {
    async fn run_initialization(&self) -> Result<(), InternalError> {
        let catalog = self.catalog.upgrade();
        let flow_types: Vec<&'static str> = catalog
            .builders_for::<dyn FlowController>()
            .flat_map(|builder| {
                builder
                    .metadata_get_all::<FlowControllerMeta>()
                    .into_iter()
                    .map(|meta| meta.flow_type)
                    .collect::<Vec<_>>()
            })
            .collect();
        self.metrics.init(flow_types.iter().copied());
        self.completion_metrics.init(flow_types.iter().copied());

        // Run recovery procedure
        let start_time = self.agent_config.round_time(self.time_source.now())?;
        self.recover_initial_flows_state(start_time).await?;

        // Mark the agent as started
        self.mark_engine_as_started();

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl BackgroundAgent for FlowAgentImpl {
    fn agent_name(&self) -> &'static str {
        FLOW_AGENT_NAME
    }

    /// Runs the update main loop
    async fn run(&self) -> Result<(), InternalError> {
        // Kept across iterations, so that no change is missed between them
        let wakeup_listener = self.flow_activation_wakeup_source.new_wakeup_listener();

        loop {
            // Activate all flows that are due by now
            self.activate_due_flows()
                .instrument(tracing::debug_span!("FlowAgent::tick"))
                .await?;

            // The deadline is re-read from the store on every iteration,
            // so aborted or rescheduled flows need no signal
            self.wait_for_next_activation(wakeup_listener.as_ref())
                .await?;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowAgent for FlowAgentImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DueFlowsPage {
    /// Loaded flows, paired with their activation moments
    flows: Vec<(Flow, DateTime<Utc>)>,
    /// Where the next page starts, unless this page was the last one
    next_page_after: Option<DueFlowActivation>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub(crate) enum ActivateFlowError {
    #[error("Flow was modified concurrently")]
    ConcurrentModification,

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowAgentTestDriver for FlowAgentImpl {
    /// Pretends it is time to schedule the given flow that was not waiting for
    /// anything else
    async fn mimic_flow_scheduled(
        &self,
        target_catalog: &Catalog,
        flow_id: FlowID,
        schedule_time: DateTime<Utc>,
    ) -> Result<TaskID, InternalError> {
        let flow_event_store = target_catalog.get_one::<dyn FlowEventStore>().unwrap();

        let mut flow = Flow::load(flow_id, flow_event_store.as_ref())
            .await
            .int_err()?;

        let task_id = self
            .schedule_flow_task(target_catalog.clone(), &mut flow, schedule_time)
            .await
            .int_err()?;

        Ok(task_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for FlowAgentImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<TaskProgressMessage> for FlowAgentImpl {
    #[tracing::instrument(level = "debug", skip_all, name = "FlowAgentImpl[TaskProgressMessage]")]
    async fn consume_message(
        &self,
        target_catalog: &Catalog,
        message: &TaskProgressMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received task progress message");

        let flow_event_store = target_catalog.get_one::<dyn FlowEventStore>().unwrap();

        match message {
            TaskProgressMessage::Running(message) => {
                // Is this a task associated with flows?
                let maybe_flow_id = Self::flow_id_from_task_metadata(&message.task_metadata)?;
                if let Some(flow_id) = maybe_flow_id {
                    let mut flow = Flow::load(flow_id, flow_event_store.as_ref())
                        .await
                        .int_err()?;
                    if flow.status() != FlowStatus::Finished {
                        flow.on_task_running(message.event_time, message.task_id)
                            .int_err()?;
                        flow.save(flow_event_store.as_ref()).await.int_err()?;
                    } else {
                        tracing::info!(
                            flow_id = %flow.flow_id,
                            flow_status = %flow.status(),
                            task_id = %message.task_id,
                            "Flow ignores notification about running task as no longer relevant"
                        );
                    }
                }
            }
            TaskProgressMessage::Finished(message) => {
                // Is this a task associated with flows?
                let maybe_flow_id = Self::flow_id_from_task_metadata(&message.task_metadata)?;
                if let Some(flow_id) = maybe_flow_id {
                    let mut flow = Flow::load(flow_id, flow_event_store.as_ref())
                        .await
                        .int_err()?;

                    if flow.status() != FlowStatus::Finished {
                        flow.on_task_finished(
                            message.event_time,
                            message.task_id,
                            message.outcome.clone(),
                        )
                        .int_err()?;
                        flow.save(flow_event_store.as_ref()).await.int_err()?;

                        // The outcome might not be final in case of retrying flows.
                        // If the flow is still retrying, await for the result of the next task
                        if flow.outcome.is_some() {
                            self.completion_metrics.on_flow_finished(&flow);

                            // Handle flow failure if it reached a terminal state
                            if message.outcome.is_failure() {
                                let recoverable = message.outcome.is_recoverable_failure();
                                if recoverable {
                                    tracing::warn!(
                                        flow_id = %flow.flow_id,
                                        "Flow has reached a failed state after exhausting all retries"
                                    );
                                } else {
                                    tracing::warn!(
                                        flow_id = %flow.flow_id,
                                        "Flow has reached a failed state after unrecoverable failure"
                                    );
                                }
                            }

                            // In case of success: propagate success and dispatch sensitive events
                            if let Some(task_result) = flow.try_task_result_as_ref()
                                && !task_result.is_empty()
                            {
                                let flow_controller = get_flow_controller_from_catalog(
                                    target_catalog,
                                    &flow.flow_binding.flow_type,
                                )?;

                                flow_controller
                                    .propagate_success(&flow, task_result, message.event_time)
                                    .await
                                    .int_err()?;
                            }
                        }
                    } else {
                        tracing::info!(
                            flow_id = %flow.flow_id,
                            flow_status = %flow.status(),
                            task_id = %message.task_id,
                            "Flow ignores notification about finished task as no longer relevant"
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<FlowTriggerUpdatedMessage> for FlowAgentImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowAgentImpl[FlowTriggerUpdatedMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &Catalog,
        message: &FlowTriggerUpdatedMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received flow trigger message");

        if !self.has_agent_started() {
            // If the agent is not started yet, we do not process flow trigger updates
            // as they are only relevant after the agent has started.
            return Ok(());
        }

        // Active trigger => activate it
        if message.trigger_status.is_active() {
            let scheduling_service = target_catalog
                .get_one::<FlowSchedulingServiceImpl>()
                .unwrap();
            scheduling_service
                .activate_flow_trigger(
                    target_catalog,
                    self.agent_config.round_time(message.event_time)?,
                    &message.flow_binding,
                    message.rule.clone(),
                )
                .await?;
        } else {
            // Inactive trigger => abort it
            let abort_helper = target_catalog.get_one::<FlowAbortHelper>().unwrap();
            abort_helper
                .deactivate_flow_trigger(target_catalog, &message.flow_binding)
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
