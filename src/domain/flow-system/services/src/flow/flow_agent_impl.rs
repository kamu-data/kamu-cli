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

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use database_common_macros::transactional_method;
use dill::*;
use futures::TryStreamExt;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::InternalError;
use kamu_datasets::JOB_KAMU_DATASETS_DEPENDENCY_GRAPH_INDEXER;
use kamu_flow_system::*;
use kamu_task_system::*;
use messaging_outbox::{
    InitialConsumerBoundary,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
    Outbox,
    OutboxExt,
};
use time_source::SystemTimeSource;
use tracing::Instrument as _;

use crate::{FlowAbortHelper, FlowSchedulingHelper};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowAgentImpl {
    catalog: Catalog,
    time_source: Arc<dyn SystemTimeSource>,
    agent_config: Arc<FlowAgentConfig>,
    agent_started: Arc<Mutex<bool>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
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
    delivery: MessageDeliveryMechanism::Transactional,
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
        catalog: Catalog,
        time_source: Arc<dyn SystemTimeSource>,
        agent_config: Arc<FlowAgentConfig>,
    ) -> Self {
        Self {
            catalog,
            time_source,
            agent_config,
            agent_started: Arc::new(Mutex::new(false)),
        }
    }

    fn has_agent_started(&self) -> bool {
        let engine_started = self.agent_started.lock().unwrap();
        *engine_started
    }

    fn mark_engine_as_started(&self) {
        let mut engine_started = self.agent_started.lock().unwrap();
        *engine_started = true;
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

        // Publish progress event
        let outbox = transaction_catalog.get_one::<dyn Outbox>().unwrap();
        outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_FLOW_AGENT,
                FlowAgentUpdatedMessage {
                    update_time: start_time,
                    update_details: FlowAgentUpdateDetails::Loaded,
                },
            )
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
        let scheduling_helper = target_catalog.get_one::<FlowSchedulingHelper>().unwrap();

        // How many waiting flows do we have?
        let waiting_filters = FlowFilters {
            by_flow_type: None,
            by_flow_status: Some(FlowStatus::Waiting),
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
                    scheduling_helper
                        .trigger_flow_common(
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

        let scheduling_helper = target_catalog.get_one::<FlowSchedulingHelper>().unwrap();

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
                scheduling_helper
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

    #[transactional_method]
    async fn tick_current_timeslot(&self) -> Result<(), InternalError> {
        let flow_event_store = transaction_catalog.get_one::<dyn FlowEventStore>().unwrap();

        // Do we have a timeslot scheduled?
        let Some(nearest_flow_activation_moment) =
            flow_event_store.nearest_flow_activation_moment().await?
        else {
            return Ok(());
        };

        // Is it time to execute it yet?
        let current_time = self.time_source.now();
        if nearest_flow_activation_moment > current_time {
            return Ok(());
        }

        self.run_flows_for_timeslot(
            nearest_flow_activation_moment,
            flow_event_store,
            transaction_catalog,
        )
        .instrument(observability::tracing::root_span!("FlowAgent::activation"))
        .await
    }

    async fn run_flows_for_timeslot(
        &self,
        activation_moment: DateTime<Utc>,
        flow_event_store: Arc<dyn FlowEventStore>,
        transaction_catalog: dill::Catalog,
    ) -> Result<(), InternalError> {
        let planned_flow_ids: Vec<_> = flow_event_store
            .get_flows_scheduled_for_activation_at(activation_moment)
            .await?;

        let mut planned_task_futures = Vec::new();
        for planned_flow_id in planned_flow_ids {
            let transaction_catalog = transaction_catalog.clone();
            let flow_event_store = flow_event_store.clone();

            planned_task_futures.push(async move {
                let mut flow = Flow::load(planned_flow_id, flow_event_store.as_ref())
                    .await
                    .int_err()?;

                if flow.can_schedule() {
                    self.schedule_flow_task(transaction_catalog, &mut flow, activation_moment)
                        .await?;
                } else {
                    tracing::warn!(
                        flow_id = %planned_flow_id,
                        flow_status = %flow.status(),
                        "Skipped flow scheduling as no longer relevant"
                    );
                }

                Ok(())
            });
        }

        let results = futures::future::join_all(planned_task_futures).await;
        results
            .into_iter()
            .filter(Result::is_err)
            .map(|e| e.err().unwrap())
            .for_each(|e: InternalError| {
                tracing::error!(
                    error = ?e,
                    error_msg = %e,
                    "Scheduling flow failed"
                );
            });

        // Publish progress event
        let outbox = transaction_catalog.get_one::<dyn Outbox>().unwrap();
        outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_FLOW_AGENT,
                FlowAgentUpdatedMessage {
                    update_time: activation_moment,
                    update_details: FlowAgentUpdateDetails::ExecutedTimeslot,
                },
            )
            .await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(flow_id = %flow.flow_id))]
    async fn schedule_flow_task(
        &self,
        target_catalog: Catalog,
        flow: &mut Flow,
        schedule_time: DateTime<Utc>,
    ) -> Result<TaskID, InternalError> {
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
        flow.save(flow_event_store.as_ref()).await.int_err()?;

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
impl FlowAgent for FlowAgentImpl {
    /// Runs the update main loop
    async fn run(&self) -> Result<(), InternalError> {
        // Main scanning loop
        loop {
            // Run scheduling for current time slot
            self.tick_current_timeslot()
                .instrument(tracing::debug_span!("FlowAgent::tick"))
                .await?;

            self.time_source
                .sleep(self.agent_config.awaiting_step)
                .await;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for FlowAgentImpl {
    async fn run_initialization(&self) -> Result<(), InternalError> {
        let start_time = self.agent_config.round_time(self.time_source.now())?;
        self.recover_initial_flows_state(start_time).await?;

        self.mark_engine_as_started();

        Ok(())
    }
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
            .await?;

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

                        let outbox = target_catalog.get_one::<dyn Outbox>().unwrap();
                        outbox
                            .post_message(
                                MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
                                FlowProgressMessage::running(message.event_time, flow_id),
                            )
                            .await?;
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

                        let scheduling_helper =
                            target_catalog.get_one::<FlowSchedulingHelper>().unwrap();

                        let finish_time = self.agent_config.round_time(message.event_time)?;

                        // In case of success:
                        //  - execute follow-up method
                        if let Some(task_result) = flow.try_task_result_as_ref()
                            && !task_result.is_empty()
                        {
                            let flow_controller = get_flow_controller_from_catalog(
                                target_catalog,
                                &flow.flow_binding.flow_type,
                            )?;

                            flow_controller
                                .propagate_success(&flow, task_result, finish_time)
                                .await
                                .int_err()?;
                        }

                        // In case of success:
                        //  - schedule next flow, if we had any late activation cause
                        if message.outcome.is_success() {
                            scheduling_helper
                                .try_schedule_late_flow_activations(&flow)
                                .await?;
                        }

                        let outbox = target_catalog.get_one::<dyn Outbox>().unwrap();

                        // The outcome might not be final in case of retrying flows.
                        // If the flow is still retrying, await for the result of the next task
                        if let Some(flow_outcome) = flow.outcome.as_ref() {
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

                                // Trigger should make a decision about auto-stopping
                                let flow_trigger_service =
                                    target_catalog.get_one::<dyn FlowTriggerService>().unwrap();
                                flow_trigger_service
                                    .evaluate_trigger_on_failure(
                                        finish_time,
                                        &flow.flow_binding,
                                        !recoverable,
                                    )
                                    .await?;
                            }

                            // Try to schedule auto-polling flow, if applicable.
                            // We don't care whether we failed or succeeded,
                            // that is determined with the stop policy in the trigger.
                            scheduling_helper
                                .try_schedule_auto_polling_flow_if_enabled(
                                    finish_time,
                                    &flow.flow_binding,
                                )
                                .await?;

                            // Notify about finished flow
                            outbox
                                .post_message(
                                    MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
                                    FlowProgressMessage::finished(
                                        message.event_time,
                                        flow_id,
                                        flow_outcome.clone(),
                                    ),
                                )
                                .await?;
                        } else {
                            // Notify about scheduled retry
                            outbox
                                .post_message(
                                    MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
                                    FlowProgressMessage::retry_scheduled(
                                        message.event_time,
                                        flow_id,
                                        flow.timing
                                            .scheduled_for_activation_at
                                            .expect("Flow must have scheduled activation time"),
                                    ),
                                )
                                .await?;
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
            let scheduling_helper = target_catalog.get_one::<FlowSchedulingHelper>().unwrap();
            scheduling_helper
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
