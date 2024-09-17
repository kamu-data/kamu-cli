// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use std::sync::Arc;

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use database_common_macros::transactional_method;
use dill::*;
use futures::TryStreamExt;
use internal_error::InternalError;
use kamu_core::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE};
use kamu_flow_system::*;
use kamu_task_system::*;
use messaging_outbox::{
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageConsumptionDurability,
    Outbox,
    OutboxExt,
};
use time_source::SystemTimeSource;

use crate::{
    FlowAbortHelper,
    FlowEnqueueHelper,
    MESSAGE_CONSUMER_KAMU_FLOW_EXECUTOR,
    MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
    MESSAGE_PRODUCER_KAMU_FLOW_EXECUTOR,
    MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowExecutorImpl {
    catalog: Catalog,
    flow_time_wheel_service: Arc<dyn FlowTimeWheelService>,
    time_source: Arc<dyn SystemTimeSource>,
    executor_config: Arc<FlowExecutorConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowExecutor)]
#[interface(dyn FlowExecutorTestDriver)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<TaskProgressMessage>)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[interface(dyn MessageConsumerT<FlowConfigurationUpdatedMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_FLOW_EXECUTOR,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
        MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR,
        MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE
    ],
    durability: MessageConsumptionDurability::Durable,
})]
#[scope(Singleton)]
impl FlowExecutorImpl {
    pub fn new(
        catalog: Catalog,
        flow_time_wheel_service: Arc<dyn FlowTimeWheelService>,
        time_source: Arc<dyn SystemTimeSource>,
        executor_config: Arc<FlowExecutorConfig>,
    ) -> Self {
        Self {
            catalog,
            flow_time_wheel_service,
            time_source,
            executor_config,
        }
    }

    #[transactional_method]
    async fn recover_initial_flows_state(
        &self,
        start_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        // Recover already scheduled flows after server restart
        self.recover_time_wheel(&transaction_catalog, start_time)
            .await?;

        // Restore auto polling flows:
        //   - read active configurations
        //   - automatically trigger flows, if they are not waiting already
        self.restore_auto_polling_flows_from_configurations(&transaction_catalog, start_time)
            .await?;

        // Publish progress event
        let outbox = transaction_catalog.get_one::<dyn Outbox>().unwrap();
        outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_FLOW_EXECUTOR,
                FlowExecutorUpdatedMessage {
                    update_time: start_time,
                    update_details: FlowExecutorUpdateDetails::Loaded,
                },
            )
            .await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn recover_time_wheel(
        &self,
        target_catalog: &Catalog,
        start_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        // Extract necessary dependencies
        let flow_event_store = target_catalog.get_one::<dyn FlowEventStore>().unwrap();
        let enqueue_helper = target_catalog.get_one::<FlowEnqueueHelper>().unwrap();
        let outbox = target_catalog.get_one::<dyn Outbox>().unwrap();

        // How many waiting flows do we have?
        let waiting_filters = AllFlowFilters {
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

            // Process each waiting flow
            for waiting_flow_id in &waiting_flow_ids {
                // TODO: batch loading of flows
                let flow = Flow::load(*waiting_flow_id, flow_event_store.as_ref())
                    .await
                    .int_err()?;

                // We are not interested in flows with scheduled tasks,
                // as restoring these will be handled by TaskExecutor.
                if let Some(start_condition) = &flow.start_condition {
                    // We have to recover wakeup for scheduled/throttling condition
                    if let Some(wakeup_time) = start_condition.wake_up_at() {
                        let mut activation_time = wakeup_time;
                        if activation_time < start_time {
                            activation_time = start_time;
                        }
                        outbox
                            .post_message(
                                MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
                                FlowProgressMessage::enqueued(
                                    start_time,
                                    *waiting_flow_id,
                                    activation_time,
                                ),
                            )
                            .await?;
                    }
                    // and we also need to re-evaluate the batching condition
                    else if let FlowStartCondition::Batching(b) = start_condition {
                        enqueue_helper
                            .trigger_flow_common(
                                &flow.flow_key,
                                FlowTrigger::AutoPolling(FlowTriggerAutoPolling {
                                    trigger_time: start_time,
                                }),
                                FlowTriggerContext::Batching(b.active_transform_rule),
                                None,
                            )
                            .await?;
                    }
                }
            }

            processed_waiting_flows += waiting_flow_ids.len();
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn restore_auto_polling_flows_from_configurations(
        &self,
        target_catalog: &Catalog,
        start_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let flow_configuration_service = target_catalog
            .get_one::<dyn FlowConfigurationService>()
            .unwrap();
        let flow_event_store = target_catalog.get_one::<dyn FlowEventStore>().unwrap();

        // Query all enabled flow configurations
        let enabled_configurations: Vec<_> = flow_configuration_service
            .list_enabled_configurations()
            .try_collect()
            .await?;

        // Split configs by those which have a schedule or different rules
        let (schedule_configs, non_schedule_configs): (Vec<_>, Vec<_>) = enabled_configurations
            .into_iter()
            .partition(|config| matches!(config.rule, FlowConfigurationRule::Schedule(_)));

        let enqueue_helper = target_catalog.get_one::<FlowEnqueueHelper>().unwrap();

        // Activate all configs, ensuring schedule configs precedes non-schedule configs
        // (this i.e. forces all root datasets to be updated earlier than the derived)
        //
        // Thought: maybe we need topological sorting by derived relations as well to
        // optimize the initial execution order, but batching rules may work just fine
        for enabled_config in schedule_configs
            .into_iter()
            .chain(non_schedule_configs.into_iter())
        {
            // Do not re-trigger the flow that has already triggered
            let maybe_pending_flow_id = flow_event_store
                .try_get_pending_flow(&enabled_config.flow_key)
                .await?;
            if maybe_pending_flow_id.is_none() {
                enqueue_helper
                    .activate_flow_configuration(
                        start_time,
                        enabled_config.flow_key,
                        enabled_config.rule,
                    )
                    .await?;
            }
        }

        Ok(())
    }

    #[transactional_method]
    async fn run_flows_current_timeslot(
        &self,
        timeslot_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let planned_flow_ids: Vec<_> = self.flow_time_wheel_service.take_nearest_planned_flows();

        let mut planned_task_futures = Vec::new();
        for planned_flow_id in planned_flow_ids {
            let transaction_catalog = transaction_catalog.clone();
            let flow_event_store = transaction_catalog.get_one::<dyn FlowEventStore>().unwrap();

            planned_task_futures.push(async move {
                let mut flow = Flow::load(planned_flow_id, flow_event_store.as_ref())
                    .await
                    .int_err()?;

                if flow.can_schedule() {
                    self.schedule_flow_task(transaction_catalog, &mut flow, timeslot_time)
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
                MESSAGE_PRODUCER_KAMU_FLOW_EXECUTOR,
                FlowExecutorUpdatedMessage {
                    update_time: timeslot_time,
                    update_details: FlowExecutorUpdateDetails::ExecutedTimeslot,
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
        let logical_plan =
            self.make_task_logical_plan(&flow.flow_key, flow.config_snapshot.as_ref())?;

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

    /// Creates task logical plan that corresponds to template
    pub fn make_task_logical_plan(
        &self,
        flow_key: &FlowKey,
        maybe_config_snapshot: Option<&FlowConfigurationSnapshot>,
    ) -> Result<LogicalPlan, InternalError> {
        match flow_key {
            FlowKey::Dataset(flow_key) => match flow_key.flow_type {
                DatasetFlowType::Ingest | DatasetFlowType::ExecuteTransform => {
                    let mut fetch_uncacheable = false;
                    if let Some(config_snapshot) = maybe_config_snapshot
                        && let FlowConfigurationSnapshot::Ingest(ingest_rule) = config_snapshot
                    {
                        fetch_uncacheable = ingest_rule.fetch_uncacheable;
                    }
                    Ok(LogicalPlan::UpdateDataset(UpdateDataset {
                        dataset_id: flow_key.dataset_id.clone(),
                        fetch_uncacheable,
                    }))
                }
                DatasetFlowType::HardCompaction => {
                    let mut max_slice_size: Option<u64> = None;
                    let mut max_slice_records: Option<u64> = None;
                    let mut keep_metadata_only = false;

                    if let Some(config_snapshot) = maybe_config_snapshot
                        && let FlowConfigurationSnapshot::Compaction(compaction_rule) =
                            config_snapshot
                    {
                        max_slice_size = compaction_rule.max_slice_size();
                        max_slice_records = compaction_rule.max_slice_records();
                        keep_metadata_only =
                            matches!(compaction_rule, CompactionRule::MetadataOnly(_));
                    };

                    Ok(LogicalPlan::HardCompactionDataset(HardCompactionDataset {
                        dataset_id: flow_key.dataset_id.clone(),
                        max_slice_size,
                        max_slice_records,
                        keep_metadata_only,
                    }))
                }
                DatasetFlowType::Reset => {
                    if let Some(config_rule) = maybe_config_snapshot
                        && let FlowConfigurationSnapshot::Reset(reset_rule) = config_rule
                    {
                        return Ok(LogicalPlan::Reset(ResetDataset {
                            dataset_id: flow_key.dataset_id.clone(),
                            new_head_hash: reset_rule.new_head_hash.clone(),
                            old_head_hash: reset_rule.old_head_hash.clone(),
                            recursive: reset_rule.recursive,
                        }));
                    }
                    InternalError::bail("Reset flow cannot be called without configuration")
                }
            },
            FlowKey::System(flow_key) => {
                match flow_key.flow_type {
                    // TODO: replace on correct logical plan
                    SystemFlowType::GC => Ok(LogicalPlan::Probe(Probe {
                        dataset_id: None,
                        busy_time: Some(std::time::Duration::from_secs(20)),
                        end_with_outcome: Some(TaskOutcome::Success(TaskResult::Empty)),
                    })),
                }
            }
        }
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
impl FlowExecutor for FlowExecutorImpl {
    #[tracing::instrument(level = "info", skip_all)]
    async fn pre_run(&self, planned_start_time: DateTime<Utc>) -> Result<(), InternalError> {
        let start_time = self.executor_config.round_time(planned_start_time)?;
        self.recover_initial_flows_state(start_time).await
    }

    /// Runs the update main loop
    async fn run(&self) -> Result<(), InternalError> {
        // Main scanning loop
        loop {
            let tick_span = tracing::trace_span!("FlowExecutor::tick");
            let _ = tick_span.enter();

            let current_time = self.time_source.now();

            // Do we have a timeslot scheduled?
            let maybe_nearest_activation_time =
                self.flow_time_wheel_service.nearest_activation_moment();

            // Is it time to execute it yet?
            if let Some(nearest_activation_time) = maybe_nearest_activation_time
                && nearest_activation_time <= current_time
            {
                let activation_span = tracing::info_span!("FlowExecutor::activation");
                let _ = activation_span.enter();

                // Run scheduling for current time slot. Should not throw any errors
                self.run_flows_current_timeslot(nearest_activation_time)
                    .await?;
            }

            self.time_source
                .sleep(self.executor_config.awaiting_step)
                .await;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowExecutorTestDriver for FlowExecutorImpl {
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

impl MessageConsumer for FlowExecutorImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<TaskProgressMessage> for FlowExecutorImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowExecutorImpl[TaskProgressMessage]"
    )]
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

                        let enqueue_helper = target_catalog.get_one::<FlowEnqueueHelper>().unwrap();

                        let finish_time = self.executor_config.round_time(message.event_time)?;

                        // In case of success:
                        //  - execute followup method
                        if let Some(flow_result) = flow.try_result_as_ref()
                            && !flow_result.is_empty()
                        {
                            match flow.flow_key.get_type().success_followup_method() {
                                FlowSuccessFollowupMethod::Ignore => {}
                                FlowSuccessFollowupMethod::TriggerDependent => {
                                    enqueue_helper
                                        .enqueue_dependent_flows(finish_time, &flow, flow_result)
                                        .await?;
                                }
                            }
                        }

                        // In case of success:
                        //  - enqueue next auto-polling flow cycle
                        if message.outcome.is_success() {
                            enqueue_helper
                                .try_enqueue_scheduled_auto_polling_flow_if_enabled(
                                    finish_time,
                                    &flow.flow_key,
                                )
                                .await?;
                        }

                        let outbox = target_catalog.get_one::<dyn Outbox>().unwrap();
                        outbox
                            .post_message(
                                MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
                                FlowProgressMessage::finished(
                                    message.event_time,
                                    flow_id,
                                    flow.outcome
                                        .as_ref()
                                        .expect("Outcome must be attached by now")
                                        .clone(),
                                ),
                            )
                            .await?;

                        // TODO: retry logic in case of failed outcome
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
impl MessageConsumerT<FlowConfigurationUpdatedMessage> for FlowExecutorImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowExecutorImpl[FlowConfigurationUpdatedMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &Catalog,
        message: &FlowConfigurationUpdatedMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received flow configuration message");

        if message.paused {
            let maybe_pending_flow_id = {
                let flow_event_store = target_catalog.get_one::<dyn FlowEventStore>().unwrap();
                flow_event_store
                    .try_get_pending_flow(&message.flow_key)
                    .await?
            };

            if let Some(flow_id) = maybe_pending_flow_id {
                let abort_helper = target_catalog.get_one::<FlowAbortHelper>().unwrap();
                abort_helper.abort_flow(flow_id).await?;
            }
        } else {
            let enqueue_helper = target_catalog.get_one::<FlowEnqueueHelper>().unwrap();
            enqueue_helper
                .activate_flow_configuration(
                    self.executor_config.round_time(message.event_time)?,
                    message.flow_key.clone(),
                    message.rule.clone(),
                )
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for FlowExecutorImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowExecutorImpl[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        match message {
            DatasetLifecycleMessage::Deleted(message) => {
                let flow_ids_2_abort = {
                    let flow_event_store = target_catalog.get_one::<dyn FlowEventStore>().unwrap();

                    // For every possible dataset flow:
                    //  - drop queued activations
                    //  - collect ID of aborted flow
                    let mut flow_ids_2_abort: Vec<_> =
                        Vec::with_capacity(DatasetFlowType::all().len());
                    for flow_type in DatasetFlowType::all() {
                        if let Some(flow_id) = flow_event_store
                            .try_get_pending_flow(&FlowKey::dataset(
                                message.dataset_id.clone(),
                                *flow_type,
                            ))
                            .await?
                        {
                            flow_ids_2_abort.push(flow_id);
                        }
                    }
                    flow_ids_2_abort
                };

                // Abort matched flows
                for flow_id in flow_ids_2_abort {
                    let abort_helper = target_catalog.get_one::<FlowAbortHelper>().unwrap();
                    abort_helper.abort_flow(flow_id).await?;
                }
            }

            DatasetLifecycleMessage::Created(_)
            | DatasetLifecycleMessage::DependenciesUpdated(_) => {
                // No action required
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
pub enum FlowTriggerContext {
    Unconditional,
    Scheduled(Schedule),
    Batching(TransformRule),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
pub struct DownstreamDependencyFlowPlan {
    pub flow_key: FlowKey,
    pub flow_trigger_context: FlowTriggerContext,
    pub maybe_config_snapshot: Option<FlowConfigurationSnapshot>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum DownstreamDependencyTriggerType {
    TriggerAllEnabledExecuteTransform,
    TriggerOwnHardCompaction,
    Empty,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
