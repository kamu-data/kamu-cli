// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::component;
use internal_error::InternalError;
use kamu_core::{DatasetChangesService, DatasetOwnershipService, DependencyGraphService};
use kamu_flow_system::*;
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;

use super::{DownstreamDependencyFlowPlan, FlowTriggerContext};
use crate::{DownstreamDependencyTriggerType, MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FlowEnqueueHelper {
    flow_timewheel_service: Arc<dyn FlowTimeWheelService>,
    flow_event_store: Arc<dyn FlowEventStore>,
    flow_configuration_service: Arc<dyn FlowConfigurationService>,
    outbox: Arc<dyn Outbox>,
    dataset_changes_service: Arc<dyn DatasetChangesService>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    dataset_ownership_service: Arc<dyn DatasetOwnershipService>,
    time_source: Arc<dyn SystemTimeSource>,
    executor_config: Arc<FlowExecutorConfig>,
}

#[component(pub)]
impl FlowEnqueueHelper {
    pub(crate) fn new(
        flow_timewheel_service: Arc<dyn FlowTimeWheelService>,
        flow_event_store: Arc<dyn FlowEventStore>,
        flow_configuration_service: Arc<dyn FlowConfigurationService>,
        outbox: Arc<dyn Outbox>,
        dataset_changes_service: Arc<dyn DatasetChangesService>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
        dataset_ownership_service: Arc<dyn DatasetOwnershipService>,
        time_source: Arc<dyn SystemTimeSource>,
        executor_config: Arc<FlowExecutorConfig>,
    ) -> Self {
        Self {
            flow_timewheel_service,
            flow_event_store,
            flow_configuration_service,
            outbox,
            dataset_changes_service,
            dependency_graph_service,
            dataset_ownership_service,
            time_source,
            executor_config,
        }
    }

    pub(crate) async fn activate_flow_configuration(
        &self,
        start_time: DateTime<Utc>,
        flow_key: FlowKey,
        rule: FlowConfigurationRule,
    ) -> Result<(), InternalError> {
        tracing::trace!(flow_key=?flow_key, rule=?rule, "Activating flow configuration");

        match &flow_key {
            FlowKey::Dataset(_) => {
                match &rule {
                    FlowConfigurationRule::TransformRule(_) => {
                        self.enqueue_auto_polling_flow_unconditionally(start_time, &flow_key)
                            .await?;
                    }
                    FlowConfigurationRule::IngestRule(ingest_rule) => {
                        self.enqueue_scheduled_auto_polling_flow(
                            start_time,
                            &flow_key,
                            &ingest_rule.schedule_condition,
                        )
                        .await?;
                    }
                    // Such as compaction and reset is very dangerous operation we
                    // skip running it during activation flow configurations.
                    // And schedule will be used only for system flows
                    FlowConfigurationRule::CompactionRule(_)
                    | FlowConfigurationRule::Schedule(_)
                    | FlowConfigurationRule::ResetRule(_) => (),
                }
            }
            FlowKey::System(_) => {
                if let FlowConfigurationRule::Schedule(schedule) = &rule {
                    self.enqueue_scheduled_auto_polling_flow(start_time, &flow_key, schedule)
                        .await?;
                } else {
                    unimplemented!(
                        "Doubt will ever need to schedule system flows via batching rules"
                    )
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn try_enqueue_scheduled_auto_polling_flow_if_enabled(
        &self,
        start_time: DateTime<Utc>,
        flow_key: &FlowKey,
    ) -> Result<(), InternalError> {
        let maybe_active_schedule = self
            .flow_configuration_service
            .try_get_flow_schedule(flow_key.clone())
            .await
            .int_err()?;

        if let Some(active_schedule) = maybe_active_schedule {
            self.enqueue_scheduled_auto_polling_flow(start_time, flow_key, &active_schedule)
                .await?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(?flow.flow_key, %flow.flow_id))]
    pub(crate) async fn enqueue_dependent_flows(
        &self,
        input_success_time: DateTime<Utc>,
        flow: &Flow,
        flow_result: &FlowResult,
    ) -> Result<(), InternalError> {
        if let FlowKey::Dataset(fk_dataset) = &flow.flow_key {
            let dependent_dataset_flow_plans = self
                .make_downstream_dependencies_flow_plans(fk_dataset, flow.config_snapshot.as_ref())
                .await?;
            if dependent_dataset_flow_plans.is_empty() {
                return Ok(());
            }
            let trigger = FlowTrigger::InputDatasetFlow(FlowTriggerInputDatasetFlow {
                trigger_time: input_success_time,
                dataset_id: fk_dataset.dataset_id.clone(),
                flow_type: fk_dataset.flow_type,
                flow_id: flow.flow_id,
                flow_result: flow_result.clone(),
            });
            // For each, trigger needed flow
            for dependent_dataset_flow_plan in dependent_dataset_flow_plans {
                self.trigger_flow_common(
                    &dependent_dataset_flow_plan.flow_key,
                    trigger.clone(),
                    dependent_dataset_flow_plan.flow_trigger_context,
                    dependent_dataset_flow_plan.maybe_config_snapshot,
                )
                .await?;
            }

            Ok(())
        } else {
            unreachable!("Not expecting other types of flow keys than dataset");
        }
    }

    async fn make_downstream_dependencies_flow_plans(
        &self,
        fk_dataset: &FlowKeyDataset,
        maybe_config_snapshot: Option<&FlowConfigurationSnapshot>,
    ) -> Result<Vec<DownstreamDependencyFlowPlan>, InternalError> {
        // ToDo: extend dependency graph with possibility to fetch downstream
        // dependencies by owner
        use futures::StreamExt;
        let dependent_dataset_ids: Vec<_> = self
            .dependency_graph_service
            .get_downstream_dependencies(&fk_dataset.dataset_id)
            .await
            .int_err()?
            .collect()
            .await;

        let mut plans: Vec<DownstreamDependencyFlowPlan> = vec![];
        if dependent_dataset_ids.is_empty() {
            return Ok(plans);
        }

        match self.classify_dependent_trigger_type(fk_dataset.flow_type, maybe_config_snapshot) {
            DownstreamDependencyTriggerType::TriggerAllEnabledExecuteTransform => {
                for dataset_id in dependent_dataset_ids {
                    if let Some(transform_rule) = self
                        .flow_configuration_service
                        .try_get_dataset_transform_rule(
                            dataset_id.clone(),
                            DatasetFlowType::ExecuteTransform,
                        )
                        .await
                        .int_err()?
                    {
                        plans.push(DownstreamDependencyFlowPlan {
                            flow_key: FlowKeyDataset::new(
                                dataset_id,
                                DatasetFlowType::ExecuteTransform,
                            )
                            .into(),
                            flow_trigger_context: FlowTriggerContext::Batching(transform_rule),
                            maybe_config_snapshot: None,
                        });
                    };
                }
            }

            DownstreamDependencyTriggerType::TriggerOwnHardCompaction => {
                let dataset_owner_account_ids = self
                    .dataset_ownership_service
                    .get_dataset_owners(&fk_dataset.dataset_id)
                    .await?;

                for dependent_dataset_id in dependent_dataset_ids {
                    for owner_account_id in &dataset_owner_account_ids {
                        if self
                            .dataset_ownership_service
                            .is_dataset_owned_by(&dependent_dataset_id, owner_account_id)
                            .await?
                        {
                            plans.push(DownstreamDependencyFlowPlan {
                                flow_key: FlowKeyDataset::new(
                                    dependent_dataset_id.clone(),
                                    DatasetFlowType::HardCompaction,
                                )
                                .into(),
                                flow_trigger_context: FlowTriggerContext::Unconditional,
                                // Currently we trigger Hard compaction recursively only in keep
                                // metadata only mode
                                maybe_config_snapshot: Some(FlowConfigurationSnapshot::Compaction(
                                    CompactionRule::MetadataOnly(CompactionRuleMetadataOnly {
                                        recursive: true,
                                    }),
                                )),
                            });
                            break;
                        }
                    }
                }
            }

            DownstreamDependencyTriggerType::Empty => {}
        }

        Ok(plans)
    }

    fn classify_dependent_trigger_type(
        &self,
        dataset_flow_type: DatasetFlowType,
        maybe_config_snapshot: Option<&FlowConfigurationSnapshot>,
    ) -> DownstreamDependencyTriggerType {
        match dataset_flow_type {
            DatasetFlowType::Ingest | DatasetFlowType::ExecuteTransform => {
                DownstreamDependencyTriggerType::TriggerAllEnabledExecuteTransform
            }
            DatasetFlowType::HardCompaction => {
                if let Some(config_snapshot) = &maybe_config_snapshot
                    && let FlowConfigurationSnapshot::Compaction(compaction_rule) = config_snapshot
                {
                    if compaction_rule.recursive() {
                        DownstreamDependencyTriggerType::TriggerOwnHardCompaction
                    } else {
                        DownstreamDependencyTriggerType::Empty
                    }
                } else {
                    DownstreamDependencyTriggerType::TriggerAllEnabledExecuteTransform
                }
            }
            DatasetFlowType::Reset => {
                if let Some(config_snapshot) = &maybe_config_snapshot
                    && let FlowConfigurationSnapshot::Reset(reset_rule) = config_snapshot
                    && reset_rule.recursive
                {
                    DownstreamDependencyTriggerType::TriggerOwnHardCompaction
                } else {
                    DownstreamDependencyTriggerType::Empty
                }
            }
        }
    }

    pub(crate) async fn enqueue_scheduled_auto_polling_flow(
        &self,
        start_time: DateTime<Utc>,
        flow_key: &FlowKey,
        schedule: &Schedule,
    ) -> Result<FlowState, InternalError> {
        tracing::trace!(flow_key=?flow_key, schedule=?schedule, "Enqueuing scheduled flow");

        self.trigger_flow_common(
            flow_key,
            FlowTrigger::AutoPolling(FlowTriggerAutoPolling {
                trigger_time: start_time,
            }),
            FlowTriggerContext::Scheduled(schedule.clone()),
            None,
        )
        .await
    }

    pub(crate) async fn enqueue_auto_polling_flow_unconditionally(
        &self,
        start_time: DateTime<Utc>,
        flow_key: &FlowKey,
    ) -> Result<FlowState, InternalError> {
        // Very similar to manual trigger, but automatic reasons
        self.trigger_flow_common(
            flow_key,
            FlowTrigger::AutoPolling(FlowTriggerAutoPolling {
                trigger_time: start_time,
            }),
            FlowTriggerContext::Unconditional,
            None,
        )
        .await
    }

    pub(crate) async fn trigger_flow_common(
        &self,
        flow_key: &FlowKey,
        trigger: FlowTrigger,
        context: FlowTriggerContext,
        config_snapshot_maybe: Option<FlowConfigurationSnapshot>,
    ) -> Result<FlowState, InternalError> {
        // Query previous runs stats to determine activation time
        let flow_run_stats = self.flow_run_stats(flow_key).await?;

        // Flows may not be attempted more frequent than mandatory throttling period.
        // If flow has never run before, let it go without restriction.
        let trigger_time = trigger.trigger_time();
        let mut throttling_boundary_time =
            flow_run_stats.last_attempt_time.map_or(trigger_time, |t| {
                t + self.executor_config.mandatory_throttling_period
            });
        // It's also possible we are waiting for some start condition much longer..
        if throttling_boundary_time < trigger_time {
            throttling_boundary_time = trigger_time;
        }

        // Is a pending flow present for this config?
        match self.find_pending_flow(flow_key).await? {
            // Already pending flow
            Some(flow_id) => {
                // Load, merge triggers, update activation time
                let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
                    .await
                    .int_err()?;

                // Only merge unique triggers, ignore identical
                flow.add_trigger_if_unique(self.time_source.now(), trigger)
                    .int_err()?;

                match context {
                    FlowTriggerContext::Batching(transform_rule) => {
                        // Is this rule still waited?
                        if matches!(flow.start_condition, Some(FlowStartCondition::Batching(_))) {
                            self.evaluate_flow_transform_rule(
                                trigger_time,
                                &mut flow,
                                &transform_rule,
                                throttling_boundary_time,
                            )
                            .await?;
                        } else {
                            // Skip, the flow waits for something else
                        }
                    }
                    FlowTriggerContext::Scheduled(_) | FlowTriggerContext::Unconditional => {
                        // Evaluate throttling condition: is new time earlier than planned?
                        let maybe_planned_time =
                            self.find_planned_flow_activation_time(flow.flow_id);

                        // In case of batching condition and manual trigger,
                        // there is no planned time, but otherwise compare
                        if maybe_planned_time.is_none()
                            || maybe_planned_time
                                .is_some_and(|planned_time| throttling_boundary_time < planned_time)
                        {
                            // If so, enqueue the flow earlier
                            self.enqueue_flow(flow.flow_id, throttling_boundary_time)
                                .await?;

                            // Indicate throttling, if applied
                            if throttling_boundary_time > trigger_time {
                                self.indicate_throttling_activity(
                                    &mut flow,
                                    throttling_boundary_time,
                                    trigger_time,
                                )?;
                            }
                        }
                    }
                }

                flow.save(self.flow_event_store.as_ref()).await.int_err()?;
                Ok(flow.into())
            }

            // Otherwise, initiate a new flow, and enqueue it in the time wheel
            None => {
                // Initiate new flow
                let config_snapshot_maybe = if config_snapshot_maybe.is_some() {
                    config_snapshot_maybe
                } else {
                    self.flow_configuration_service
                        .try_get_config_snapshot_by_key(flow_key.clone())
                        .await
                        .int_err()?
                };
                let mut flow = self
                    .make_new_flow(
                        self.flow_event_store.as_ref(),
                        flow_key.clone(),
                        trigger,
                        config_snapshot_maybe,
                    )
                    .await?;

                match context {
                    FlowTriggerContext::Batching(transform_rule) => {
                        // Don't activate if batching condition not satisfied
                        self.evaluate_flow_transform_rule(
                            trigger_time,
                            &mut flow,
                            &transform_rule,
                            throttling_boundary_time,
                        )
                        .await?;
                    }
                    FlowTriggerContext::Scheduled(schedule) => {
                        // Next activation time depends on:
                        //  - last success time, if ever launched
                        //  - schedule, if defined
                        let naive_next_activation_time = schedule
                            .next_activation_time(trigger_time, flow_run_stats.last_success_time);

                        // Apply throttling boundary
                        let next_activation_time =
                            std::cmp::max(throttling_boundary_time, naive_next_activation_time);
                        self.enqueue_flow(flow.flow_id, next_activation_time)
                            .await?;

                        // Set throttling activity as start condition
                        if throttling_boundary_time > naive_next_activation_time {
                            self.indicate_throttling_activity(
                                &mut flow,
                                throttling_boundary_time,
                                naive_next_activation_time,
                            )?;
                        } else if naive_next_activation_time > trigger_time {
                            // Set waiting according to the schedule
                            flow.set_relevant_start_condition(
                                self.time_source.now(),
                                FlowStartCondition::Schedule(FlowStartConditionSchedule {
                                    wake_up_at: naive_next_activation_time,
                                }),
                            )
                            .int_err()?;
                        }
                    }
                    FlowTriggerContext::Unconditional => {
                        // Apply throttling boundary
                        let next_activation_time =
                            std::cmp::max(throttling_boundary_time, trigger_time);
                        self.enqueue_flow(flow.flow_id, next_activation_time)
                            .await?;

                        // Set throttling activity as start condition
                        if throttling_boundary_time > trigger_time {
                            self.indicate_throttling_activity(
                                &mut flow,
                                throttling_boundary_time,
                                trigger_time,
                            )?;
                        }
                    }
                }

                flow.save(self.flow_event_store.as_ref()).await.int_err()?;
                Ok(flow.into())
            }
        }
    }

    async fn evaluate_flow_transform_rule(
        &self,
        evaluation_time: DateTime<Utc>,
        flow: &mut Flow,
        transform_rule: &TransformRule,
        throttling_boundary_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        assert!(matches!(
            flow.flow_key.get_type(),
            AnyFlowType::Dataset(
                DatasetFlowType::ExecuteTransform | DatasetFlowType::HardCompaction
            )
        ));

        // TODO: it's likely assumed the accumulation is per each input separately, but
        // for now count overall number
        let mut accumulated_records_count = 0;
        let mut watermark_modified = false;
        let mut is_compacted = false;

        // Scan each accumulated trigger to decide
        for trigger in &flow.triggers {
            if let FlowTrigger::InputDatasetFlow(trigger) = trigger {
                match &trigger.flow_result {
                    FlowResult::Empty | FlowResult::DatasetReset(_) => {}
                    FlowResult::DatasetCompact(_) => {
                        is_compacted = true;
                    }
                    FlowResult::DatasetUpdate(update) => {
                        // Compute increment since the first trigger by this dataset.
                        // Note: there might have been multiple updates since that time.
                        // We are only recording the first trigger of particular dataset.
                        if let FlowResultDatasetUpdate::Changed(update_result) = update {
                            let increment = self
                                .dataset_changes_service
                                .get_increment_since(
                                    &trigger.dataset_id,
                                    update_result.old_head.as_ref(),
                                )
                                .await
                                .int_err()?;

                            accumulated_records_count += increment.num_records;
                            watermark_modified |= increment.updated_watermark.is_some();
                        }
                    }
                }
            }
        }

        // The timeout for batching will happen at:
        let batching_deadline =
            flow.primary_trigger().trigger_time() + *transform_rule.max_batching_interval();

        // Accumulated something if at least some input changed or watermark was touched
        let accumulated_something = accumulated_records_count > 0 || watermark_modified;

        // The condition is satisfied if
        //   - we crossed the number of new records thresholds
        //   - or waited long enough, assuming
        //      - there is at least some change of the inputs
        //      - watermark got touched
        let satisfied = accumulated_something
            && (accumulated_records_count >= transform_rule.min_records_to_await()
                || evaluation_time >= batching_deadline);

        // Set batching condition data, but only during the first rule evaluation.
        if !matches!(
            flow.start_condition.as_ref(),
            Some(FlowStartCondition::Batching(_))
        ) {
            flow.set_relevant_start_condition(
                self.time_source.now(),
                FlowStartCondition::Batching(FlowStartConditionBatching {
                    active_transform_rule: *transform_rule,
                    batching_deadline,
                }),
            )
            .int_err()?;
        }

        //  If we accumulated at least something (records or watermarks),
        //   the upper bound of potential finish time for batching is known
        if accumulated_something || is_compacted {
            // Finish immediately if satisfied, or not later than the deadline
            let batching_finish_time = if satisfied || is_compacted {
                evaluation_time
            } else {
                batching_deadline
            };

            // Throttling boundary correction
            let corrected_finish_time =
                std::cmp::max(batching_finish_time, throttling_boundary_time);

            let should_activate = match self.find_planned_flow_activation_time(flow.flow_id) {
                Some(activation_time) => activation_time > corrected_finish_time,
                None => true,
            };
            if should_activate {
                self.enqueue_flow(flow.flow_id, corrected_finish_time)
                    .await?;
            }

            // If batching is over, it's start condition is no longer valid.
            // However, set throttling condition, if it applies
            if (satisfied || is_compacted) && throttling_boundary_time > batching_finish_time {
                self.indicate_throttling_activity(
                    flow,
                    throttling_boundary_time,
                    batching_finish_time,
                )?;
            }
        }

        Ok(())
    }

    fn indicate_throttling_activity(
        &self,
        flow: &mut Flow,
        wake_up_at: DateTime<Utc>,
        shifted_from: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        flow.set_relevant_start_condition(
            self.time_source.now(),
            FlowStartCondition::Throttling(FlowStartConditionThrottling {
                interval: self.executor_config.mandatory_throttling_period,
                wake_up_at,
                shifted_from,
            }),
        )
        .int_err()?;
        Ok(())
    }

    async fn find_pending_flow(&self, flow_key: &FlowKey) -> Result<Option<FlowID>, InternalError> {
        self.flow_event_store.try_get_pending_flow(flow_key).await
    }

    #[inline]
    fn find_planned_flow_activation_time(&self, flow_id: FlowID) -> Option<DateTime<Utc>> {
        self.flow_timewheel_service
            .get_planned_flow_activation_time(flow_id)
    }

    async fn make_new_flow(
        &self,
        flow_event_store: &dyn FlowEventStore,
        flow_key: FlowKey,
        trigger: FlowTrigger,
        config_snapshot: Option<FlowConfigurationSnapshot>,
    ) -> Result<Flow, InternalError> {
        tracing::trace!(flow_key=?flow_key, trigger=?trigger, "Creating new flow");

        let flow = Flow::new(
            self.time_source.now(),
            flow_event_store.new_flow_id().await?,
            flow_key,
            trigger,
            config_snapshot,
        );

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

    async fn enqueue_flow(
        &self,
        flow_id: FlowID,
        activate_at: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
                FlowProgressMessage::enqueued(self.time_source.now(), flow_id, activate_at),
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
