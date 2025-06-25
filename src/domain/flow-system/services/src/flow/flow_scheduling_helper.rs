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
use kamu_datasets::DatasetIncrementQueryService;
use kamu_flow_system::*;
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;

use super::FlowTriggerContext;
use crate::MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
pub(crate) struct FlowSchedulingHelper {
    flow_event_store: Arc<dyn FlowEventStore>,
    flow_configuration_service: Arc<dyn FlowConfigurationService>,
    flow_trigger_service: Arc<dyn FlowTriggerService>,
    flow_support_service: Arc<dyn FlowSupportService>,
    outbox: Arc<dyn Outbox>,
    dataset_increment_query_service: Arc<dyn DatasetIncrementQueryService>,
    time_source: Arc<dyn SystemTimeSource>,
    agent_config: Arc<FlowAgentConfig>,
}

impl FlowSchedulingHelper {
    pub(crate) async fn activate_flow_trigger(
        &self,
        start_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
        rule: FlowTriggerRule,
    ) -> Result<(), InternalError> {
        tracing::trace!(flow_key = ?flow_binding, rule = ?rule, "Activating flow trigger");

        match &flow_binding.scope {
            FlowScope::Dataset { .. } => match &rule {
                FlowTriggerRule::Batching(batching_rule) => {
                    let flow_run_stats = self
                        .flow_event_store
                        .get_flow_run_stats(flow_binding)
                        .await?;
                    if flow_run_stats.last_success_time.is_some() {
                        self.trigger_flow_common(
                            flow_binding,
                            Some(FlowTriggerRule::Batching(*batching_rule)),
                            FlowTriggerInstance::AutoPolling(FlowTriggerAutoPolling {
                                trigger_time: start_time,
                            }),
                            None,
                        )
                        .await?;
                    } else {
                        self.schedule_auto_polling_flow_unconditionally(start_time, flow_binding)
                            .await?;
                    }
                }
                FlowTriggerRule::Schedule(schedule_rule) => {
                    self.schedule_auto_polling_flow(start_time, flow_binding, schedule_rule)
                        .await?;
                }
            },
            FlowScope::System => {
                if let FlowTriggerRule::Schedule(schedule) = &rule {
                    self.schedule_auto_polling_flow(start_time, flow_binding, schedule)
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

    pub(crate) async fn try_schedule_auto_polling_flow_if_enabled(
        &self,
        start_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
    ) -> Result<(), InternalError> {
        let maybe_active_schedule = self
            .flow_trigger_service
            .try_get_flow_schedule_rule(flow_binding)
            .await
            .int_err()?;

        if let Some(active_schedule) = maybe_active_schedule {
            self.schedule_auto_polling_flow(start_time, flow_binding, &active_schedule)
                .await?;
        }

        Ok(())
    }

    pub(crate) async fn schedule_auto_polling_flow(
        &self,
        start_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
        schedule: &Schedule,
    ) -> Result<FlowState, InternalError> {
        tracing::trace!(flow_key = ?flow_binding, schedule = ?schedule, "Enqueuing scheduled flow");

        self.trigger_flow_common(
            flow_binding,
            Some(FlowTriggerRule::Schedule(schedule.clone())),
            FlowTriggerInstance::AutoPolling(FlowTriggerAutoPolling {
                trigger_time: start_time,
            }),
            None,
        )
        .await
    }

    pub(crate) async fn schedule_auto_polling_flow_unconditionally(
        &self,
        start_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
    ) -> Result<FlowState, InternalError> {
        // Very similar to manual trigger, but automatic reasons
        self.trigger_flow_common(
            flow_binding,
            None,
            FlowTriggerInstance::AutoPolling(FlowTriggerAutoPolling {
                trigger_time: start_time,
            }),
            None,
        )
        .await
    }

    pub(crate) async fn trigger_flow_common(
        &self,
        flow_binding: &FlowBinding,
        trigger_rule_maybe: Option<FlowTriggerRule>,
        trigger_type: FlowTriggerInstance,
        maybe_flow_config_snapshot: Option<FlowConfigurationRule>,
    ) -> Result<FlowState, InternalError> {
        // Query previous runs stats to determine activation time
        let flow_run_stats = self
            .flow_event_store
            .get_flow_run_stats(flow_binding)
            .await?;

        // Flows may not be attempted more frequent than mandatory throttling period.
        // If flow has never run before, let it go without restriction.
        let trigger_time = trigger_type.trigger_time();
        let mut throttling_boundary_time =
            flow_run_stats.last_attempt_time.map_or(trigger_time, |t| {
                t + self.agent_config.mandatory_throttling_period
            });
        // It's also possible we are waiting for some start condition much longer..
        if throttling_boundary_time < trigger_time {
            throttling_boundary_time = trigger_time;
        }

        let trigger_context = match &trigger_rule_maybe {
            None => FlowTriggerContext::Unconditional,
            Some(rule) => match rule {
                FlowTriggerRule::Schedule(schedule) => {
                    FlowTriggerContext::Scheduled(schedule.clone())
                }
                FlowTriggerRule::Batching(batching) => FlowTriggerContext::Batching(*batching),
            },
        };

        // Is a pending flow present for this config?
        match self.find_pending_flow(flow_binding).await? {
            // Already pending flow
            Some(flow_id) => {
                // Load, merge triggers, update activation time
                let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
                    .await
                    .int_err()?;

                // Only merge unique triggers, ignore identical
                flow.add_trigger_if_unique(self.time_source.now(), trigger_type.clone())
                    .int_err()?;

                match trigger_context {
                    FlowTriggerContext::Batching(batching_rule) => {
                        // Is this rule still waited?
                        if matches!(flow.start_condition, Some(FlowStartCondition::Batching(_))) {
                            self.evaluate_flow_batching_rule(
                                trigger_time,
                                &mut flow,
                                &batching_rule,
                                throttling_boundary_time,
                            )
                            .await?;
                        } else {
                            // Skip, the flow waits for something else
                        }
                    }
                    FlowTriggerContext::Scheduled(_) | FlowTriggerContext::Unconditional => {
                        // Evaluate throttling condition: is new time earlier than planned?
                        // In case of batching condition and manual trigger,
                        // there is no planned time, but otherwise compare
                        if flow.timing.scheduled_for_activation_at.is_none()
                            || flow
                                .timing
                                .scheduled_for_activation_at
                                .is_some_and(|planned_time| throttling_boundary_time < planned_time)
                        {
                            // Indicate throttling, if applied
                            if throttling_boundary_time > trigger_time {
                                self.indicate_throttling_activity(
                                    &mut flow,
                                    throttling_boundary_time,
                                    trigger_time,
                                )?;
                            }

                            if let Some(config_snapshot) = maybe_flow_config_snapshot {
                                flow.modify_config_snapshot(trigger_time, config_snapshot)
                                    .int_err()?;
                            }

                            // Schedule the flow earlier than previously planned
                            self.schedule_flow_for_activation(&mut flow, throttling_boundary_time)
                                .await?;
                        }
                    }
                }

                flow.save(self.flow_event_store.as_ref()).await.int_err()?;
                Ok(flow.into())
            }

            // Otherwise, initiate a new flow and schedule it for activation
            None => {
                // Initiate new flow
                let maybe_flow_config_snapshot = if maybe_flow_config_snapshot.is_some() {
                    maybe_flow_config_snapshot
                } else {
                    self.flow_configuration_service
                        .try_get_config_snapshot_by_binding(flow_binding)
                        .await
                        .int_err()?
                };
                let mut flow = self
                    .make_new_flow(
                        self.flow_event_store.as_ref(),
                        flow_binding.clone(),
                        &trigger_type,
                        maybe_flow_config_snapshot,
                    )
                    .await?;

                match trigger_context {
                    FlowTriggerContext::Batching(batching_rule) => {
                        // Don't activate if batching condition not satisfied
                        self.evaluate_flow_batching_rule(
                            trigger_time,
                            &mut flow,
                            &batching_rule,
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

                        // Schedule flow for the decided moment
                        self.schedule_flow_for_activation(&mut flow, next_activation_time)
                            .await?;
                    }
                    FlowTriggerContext::Unconditional => {
                        // Apply throttling boundary
                        let next_activation_time =
                            std::cmp::max(throttling_boundary_time, trigger_time);

                        // Set throttling activity as start condition
                        if throttling_boundary_time > trigger_time {
                            self.indicate_throttling_activity(
                                &mut flow,
                                throttling_boundary_time,
                                trigger_time,
                            )?;
                        }

                        // Schedule flow for the decided moment
                        self.schedule_flow_for_activation(&mut flow, next_activation_time)
                            .await?;
                    }
                }

                flow.save(self.flow_event_store.as_ref()).await.int_err()?;
                Ok(flow.into())
            }
        }
    }

    #[allow(unused_mut)]
    async fn evaluate_flow_batching_rule(
        &self,
        evaluation_time: DateTime<Utc>,
        flow: &mut Flow,
        batching_rule: &BatchingRule,
        throttling_boundary_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        /*assert!(matches!(
            flow.flow_key.get_type(),
            AnyFlowType::Dataset(
                DatasetFlowType::ExecuteTransform | DatasetFlowType::HardCompaction
            )
        )); */

        // TODO: it's likely assumed the accumulation is per each input separately, but
        // for now count overall number
        let mut accumulated_records_count = 0;
        let mut accumulated_something = false;
        let mut is_compacted = false;

        // Scan each accumulated trigger to decide
        for trigger in &flow.triggers {
            match trigger {
                FlowTriggerInstance::InputDatasetFlow(trigger_type) => {
                    let interpretation = self
                        .flow_support_service
                        .interpret_input_dataset_result(
                            &trigger_type.dataset_id,
                            &trigger_type.task_result,
                        )
                        .await?;

                    if interpretation.was_compacted {
                        is_compacted = true;
                    } else {
                        accumulated_records_count += interpretation.new_records_count;
                        accumulated_something = true;
                    }
                }
                FlowTriggerInstance::Push(trigger_type) => {
                    let old_head_maybe = match trigger_type.result {
                        DatasetPushResult::HttpIngest(ref update_result) => {
                            update_result.old_head_maybe.as_ref()
                        }
                        DatasetPushResult::SmtpSync(ref update_result) => {
                            if update_result.is_force {
                                // Force sync currently does not supported as a trigger for
                                // dependent datasets
                                return Ok(());
                            }
                            update_result.old_head_maybe.as_ref()
                        }
                    };

                    let increment = self
                        .dataset_increment_query_service
                        .get_increment_since(&trigger_type.dataset_id, old_head_maybe)
                        .await
                        .int_err()?;

                    accumulated_records_count += increment.num_records;
                    accumulated_something = true;
                }
                _ => {}
            }
        }

        // The timeout for batching will happen at:
        let batching_deadline =
            flow.primary_trigger().trigger_time() + *batching_rule.max_batching_interval();

        // The condition is satisfied if
        //   - we crossed the number of new records thresholds
        //   - or waited long enough, assuming
        //      - there is at least some change of the inputs
        //      - watermark got touched
        let satisfied = accumulated_something
            && (accumulated_records_count >= batching_rule.min_records_to_await()
                || evaluation_time >= batching_deadline);

        // Set batching condition data, but only during the first rule evaluation.
        if !matches!(
            flow.start_condition.as_ref(),
            Some(FlowStartCondition::Batching(_))
        ) {
            flow.set_relevant_start_condition(
                self.time_source.now(),
                FlowStartCondition::Batching(FlowStartConditionBatching {
                    active_batching_rule: *batching_rule,
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

            // If batching is over, it's start condition is no longer valid.
            // However, set throttling condition, if it applies
            if (satisfied || is_compacted) && throttling_boundary_time > batching_finish_time {
                self.indicate_throttling_activity(
                    flow,
                    throttling_boundary_time,
                    batching_finish_time,
                )?;
            }

            // Throttling boundary correction
            let corrected_finish_time =
                std::cmp::max(batching_finish_time, throttling_boundary_time);

            let should_activate = match flow.timing.scheduled_for_activation_at {
                Some(scheduled_for_activation_at) => {
                    scheduled_for_activation_at > corrected_finish_time
                }
                None => true,
            };
            if should_activate {
                self.schedule_flow_for_activation(flow, corrected_finish_time)
                    .await?;
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
                interval: self.agent_config.mandatory_throttling_period,
                wake_up_at,
                shifted_from,
            }),
        )
        .int_err()?;
        Ok(())
    }

    async fn find_pending_flow(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowID>, InternalError> {
        self.flow_event_store
            .try_get_pending_flow(flow_binding)
            .await
    }

    async fn make_new_flow(
        &self,
        flow_event_store: &dyn FlowEventStore,
        flow_binding: FlowBinding,
        trigger_type: &FlowTriggerInstance,
        config_snapshot: Option<FlowConfigurationRule>,
    ) -> Result<Flow, InternalError> {
        tracing::trace!(flow_key = ?flow_binding, trigger = ?trigger_type, "Creating new flow");

        let flow = Flow::new(
            self.time_source.now(),
            flow_event_store.new_flow_id().await?,
            flow_binding,
            trigger_type.clone(),
            config_snapshot,
            RetryPolicy::default(), // TODO: configuration
        );

        Ok(flow)
    }

    async fn schedule_flow_for_activation(
        &self,
        flow: &mut Flow,
        activate_at: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        flow.schedule_for_activation(self.time_source.now(), activate_at)
            .int_err()?;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
                FlowProgressMessage::scheduled(self.time_source.now(), flow.flow_id, activate_at),
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
