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
use kamu_flow_system::*;
use kamu_task_system as ts;
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;

use crate::MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
pub(crate) struct FlowSchedulingHelper {
    flow_event_store: Arc<dyn FlowEventStore>,
    flow_configuration_service: Arc<dyn FlowConfigurationService>,
    flow_trigger_service: Arc<dyn FlowTriggerService>,
    outbox: Arc<dyn Outbox>,
    time_source: Arc<dyn SystemTimeSource>,
    agent_config: Arc<FlowAgentConfig>,
}

impl FlowSchedulingHelper {
    pub(crate) async fn activate_flow_trigger(
        &self,
        target_catalog: &dill::Catalog,
        activation_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
        rule: FlowTriggerRule,
    ) -> Result<(), InternalError> {
        tracing::trace!(?flow_binding, ?rule, "Activating flow trigger");

        match &rule {
            // Reactive batching action
            FlowTriggerRule::Batching(batching_rule) => {
                let flow_controller =
                    get_flow_controller_from_catalog(target_catalog, &flow_binding.flow_type)?;
                flow_controller
                    .register_flow_sensor(flow_binding, activation_time, *batching_rule)
                    .await
                    .int_err()?;
            }

            // Scheduled action
            FlowTriggerRule::Schedule(schedule_rule) => {
                self.schedule_auto_polling_flow(activation_time, flow_binding, schedule_rule)
                    .await?;
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
        tracing::trace!(?flow_binding, ?schedule, "Enqueuing scheduled flow");

        self.trigger_flow_common(
            flow_binding,
            Some(FlowTriggerRule::Schedule(schedule.clone())),
            FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
                activation_time: start_time,
            }),
            None,
            None,
        )
        .await
    }

    pub(crate) async fn trigger_flow_common(
        &self,
        flow_binding: &FlowBinding,
        trigger_rule_maybe: Option<FlowTriggerRule>,
        activation_cause: FlowActivationCause,
        maybe_forced_flow_config_rule: Option<FlowConfigurationRule>,
        maybe_task_run_arguments: Option<ts::TaskRunArguments>,
    ) -> Result<FlowState, InternalError> {
        // Query previous runs stats to determine activation time
        let flow_run_stats = self
            .flow_event_store
            .get_flow_run_stats(flow_binding)
            .await?;

        // Flows may not be attempted more frequent than mandatory throttling period.
        // If flow has never run before, let it go without restriction.
        let activation_time = activation_cause.activation_time();
        let mut throttling_boundary_time = flow_run_stats
            .last_attempt_time
            .map_or(activation_time, |t| {
                t + self.agent_config.mandatory_throttling_period
            });
        // It's also possible we are waiting for some start condition much longer..
        if throttling_boundary_time < activation_time {
            throttling_boundary_time = activation_time;
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
                flow.add_activation_cause_if_unique(
                    self.time_source.now(),
                    activation_cause.clone(),
                )
                .int_err()?;

                match trigger_context {
                    FlowTriggerContext::Batching(batching_rule) => {
                        // Is this rule still waited?
                        if matches!(flow.start_condition, Some(FlowStartCondition::Batching(_))) {
                            self.evaluate_flow_batching_rule(
                                activation_time,
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
                            if throttling_boundary_time > activation_time {
                                self.indicate_throttling_activity(
                                    &mut flow,
                                    throttling_boundary_time,
                                    activation_time,
                                )?;
                            }

                            if let Some(config_snapshot) = maybe_forced_flow_config_rule {
                                flow.modify_config_snapshot(activation_time, config_snapshot)
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
                // Do we have a defined configuration for this flow?
                let maybe_flow_configuration = self
                    .flow_configuration_service
                    .find_configuration(flow_binding)
                    .await
                    .int_err()?;

                // Decide on configuration rule snapshot:
                //  - if forced, use it
                //  - if not, use the latest configuration rule, if any
                //  - if no configuration, use default rule
                let maybe_flow_config_rule_snapshot =
                    maybe_forced_flow_config_rule.map(Some).unwrap_or_else(|| {
                        maybe_flow_configuration
                            .as_ref()
                            .map(|config| Some(config.rule.clone()))
                            .unwrap_or_default()
                    });

                // Decide on retry policy:
                // - if configuration defines it, use it
                // - if not, use default retry policy for this flow type, if it's defined
                let retry_policy = maybe_flow_configuration
                    .and_then(|config| config.retry_policy)
                    .or_else(|| {
                        self.agent_config
                            .default_retry_policy_by_flow_type
                            .get(&flow_binding.flow_type)
                            .copied()
                    });

                // Initiate new flow
                let mut flow = self
                    .make_new_flow(
                        self.flow_event_store.as_ref(),
                        flow_binding.clone(),
                        &activation_cause,
                        maybe_flow_config_rule_snapshot,
                        retry_policy,
                        maybe_task_run_arguments,
                    )
                    .await?;

                match trigger_context {
                    FlowTriggerContext::Batching(batching_rule) => {
                        // Don't activate if batching condition not satisfied
                        self.evaluate_flow_batching_rule(
                            activation_time,
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
                        let naive_next_activation_time = schedule.next_activation_time(
                            activation_time,
                            flow_run_stats.last_success_time,
                        );

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
                        } else if naive_next_activation_time > activation_time {
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
                            std::cmp::max(throttling_boundary_time, activation_time);

                        // Set throttling activity as start condition
                        if throttling_boundary_time > activation_time {
                            self.indicate_throttling_activity(
                                &mut flow,
                                throttling_boundary_time,
                                activation_time,
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
        // TODO: it's likely assumed the accumulation is per each input separately, but
        // for now count overall number
        let mut accumulated_records_count = 0;
        let mut accumulated_something = false;
        let mut had_breaking_changes = false;

        // Scan each accumulated trigger to decide
        for activation_cause in &flow.activation_causes {
            if let FlowActivationCause::ResourceUpdate(activation_cause) = activation_cause {
                match activation_cause.changes {
                    ResourceChanges::NewData { records_added, .. } => {
                        accumulated_records_count += records_added;
                        accumulated_something = true;
                    }
                    ResourceChanges::Breaking => {
                        had_breaking_changes = true;
                        break;
                    }
                }
            }
        }

        // The timeout for batching will happen at:
        let batching_deadline = flow.primary_activation_cause().activation_time()
            + *batching_rule.max_batching_interval();

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
        if accumulated_something || had_breaking_changes {
            // Finish immediately if satisfied, or not later than the deadline
            let batching_finish_time = if satisfied || had_breaking_changes {
                evaluation_time
            } else {
                batching_deadline
            };

            // If batching is over, it's start condition is no longer valid.
            // However, set throttling condition, if it applies
            if (satisfied || had_breaking_changes)
                && throttling_boundary_time > batching_finish_time
            {
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
        activation_cause: &FlowActivationCause,
        maybe_config_rule_snapshot: Option<FlowConfigurationRule>,
        retry_policy: Option<RetryPolicy>,
        maybe_task_run_arguments: Option<ts::TaskRunArguments>,
    ) -> Result<Flow, InternalError> {
        tracing::trace!(?flow_binding, ?activation_cause, "Creating new flow");

        let flow = Flow::new(
            self.time_source.now(),
            flow_event_store.new_flow_id().await?,
            flow_binding,
            activation_cause.clone(),
            maybe_config_rule_snapshot,
            retry_policy,
            maybe_task_run_arguments,
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

#[derive(Debug, Eq, PartialEq)]
enum FlowTriggerContext {
    Unconditional,
    Scheduled(Schedule),
    Batching(BatchingRule),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
