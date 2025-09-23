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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
pub(crate) struct FlowSchedulingHelper {
    flow_event_store: Arc<dyn FlowEventStore>,
    flow_configuration_service: Arc<dyn FlowConfigurationService>,
    flow_process_state_query: Arc<dyn FlowProcessStateQuery>,
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
            // Reactive action
            FlowTriggerRule::Reactive(reactive_rule) => {
                let flow_controller =
                    get_flow_controller_from_catalog(target_catalog, &flow_binding.flow_type)?;
                flow_controller
                    .ensure_flow_sensor(flow_binding, activation_time, *reactive_rule)
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

    pub(crate) async fn schedule_late_flow_activations(
        &self,
        flow_success_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
        late_activation_causes: &[FlowActivationCause],
    ) -> Result<(), InternalError> {
        tracing::info!(
            ?flow_binding,
            late_activation_causes = late_activation_causes.len(),
            "Scheduling late flow activations"
        );

        // Schedule the next flow immediately
        self.trigger_flow_common(
            flow_success_time,
            flow_binding,
            None,
            late_activation_causes.to_vec(),
            None,
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn try_schedule_auto_polling_flow_continuation_if_enabled(
        &self,
        flow_finish_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
        trigger_state: &FlowTriggerState,
    ) -> Result<(), InternalError> {
        // Try locating an active schedule for this flow
        let maybe_active_schedule = if trigger_state.is_active() {
            trigger_state.try_get_schedule_rule()
        } else {
            None
        };

        // If there is an active schedule, schedule the next run immediately
        if let Some(active_schedule) = maybe_active_schedule {
            tracing::info!(
                ?flow_binding,
                "Scheduling flow continuation due to active schedule"
            );

            // Schedule the next flow immediately
            self.trigger_flow_common(
                flow_finish_time,
                flow_binding,
                Some(FlowTriggerRule::Schedule(active_schedule.clone())),
                vec![FlowActivationCause::AutoPolling(
                    FlowActivationCauseAutoPolling {
                        activation_time: flow_finish_time,
                    },
                )],
                None,
            )
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
            start_time,
            flow_binding,
            Some(FlowTriggerRule::Schedule(schedule.clone())),
            vec![FlowActivationCause::AutoPolling(
                FlowActivationCauseAutoPolling {
                    activation_time: start_time,
                },
            )],
            None,
        )
        .await
    }

    pub(crate) async fn trigger_flow_common(
        &self,
        trigger_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
        maybe_trigger_rule: Option<FlowTriggerRule>,
        activation_causes: Vec<FlowActivationCause>,
        maybe_forced_flow_config_rule: Option<FlowConfigurationRule>,
    ) -> Result<FlowState, InternalError> {
        assert!(
            !activation_causes.is_empty(),
            "At least one activation cause is required"
        );

        // Flows may not be attempted more frequent than mandatory throttling period.
        // If flow has never run before, let it go without restriction.
        let activation_time = activation_causes
            .first()
            .as_ref()
            .unwrap()
            .activation_time();

        // Query previous runs stats to determine last attempt time
        let maybe_flow_process_state = self
            .flow_process_state_query
            .try_get_process_state(flow_binding)
            .await?;
        let maybe_last_attempt_time: Option<DateTime<Utc>> = maybe_flow_process_state
            .as_ref()
            .and_then(FlowProcessState::last_attempt_at);

        let mut throttling_boundary_time = maybe_last_attempt_time.map_or(activation_time, |t| {
            t + self.agent_config.mandatory_throttling_period
        });
        // It's also possible we are waiting for some start condition much longer..
        if throttling_boundary_time < activation_time {
            throttling_boundary_time = activation_time;
        }

        let trigger_context = match &maybe_trigger_rule {
            None => FlowTriggerContext::Unconditional,
            Some(rule) => match rule {
                FlowTriggerRule::Schedule(schedule) => {
                    FlowTriggerContext::Scheduled(schedule.clone())
                }
                FlowTriggerRule::Reactive(reactive) => FlowTriggerContext::Reactive(*reactive),
            },
        };

        // Is a pending flow present for this config?
        let (mut flow, maybe_next_activation_time) =
            match self.find_pending_flow(flow_binding).await? {
                // Already pending flow
                Some(flow_id) => {
                    // Load, merge triggers, update activation time
                    let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
                        .await
                        .int_err()?;

                    // Only merge unique triggers, ignore identical
                    for activation_cause in activation_causes {
                        flow.add_activation_cause_if_unique(trigger_time, activation_cause)
                            .int_err()?;
                    }

                    let maybe_next_activation_time = match trigger_context {
                        FlowTriggerContext::Reactive(reactive_rule) => {
                            // Is this rule still waited?
                            if matches!(flow.start_condition, Some(FlowStartCondition::Reactive(_)))
                            {
                                self.evaluate_flow_reactive_rule(
                                    activation_time,
                                    &mut flow,
                                    &reactive_rule,
                                    throttling_boundary_time,
                                )?
                            } else {
                                // Skip, the flow waits for something else
                                None
                            }
                        }
                        FlowTriggerContext::Scheduled(_) | FlowTriggerContext::Unconditional => {
                            // Evaluate throttling condition: is new time earlier than planned?
                            // In case of reactive condition and manual trigger,
                            // there is no planned time, but otherwise compare
                            if flow.timing.scheduled_for_activation_at.is_none()
                                || flow.timing.scheduled_for_activation_at.is_some_and(
                                    |planned_time| throttling_boundary_time < planned_time,
                                )
                            {
                                // Indicate throttling, if applied
                                if throttling_boundary_time > activation_time {
                                    self.indicate_throttling_activity(
                                        trigger_time,
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
                                Some(throttling_boundary_time)
                            } else {
                                None
                            }
                        }
                    };

                    (flow, maybe_next_activation_time)
                }

                // Otherwise, initiate a new flow and schedule it for activation
                None => {
                    // Initiate new flow
                    let mut flow = self
                        .make_new_flow(
                            trigger_time,
                            self.flow_event_store.as_ref(),
                            flow_binding.clone(),
                            activation_causes,
                            maybe_forced_flow_config_rule,
                        )
                        .await?;

                    // Decide when to activate it
                    let maybe_next_activation_time = match trigger_context {
                        FlowTriggerContext::Reactive(reactive_rule) => {
                            // Don't activate if reactive condition not satisfied
                            self.evaluate_flow_reactive_rule(
                                activation_time,
                                &mut flow,
                                &reactive_rule,
                                throttling_boundary_time,
                            )?
                        }

                        FlowTriggerContext::Scheduled(schedule) => {
                            // Next activation time depends on:
                            //  - last attempt time, if ever launched
                            //  - schedule

                            let naive_next_activation_time = schedule
                                .next_activation_time(activation_time, maybe_last_attempt_time);

                            // Apply throttling boundary
                            let next_activation_time =
                                std::cmp::max(throttling_boundary_time, naive_next_activation_time);

                            // Set throttling activity as start condition
                            if throttling_boundary_time > naive_next_activation_time {
                                self.indicate_throttling_activity(
                                    trigger_time,
                                    &mut flow,
                                    throttling_boundary_time,
                                    naive_next_activation_time,
                                )?;
                            } else if naive_next_activation_time > activation_time {
                                // Set waiting according to the schedule
                                flow.set_relevant_start_condition(
                                    trigger_time,
                                    FlowStartCondition::Schedule(FlowStartConditionSchedule {
                                        wake_up_at: naive_next_activation_time,
                                    }),
                                )
                                .int_err()?;
                            }

                            Some(next_activation_time)
                        }

                        FlowTriggerContext::Unconditional => {
                            // Apply throttling boundary
                            let next_activation_time =
                                std::cmp::max(throttling_boundary_time, activation_time);

                            // Set throttling activity as start condition
                            if throttling_boundary_time > activation_time {
                                self.indicate_throttling_activity(
                                    trigger_time,
                                    &mut flow,
                                    throttling_boundary_time,
                                    activation_time,
                                )?;
                            }

                            Some(next_activation_time)
                        }
                    };

                    (flow, maybe_next_activation_time)
                }
            };

        // Schedule for activation, if needed
        if let Some(next_activation_time) = maybe_next_activation_time
            && flow.timing.scheduled_for_activation_at != maybe_next_activation_time
        {
            flow.schedule_for_activation(trigger_time, next_activation_time)
                .int_err()?;
            flow.save(self.flow_event_store.as_ref()).await.int_err()?;
        } else {
            // Save flow state, even if not scheduled
            flow.save(self.flow_event_store.as_ref()).await.int_err()?;
        }

        Ok(flow.into())
    }

    fn evaluate_flow_reactive_rule(
        &self,
        evaluation_time: DateTime<Utc>,
        flow: &mut Flow,
        reactive_rule: &ReactiveRule,
        throttling_boundary_time: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, InternalError> {
        // TODO: it's likely assumed the accumulation is per each input separately, but
        // for now count overall number
        let mut accumulated_records_count = 0;
        let mut accumulated_something = false;
        let mut had_breaking_changes = false;

        // Scan each accumulated activation case to decide
        assert!(flow.late_activation_causes.is_empty());
        for activation_cause in &flow.activation_causes {
            if let FlowActivationCause::ResourceUpdate(activation_cause) = activation_cause {
                match activation_cause.changes {
                    ResourceChanges::NewData(changes) => {
                        accumulated_records_count += changes.records_added;
                        accumulated_something = true;
                    }
                    ResourceChanges::Breaking => {
                        had_breaking_changes = true;
                        break;
                    }
                }
            }
        }

        // Extract batching rule
        let batching_rule = &reactive_rule.for_new_data;

        // The timeout for batching will happen at:
        let primary_activation_time = flow.primary_activation_cause().activation_time();
        let max_batching_interval = batching_rule.max_batching_interval();
        let batching_deadline = primary_activation_time + max_batching_interval;

        // The condition is satisfied if
        //   - we crossed the number of new records thresholds
        //   - or waited long enough, assuming
        //      - there is at least some change of the inputs
        //      - watermark got touched
        let satisfied = accumulated_something
            && (accumulated_records_count >= batching_rule.min_records_to_await()
                || evaluation_time >= batching_deadline);

        // Set reactive condition data always, so that progress can be tracked
        flow.set_relevant_start_condition(
            evaluation_time,
            FlowStartCondition::Reactive(FlowStartConditionReactive {
                active_rule: *reactive_rule,
                batching_deadline,
                last_activation_cause_index: flow.activation_causes.len() - 1,
            }),
        )
        .int_err()?;

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
                    evaluation_time,
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
                return Ok(Some(corrected_finish_time));
            }
        }

        Ok(None)
    }

    fn indicate_throttling_activity(
        &self,
        trigger_time: DateTime<Utc>,
        flow: &mut Flow,
        wake_up_at: DateTime<Utc>,
        shifted_from: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        flow.set_relevant_start_condition(
            trigger_time,
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
        trigger_time: DateTime<Utc>,
        flow_event_store: &dyn FlowEventStore,
        flow_binding: FlowBinding,
        mut activation_causes: Vec<FlowActivationCause>,
        maybe_forced_flow_config_rule: Option<FlowConfigurationRule>,
    ) -> Result<Flow, InternalError> {
        tracing::trace!(?flow_binding, ?activation_causes, "Creating new flow");

        assert!(
            !activation_causes.is_empty(),
            "At least one activation cause is required"
        );

        let other_causes = activation_causes.split_off(1);
        let first_cause = activation_causes.into_iter().next().unwrap();

        // Do we have a defined configuration for this flow?
        let maybe_flow_configuration = self
            .flow_configuration_service
            .find_configuration(&flow_binding)
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

        // Create the flow
        let mut flow = Flow::new(
            trigger_time,
            flow_event_store.new_flow_id().await?,
            flow_binding,
            first_cause,
            maybe_flow_config_rule_snapshot,
            retry_policy,
        );

        // Register additional activation causes
        for cause in other_causes {
            flow.add_activation_cause_if_unique(trigger_time, cause)
                .int_err()?;
        }

        Ok(flow)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
enum FlowTriggerContext {
    Unconditional,
    Scheduled(Schedule),
    Reactive(ReactiveRule),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
