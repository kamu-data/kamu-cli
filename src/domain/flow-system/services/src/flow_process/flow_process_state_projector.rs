// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_flow_system::*;
use kamu_task_system::TaskOutcome;

use crate::FlowSchedulingHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn FlowSystemEventProjector)]
pub struct FlowProcessStateProjector {
    flow_process_state_repository: Arc<dyn FlowProcessStateRepository>,
    flow_trigger_service: Arc<dyn FlowTriggerService>,
    flow_event_store: Arc<dyn FlowEventStore>,
    flow_scheduling_helper: Arc<FlowSchedulingHelper>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowProcessStateProjector {
    async fn process_flow_trigger_event(
        &self,
        event_id: EventID,
        trigger_event: FlowTriggerEvent,
    ) -> Result<(), InternalError> {
        match trigger_event {
            FlowTriggerEvent::Created(e) => {
                self.flow_process_state_repository
                    .upsert_process_state_on_trigger_event(
                        event_id,
                        e.flow_binding,
                        e.paused,
                        e.stop_policy,
                    )
                    .await
                    .int_err()?;
            }

            FlowTriggerEvent::Modified(e) => {
                self.flow_process_state_repository
                    .upsert_process_state_on_trigger_event(
                        event_id,
                        e.flow_binding,
                        e.paused,
                        e.stop_policy,
                    )
                    .await
                    .int_err()?;
            }

            FlowTriggerEvent::AutoStopped(_) => {
                // Ignored, brings no new information to this projection,
                // since it's the one that initiates this event in the first
                // place.
            }

            FlowTriggerEvent::ScopeRemoved(e) => {
                // Idempotent delete
                self.flow_process_state_repository
                    .delete_process_states_by_scope(&e.flow_binding.scope)
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }

    async fn process_flow_event(
        &self,
        event_id: EventID,
        flow_event: FlowEvent,
    ) -> Result<(), InternalError> {
        match &flow_event {
            // Tracking "next activation" time for scheduled flows
            FlowEvent::ScheduledForActivation(e) => {
                self.flow_process_state_repository
                    .on_flow_scheduled(
                        event_id,
                        flow_event.flow_binding(),
                        e.scheduled_for_activation_at,
                    )
                    .await
                    .int_err()?;
            }

            // Tracking completed tasks
            FlowEvent::TaskFinished(e) => {
                // This must be the last task in the flow to consider the flow finished.
                // Make sure we ignore intermediate task completions that will be retried.
                let last_task_in_flow = match e.task_outcome {
                    TaskOutcome::Success(_) | TaskOutcome::Cancelled => true,
                    TaskOutcome::Failed(_) => e.next_attempt_at.is_none(),
                };
                if last_task_in_flow {
                    // Now the flow is really finished, we can modify the projection
                    let is_success = match e.task_outcome {
                        TaskOutcome::Success(_) => true,
                        TaskOutcome::Failed(_) => false,
                        TaskOutcome::Cancelled => return Ok(()), // Ignore cancelled flows
                    };

                    // Update process state. Among other values, this computes the latest ones for
                    // "last_attempted_at" and "consecutive_failures"
                    self.flow_process_state_repository
                        .apply_flow_result(
                            event_id,
                            flow_event.flow_binding(),
                            is_success,
                            flow_event.event_time(),
                        )
                        .await
                        .int_err()?;

                    // In case of a failure, trigger should make a decision about auto-stopping
                    if !is_success {
                        self.flow_trigger_service
                            .evaluate_trigger_on_failure(
                                flow_event.event_time(),
                                flow_event.flow_binding(),
                                !e.task_outcome.is_recoverable_failure(),
                            )
                            .await?;
                    }

                    // Recover the flow to check if we need to schedule next activation
                    let flow = Flow::load(flow_event.flow_id(), self.flow_event_store.as_ref())
                        .await
                        .int_err()?;

                    // In case of success:
                    //  - schedule next flow immediately, if we had any late activation cause
                    if is_success {
                        self.flow_scheduling_helper
                            .try_schedule_late_flow_activations(flow_event.event_time(), &flow)
                            .await?;
                    }

                    // Try to schedule auto-polling flow, if applicable.
                    // We don't care whether we failed or succeeded,
                    // that is determined with the stop policy in the trigger.
                    self.flow_scheduling_helper
                        .try_schedule_auto_polling_flow_continuation_if_enabled(
                            flow_event.event_time(),
                            &flow,
                        )
                        .await?;
                }
            }

            FlowEvent::Initiated(_)
            | FlowEvent::Aborted(_)
            | FlowEvent::ActivationCauseAdded(_)
            | FlowEvent::ConfigSnapshotModified(_)
            | FlowEvent::StartConditionUpdated(_)
            | FlowEvent::TaskScheduled(_)
            | FlowEvent::TaskRunning(_) => {
                // Ignored
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventProjector for FlowProcessStateProjector {
    fn name(&self) -> &'static str {
        "dev.kamu.domain.flow-system.FlowProcessStateProjector"
    }

    async fn apply(&self, event: &FlowSystemEvent) -> Result<(), InternalError> {
        tracing::debug!(
            event_id = %event.event_id,
            source_type = ?event.source_type,
            source_event_id = %event.source_event_id,
            "Applying flow system event"
        );

        match event.source_type {
            FlowSystemEventSourceType::FlowConfiguration => { /* ignored */ }

            FlowSystemEventSourceType::FlowTrigger => {
                let trigger_event: FlowTriggerEvent =
                    serde_json::from_value(event.payload.clone()).int_err()?;
                self.process_flow_trigger_event(event.event_id, trigger_event)
                    .await?;
            }

            FlowSystemEventSourceType::Flow => {
                let flow_event: FlowEvent =
                    serde_json::from_value(event.payload.clone()).int_err()?;
                self.process_flow_event(event.event_id, flow_event).await?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
