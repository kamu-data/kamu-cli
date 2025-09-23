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

use crate::FlowSchedulingHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn FlowSystemEventProjector)]
pub struct FlowProcessStateProjector {
    flow_process_state_repository: Arc<dyn FlowProcessStateRepository>,
    flow_trigger_service: Arc<dyn FlowTriggerService>,
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

            // Tracking completed flows
            FlowEvent::Completed(e) => {
                // Now the flow is really finished, we can modify the projection
                assert_ne!(e.outcome, FlowOutcome::Aborted); // Aborted flows should not generate this event

                // Update process state. Among other values, this computes the latest ones for
                // "last_attempted_at" and "consecutive_failures"
                self.flow_process_state_repository
                    .apply_flow_result(
                        event_id,
                        flow_event.flow_binding(),
                        &e.outcome,
                        flow_event.event_time(),
                    )
                    .await
                    .int_err()?;

                // In case of a failure, trigger should make a decision about auto-stopping
                if e.outcome.is_failure() {
                    self.flow_trigger_service
                        .evaluate_trigger_on_failure(
                            flow_event.event_time(),
                            flow_event.flow_binding(),
                            e.outcome.is_unrecoverable_failure(),
                        )
                        .await?;
                }

                // In case of success:
                //  - schedule next flow immediately, if we had any late activation cause
                if e.outcome.is_success() && !e.late_activation_causes.is_empty() {
                    self.flow_scheduling_helper
                        .schedule_late_flow_activations(
                            flow_event.event_time(),
                            flow_event.flow_binding(),
                            &e.late_activation_causes,
                        )
                        .await?;
                }

                // Try to schedule auto-polling flow, if applicable.
                // We don't care whether we failed or succeeded,
                // that is determined with the stop policy in the trigger.
                self.flow_scheduling_helper
                    .try_schedule_auto_polling_flow_continuation_if_enabled(
                        flow_event.event_time(),
                        flow_event.flow_binding(),
                    )
                    .await?;
            }

            FlowEvent::Initiated(_)
            | FlowEvent::Aborted(_)
            | FlowEvent::ActivationCauseAdded(_)
            | FlowEvent::ConfigSnapshotModified(_)
            | FlowEvent::StartConditionUpdated(_)
            | FlowEvent::TaskScheduled(_)
            | FlowEvent::TaskRunning(_)
            | FlowEvent::TaskFinished(_) => {
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
