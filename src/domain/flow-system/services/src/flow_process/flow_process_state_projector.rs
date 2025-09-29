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
use messaging_outbox::{Outbox, OutboxExt};

use crate::FlowSchedulingHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn FlowSystemEventProjector)]
pub struct FlowProcessStateProjector {
    flow_process_state_repository: Arc<dyn FlowProcessStateRepository>,
    flow_trigger_service: Arc<dyn FlowTriggerService>,
    flow_scheduling_helper: Arc<FlowSchedulingHelper>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowProcessStateProjector {
    async fn process_flow_trigger_event(
        &self,
        event_id: EventID,
        trigger_event: FlowTriggerEvent,
    ) -> Result<(), InternalError> {
        let maybe_new_process_state = match &trigger_event {
            FlowTriggerEvent::Created(e) => Some(
                self.flow_process_state_repository
                    .upsert_process_state_on_trigger_event(
                        event_id,
                        e.flow_binding.clone(),
                        e.paused,
                        e.stop_policy,
                    )
                    .await
                    .int_err()?,
            ),

            FlowTriggerEvent::Modified(e) => Some(
                self.flow_process_state_repository
                    .upsert_process_state_on_trigger_event(
                        event_id,
                        e.flow_binding.clone(),
                        e.paused,
                        e.stop_policy,
                    )
                    .await
                    .int_err()?,
            ),

            FlowTriggerEvent::AutoStopped(_) => {
                // Ignored, brings no new information to this projection,
                // since it's the one that initiates this event in the first
                // place.
                None
            }

            FlowTriggerEvent::ScopeRemoved(e) => {
                // Idempotent delete
                self.flow_process_state_repository
                    .delete_process_states_by_scope(&e.flow_binding.scope)
                    .await
                    .int_err()?;
                None
            }
        };

        // Apply any pending events that were generated as part of the state update
        if let Some(mut new_process_state) = maybe_new_process_state {
            self.apply_flow_process_events(
                trigger_event.flow_binding(),
                new_process_state.take_pending_events(),
            )
            .await?;
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
                let mut new_process_state = self
                    .flow_process_state_repository
                    .on_flow_scheduled(
                        event_id,
                        flow_event.flow_binding(),
                        e.scheduled_for_activation_at,
                    )
                    .await
                    .int_err()?;

                // Apply any pending events that were generated as part of the state update
                self.apply_flow_process_events(
                    flow_event.flow_binding(),
                    new_process_state.take_pending_events(),
                )
                .await?;
            }

            // Tracking completed flows
            FlowEvent::Completed(e) => {
                // Now the flow is really finished, we can modify the projection
                assert_ne!(e.outcome, FlowOutcome::Aborted); // Aborted flows should not generate this event

                // Update process state. Among other values, this computes the latest ones for
                // "last_attempted_at" and "consecutive_failures"
                let mut new_process_state = self
                    .flow_process_state_repository
                    .apply_flow_result(
                        event_id,
                        flow_event.flow_binding(),
                        &e.outcome,
                        flow_event.event_time(),
                    )
                    .await
                    .int_err()?;

                // If it's a failure outcome, emit message to external systems
                if let FlowOutcome::Failed(error) = &e.outcome {
                    self.outbox
                        .post_message(
                            MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR,
                            FlowProcessLifecycleMessage::failure_registered(
                                flow_event.event_time(),
                                flow_event.flow_binding().clone(),
                                e.flow_id,
                                error.clone(),
                                new_process_state.consecutive_failures(),
                            ),
                        )
                        .await?;
                }

                // Apply any pending events that were generated as part of the state update
                let impact = self
                    .apply_flow_process_events(
                        flow_event.flow_binding(),
                        new_process_state.take_pending_events(),
                    )
                    .await?;

                // There might be late flow activations.
                // Consider scheduling new flow to handle those, if:
                // - the last flow attempt succeeded (event if it was originally manually
                //   launched)
                // - the trigger is still active after processing the latest events
                if e.outcome.is_success()
                    || (e.outcome.is_recoverable_failure() && impact.is_trigger_still_active())
                {
                    // Schedule next flow immediately, if we had any late activation cause
                    if !e.late_activation_causes.is_empty() {
                        self.flow_scheduling_helper
                            .schedule_late_flow_activations(
                                flow_event.event_time(),
                                flow_event.flow_binding(),
                                &e.late_activation_causes,
                            )
                            .await?;
                    }
                }

                // Try to schedule next auto-polling flow, if applicable.
                if let Some(trigger_state) = &impact.maybe_latest_trigger_state {
                    // We don't care whether we failed or succeeded,
                    // as long as the trigger is still active.
                    self.flow_scheduling_helper
                        .try_schedule_auto_polling_flow_continuation_if_enabled(
                            flow_event.event_time(),
                            flow_event.flow_binding(),
                            trigger_state,
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
            | FlowEvent::TaskRunning(_)
            | FlowEvent::TaskFinished(_) => {
                // Ignored
            }
        }

        Ok(())
    }

    async fn apply_flow_process_events(
        &self,
        flow_binding: &FlowBinding,
        events: Vec<FlowProcessEvent>,
    ) -> Result<FlowProcessEventsImpact, InternalError> {
        let mut maybe_new_trigger_state: Option<FlowTriggerState> = None;

        for event in events {
            tracing::debug!(
                flow_binding = ?flow_binding,
                event = ?event,
                "Applying flow process event"
            );

            match event {
                // Autostop events have side effects on the trigger
                FlowProcessEvent::AutoStopped(e) => {
                    // Apply auto-stop decision to the trigger
                    let new_trigger_state = self
                        .flow_trigger_service
                        .apply_trigger_auto_stop_decision(e.event_time, flow_binding)
                        .await?;

                    // Merge new state with any previous state change in this batch of events
                    maybe_new_trigger_state = match (maybe_new_trigger_state, new_trigger_state) {
                        (None, None) => None,
                        (Some(x), None) => Some(x),
                        (_, Some(y)) => Some(y),
                    };

                    // Also, notify external systems
                    self.outbox
                        .post_message(
                            MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR,
                            FlowProcessLifecycleMessage::trigger_auto_stopped(
                                e.event_time,
                                flow_binding.clone(),
                                e.reason,
                            ),
                        )
                        .await?;
                }

                // State change events have no side effects on the trigger
                FlowProcessEvent::EffectiveStateChanged(e) => {
                    self.outbox
                        .post_message(
                            MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR,
                            FlowProcessLifecycleMessage::effective_state_changed(
                                e.event_time,
                                flow_binding.clone(),
                                e.old_state,
                                e.new_state,
                            ),
                        )
                        .await?;
                }
            }
        }

        // Deliver latest state of the trigger
        let maybe_latest_trigger_state = match maybe_new_trigger_state {
            Some(s) => {
                tracing::info!(
                    new_state = ?s,
                    "Flow trigger state updated as a result of flow process event"
                );
                Some(s)
            }
            None => {
                // No state change, but still load the trigger in the current state
                self.flow_trigger_service
                    .find_trigger(flow_binding)
                    .await
                    .int_err()?
            }
        };

        Ok(FlowProcessEventsImpact {
            maybe_latest_trigger_state,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventProjector for FlowProcessStateProjector {
    fn name(&self) -> &'static str {
        "dev.kamu.domain.flow-system.FlowProcessStateProjector"
    }

    #[tracing::instrument(level = "debug", skip_all, fields(event_id=%event.event_id))]
    async fn apply(&self, event: &FlowSystemEvent) -> Result<(), InternalError> {
        tracing::debug!(
            event_id = %event.event_id,
            source_type = ?event.source_type,
            source_event_id = %event.source_event_id,
            payload = ?event.payload,
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

struct FlowProcessEventsImpact {
    maybe_latest_trigger_state: Option<FlowTriggerState>,
}

impl FlowProcessEventsImpact {
    fn is_trigger_still_active(&self) -> bool {
        self.maybe_latest_trigger_state
            .as_ref()
            .map(FlowTriggerState::is_active)
            .unwrap_or(false)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
