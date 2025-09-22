// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::*;
use kamu_task_system::TaskOutcome;

use crate::FlowSchedulingHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn FlowSystemEventProjector)]
pub struct FlowProcessStateProjector {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowProcessStateProjector {
    async fn process_flow_trigger_event(
        &self,
        transaction_catalog: &dill::Catalog,
        event_id: EventID,
        trigger_event: FlowTriggerEvent,
    ) -> Result<(), InternalError> {
        let flow_process_state_repository = transaction_catalog
            .get_one::<dyn FlowProcessStateRepository>()
            .unwrap();

        match trigger_event {
            FlowTriggerEvent::Created(e) => {
                flow_process_state_repository
                    .upsert_process_state_on_trigger_event(
                        event_id,
                        e.flow_binding,
                        e.paused,
                        e.stop_policy,
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!(
                            error = ?e,
                            error_msg = %e,
                            "Failed to insert new flow process"
                        );
                        e.int_err()
                    })?;
            }

            FlowTriggerEvent::Modified(e) => {
                flow_process_state_repository
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
                // Ignored, computed by projection similarly
            }

            FlowTriggerEvent::ScopeRemoved(e) => {
                flow_process_state_repository
                    .delete_process_states_by_scope(&e.flow_binding.scope)
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }

    async fn process_flow_event(
        &self,
        transaction_catalog: &dill::Catalog,
        event_id: EventID,
        flow_event: FlowEvent,
    ) -> Result<(), InternalError> {
        match &flow_event {
            FlowEvent::ScheduledForActivation(e) => {
                let flow_process_state_repository = transaction_catalog
                    .get_one::<dyn FlowProcessStateRepository>()
                    .unwrap();

                flow_process_state_repository
                    .on_flow_scheduled(
                        event_id,
                        flow_event.flow_binding(),
                        e.scheduled_for_activation_at,
                    )
                    .await
                    .int_err()?;
            }

            FlowEvent::TaskFinished(e) => {
                let last_task_in_flow = match e.task_outcome {
                    TaskOutcome::Success(_) | TaskOutcome::Cancelled => true,
                    TaskOutcome::Failed(_) => e.next_attempt_at.is_none(),
                };

                if last_task_in_flow {
                    let is_success = match e.task_outcome {
                        TaskOutcome::Success(_) => true,
                        TaskOutcome::Failed(_) => false,
                        TaskOutcome::Cancelled => return Ok(()), // Ignore cancelled flows
                    };

                    let flow_process_state_repository = transaction_catalog
                        .get_one::<dyn FlowProcessStateRepository>()
                        .unwrap();

                    flow_process_state_repository
                        .apply_flow_result(
                            event_id,
                            flow_event.flow_binding(),
                            is_success,
                            flow_event.event_time(),
                        )
                        .await
                        .int_err()?;

                    let flow_trigger_service = transaction_catalog
                        .get_one::<dyn FlowTriggerService>()
                        .unwrap();

                    // In case of a failure, trigger should make a decision about auto-stopping
                    if !is_success {
                        flow_trigger_service
                            .evaluate_trigger_on_failure(
                                flow_event.event_time(),
                                flow_event.flow_binding(),
                                !e.task_outcome.is_recoverable_failure(),
                            )
                            .await?;
                    }

                    // Recover the flow to check if we need to schedule next activation
                    let flow_event_store =
                        transaction_catalog.get_one::<dyn FlowEventStore>().unwrap();

                    let flow = Flow::load(flow_event.flow_id(), flow_event_store.as_ref())
                        .await
                        .int_err()?;

                    let scheduling_helper = transaction_catalog
                        .get_one::<FlowSchedulingHelper>()
                        .unwrap();

                    // In case of success:
                    //  - schedule next flow immediately, if we had any late activation cause
                    if is_success {
                        scheduling_helper
                            .try_schedule_late_flow_activations(flow_event.event_time(), &flow)
                            .await?;
                    }

                    // Try to schedule auto-polling flow, if applicable.
                    // We don't care whether we failed or succeeded,
                    // that is determined with the stop policy in the trigger.
                    scheduling_helper
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

    async fn apply(
        &self,
        transaction_catalog: &dill::Catalog,
        event: &FlowSystemEvent,
    ) -> Result<(), InternalError> {
        match event.source_type {
            FlowSystemEventSourceType::FlowConfiguration => { /* ignored */ }

            FlowSystemEventSourceType::FlowTrigger => {
                let trigger_event: FlowTriggerEvent =
                    serde_json::from_value(event.payload.clone()).int_err()?;
                self.process_flow_trigger_event(transaction_catalog, event.event_id, trigger_event)
                    .await?;
            }

            FlowSystemEventSourceType::Flow => {
                let flow_event: FlowEvent =
                    serde_json::from_value(event.payload.clone()).int_err()?;
                self.process_flow_event(transaction_catalog, event.event_id, flow_event)
                    .await?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
