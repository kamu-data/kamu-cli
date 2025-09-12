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
use messaging_outbox::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<FlowTriggerUpdatedMessage>)]
#[dill::interface(dyn MessageConsumerT<FlowProgressMessage>)]
#[dill::interface(dyn FlowScopeRemovalHandler)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_FLOW_PROCESS_STATE_PROJECTOR,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
        MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct FlowProcessStateProjector {
    flow_process_state_repository: Arc<dyn FlowProcessStateRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for FlowProcessStateProjector {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<FlowTriggerUpdatedMessage> for FlowProcessStateProjector {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowProcessStateProjector[FlowTriggerUpdatedMessage]"
    )]
    async fn consume_message(
        &self,
        _: &dill::Catalog,
        message: &FlowTriggerUpdatedMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received flow trigger message");

        self.flow_process_state_repository
            .upsert_process_state_on_trigger_event(
                message.event_id,
                message.flow_binding.clone(),
                message.trigger_status == FlowTriggerStatus::PausedByUser,
                message.stop_policy,
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

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<FlowProgressMessage> for FlowProcessStateProjector {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowProcessStateProjector[FlowProgressMessage]"
    )]
    async fn consume_message(
        &self,
        _: &dill::Catalog,
        message: &FlowProgressMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received flow progress message");

        match message {
            FlowProgressMessage::Scheduled(scheduled_message) => {
                self.flow_process_state_repository
                    .on_flow_scheduled(
                        scheduled_message.event_id,
                        message.flow_binding(),
                        scheduled_message.scheduled_for_activation_at,
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!(
                            error = ?e,
                            error_msg = %e,
                            "Failed to handle flow scheduled"
                        );
                        e.int_err()
                    })?;
            }
            FlowProgressMessage::Finished(finished_message) => {
                let is_success = match finished_message.outcome {
                    FlowOutcome::Success(_) => true,
                    FlowOutcome::Failed => false,
                    FlowOutcome::Aborted => return Ok(()), // Ignore aborted flows
                };

                self.flow_process_state_repository
                    .apply_flow_result(
                        finished_message.event_id,
                        message.flow_binding(),
                        is_success,
                        message.event_time(),
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!(
                            error = ?e,
                            error_msg = %e,
                            "Failed to apply flow result"
                        );
                        e.int_err()
                    })?;
            }

            FlowProgressMessage::RetryScheduled(_)
            | FlowProgressMessage::Running(_)
            | FlowProgressMessage::Cancelled(_) => {
                // Ignore for now
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowScopeRemovalHandler for FlowProcessStateProjector {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowProcessStateProjector[FlowScopeRemovalHandler]"
    )]
    async fn handle_flow_scope_removal(&self, flow_scope: &FlowScope) -> Result<(), InternalError> {
        tracing::debug!(flow_scope = ?flow_scope, "Handling flow scope removal");

        self.flow_process_state_repository
            .delete_process_states_by_scope(flow_scope)
            .await
            .map_err(|e| {
                tracing::error!(
                    error = ?e,
                    error_msg = %e,
                    "Failed to delete flow processes by flow scope"
                );
                e.int_err()
            })?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
