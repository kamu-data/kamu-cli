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
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_FLOW_PROCESS_STATE_PROJECTOR,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
        MESSAGE_PRODUCER_KAMU_FLOW_AGENT,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct FlowProcessStateProjector {
    flow_process_state_repository: Arc<dyn FlowProcessStateRepository>,
}

impl FlowProcessStateProjector {
    async fn make_sort_key(
        &self,
        target_catalog: &dill::Catalog,
        flow_binding: &FlowBinding,
    ) -> Result<String, InternalError> {
        let flow_controller =
            get_flow_controller_from_catalog(target_catalog, &flow_binding.flow_type)
                .expect("FlowController must be present in the catalog");
        flow_controller.make_flow_sort_key(flow_binding).await
    }
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
        target_catalog: &dill::Catalog,
        message: &FlowTriggerUpdatedMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received flow trigger message");

        // New trigger?
        if message.previous_trigger_status.is_none() {
            let sort_key = self
                .make_sort_key(target_catalog, &message.flow_binding)
                .await?;

            self.flow_process_state_repository
                .insert_process_state(
                    message.event_id,
                    message.flow_binding.clone(),
                    sort_key,
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
        } else {
            // Existing trigger
            self.flow_process_state_repository
                .update_trigger_state(
                    &message.flow_binding,
                    message.event_id,
                    message.trigger_status == FlowTriggerStatus::PausedByUser,
                    message.stop_policy,
                )
                .await
                .map_err(|e| {
                    tracing::error!(
                        error = ?e,
                        error_msg = %e,
                        "Failed to update flow process trigger state"
                    );
                    e.int_err()
                })?;
        }

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
                        EventID::new(0), // TODO: pass event_id somehow
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
                        EventID::new(0), // TODO: pass event_id somehow
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
