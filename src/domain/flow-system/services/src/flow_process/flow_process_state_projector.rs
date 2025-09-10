// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
        MESSAGE_PRODUCER_KAMU_FLOW_AGENT,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct FlowProcessStateProjector {}

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
        _target_catalog: &dill::Catalog,
        message: &FlowTriggerUpdatedMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received flow trigger message");
        unimplemented!()
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
        target_catalog: &dill::Catalog,
        message: &FlowProgressMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received flow progress message");

        match message {
            /*FlowProgressMessage::Scheduled(scheduled_message) => {
                let flow_process_state_repository = target_catalog
                    .get_one::<dyn FlowProcessStateRepository>()
                    .int_err()?;

                flow_process_state_repository
                    .on_flow_scheduled(
                        message.flow_binding().clone(),
                        scheduled_message.scheduled_for_activation_at,
                        EventID::new(0), // TODO: pass event_id somehow
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
            }*/
            FlowProgressMessage::Finished(finished_message) => {
                let is_success = match finished_message.outcome {
                    FlowOutcome::Success(_) => true,
                    FlowOutcome::Failed => false,
                    FlowOutcome::Aborted => return Ok(()), // Ignore aborted flows
                };

                let flow_process_state_repository = target_catalog
                    .get_one::<dyn FlowProcessStateRepository>()
                    .int_err()?;

                flow_process_state_repository
                    .apply_flow_result(
                        message.flow_binding().clone(),
                        is_success,
                        message.event_time(),
                        None,            // TODO: pass next_planned_at somehow
                        EventID::new(0), // TODO: pass event_id somehow
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
            | FlowProgressMessage::Scheduled(_)
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
    #[tracing::instrument(level = "debug", skip_all, fields(flow_scope = ?flow_scope))]
    async fn handle_flow_scope_removal(&self, flow_scope: &FlowScope) -> Result<(), InternalError> {
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
