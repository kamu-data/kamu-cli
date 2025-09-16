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

impl FlowProcessStateProjector {
    pub async fn handle_trigger_updated(
        &self,
        trigger_event_id: EventID,
        flow_binding: &FlowBinding,
        trigger_status: FlowTriggerStatus,
        stop_policy: FlowTriggerStopPolicy,
    ) -> Result<(), InternalError> {
        let paused_manual = trigger_status == FlowTriggerStatus::PausedByUser;

        self.flow_process_state_repository
            .upsert_process_state_on_trigger_event(
                trigger_event_id,
                flow_binding.clone(),
                paused_manual,
                stop_policy,
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

    pub async fn handle_flow_scheduled(
        &self,
        flow_event_id: EventID,
        flow_binding: &FlowBinding,
        scheduled_for_activation_at: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        self.flow_process_state_repository
            .on_flow_scheduled(flow_event_id, flow_binding, scheduled_for_activation_at)
            .await
            .map_err(|e| {
                tracing::error!(
                    error = ?e,
                    error_msg = %e,
                    "Failed to handle flow scheduled"
                );
                e.int_err()
            })?;

        Ok(())
    }

    pub async fn handle_flow_finished(
        &self,
        flow_event_id: EventID,
        flow_binding: &FlowBinding,
        flow_outcome: &FlowOutcome,
        finished_at: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let is_success = match flow_outcome {
            FlowOutcome::Success(_) => true,
            FlowOutcome::Failed => false,
            FlowOutcome::Aborted => return Ok(()), // Ignore aborted flows
        };

        self.flow_process_state_repository
            .apply_flow_result(flow_event_id, flow_binding, is_success, finished_at)
            .await
            .map_err(|e| {
                tracing::error!(
                    error = ?e,
                    error_msg = %e,
                    "Failed to apply flow result"
                );
                e.int_err()
            })?;

        Ok(())
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
        _: &dill::Catalog,
        message: &FlowTriggerUpdatedMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received flow trigger message");

        self.handle_trigger_updated(
            message.event_id,
            &message.flow_binding,
            message.trigger_status,
            message.stop_policy,
        )
        .await?;

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
                self.handle_flow_scheduled(
                    scheduled_message.event_id,
                    message.flow_binding(),
                    scheduled_message.scheduled_for_activation_at,
                )
                .await?;
            }
            FlowProgressMessage::Finished(finished_message) => {
                self.handle_flow_finished(
                    finished_message.event_id,
                    message.flow_binding(),
                    &finished_message.outcome,
                    finished_message.event_time,
                )
                .await?;
            }

            FlowProgressMessage::RetryScheduled(_)
            | FlowProgressMessage::Running(_)
            | FlowProgressMessage::Cancelled(_) => {
                // Ignore
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
