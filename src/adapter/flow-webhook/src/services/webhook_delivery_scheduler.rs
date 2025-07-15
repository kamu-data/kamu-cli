// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use internal_error::InternalError;
use kamu_datasets::{DatasetReferenceMessage, MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE};
use kamu_webhooks::*;
use messaging_outbox::*;
use time_source::SystemTimeSource;
use {kamu_adapter_task_webhook as atw, kamu_flow_system as fs, kamu_task_system as ts};

use crate::FLOW_TYPE_WEBHOOK_DELIVER;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetReferenceMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_WEBHOOK_DELIVERY_SCHEDULER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct WebhookDeliveryScheduler {
    webhook_event_builder: Arc<dyn WebhookPayloadBuilder>,
    flow_run_service: Arc<dyn fs::FlowRunService>,
    time_source: Arc<dyn SystemTimeSource>,
    webhook_subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WebhookDeliveryScheduler {
    async fn schedule_dataset_webhook_event_delivery(
        &self,
        dataset_id: &odf::DatasetID,
        event_type: WebhookEventType,
        task_run_arguments: ts::TaskRunArguments,
    ) -> Result<(), InternalError> {
        // Match subscriptions for this event type
        let subscription_ids = self
            .list_enabled_webhook_subscriptions_for_dataset_event(dataset_id, &event_type)
            .await?;

        // Schedule webhook delivery flow for each subscription
        for subscription_id in subscription_ids {
            self.schedule_webhook_delivery_flow(
                subscription_id,
                dataset_id,
                task_run_arguments.clone(),
            )
            .await?;
        }

        Ok(())
    }

    async fn list_enabled_webhook_subscriptions_for_dataset_event(
        &self,
        dataset_id: &odf::DatasetID,
        event_type: &WebhookEventType,
    ) -> Result<Vec<WebhookSubscriptionID>, InternalError> {
        let subscription_ids = self
            .webhook_subscription_event_store
            .list_enabled_subscription_ids_by_dataset_and_event_type(dataset_id, event_type)
            .await
            .map_err(|e| match e {
                ListWebhookSubscriptionsError::Internal(e) => e,
            })?;

        tracing::debug!(
            len = subscription_ids.len(),
            ?subscription_ids,
            %event_type,
            "Found subscriptions for dataset event"
        );

        Ok(subscription_ids)
    }

    async fn schedule_webhook_delivery_flow(
        &self,
        subscription_id: WebhookSubscriptionID,
        dataset_id: &odf::DatasetID,
        task_run_arguments: ts::TaskRunArguments,
    ) -> Result<(), InternalError> {
        let flow_binding = fs::FlowBinding::for_webhook_subscription(
            subscription_id.into_inner(),
            Some(dataset_id.clone()),
            FLOW_TYPE_WEBHOOK_DELIVER,
        );

        let activation_cause =
            fs::FlowActivationCause::AutoPolling(fs::FlowActivationCauseAutoPolling {
                activation_time: self.time_source.now(),
            });

        let flow = self
            .flow_run_service
            .run_flow_automatically(
                &flow_binding,
                activation_cause,
                None,
                None,
                Some(task_run_arguments),
            )
            .await
            .int_err()?;

        tracing::debug!(flow = ?flow.flow_id, %subscription_id, "Scheduled webhook delivery flow");

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for WebhookDeliveryScheduler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetReferenceMessage> for WebhookDeliveryScheduler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "WebhookDeliveryScheduler[DatasetReferenceMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetReferenceMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset reference update message");

        match message {
            DatasetReferenceMessage::Updated(msg) => {
                // Build the payload for the webhook event
                let webhook_payload = self
                    .webhook_event_builder
                    .build_dataset_ref_updated_payload(
                        &msg.dataset_id,
                        &msg.block_ref,
                        &msg.new_block_hash,
                        msg.maybe_prev_block_hash.as_ref(),
                    )
                    .await
                    .int_err()?;

                // Schedule the delivery of the event to all subscriptions
                self.schedule_dataset_webhook_event_delivery(
                    &msg.dataset_id,
                    WebhookEventTypeCatalog::dataset_ref_updated(),
                    atw::TaskRunArgumentsWebhookDeliver {
                        event_type: WebhookEventTypeCatalog::dataset_ref_updated(),
                        payload: webhook_payload,
                    }
                    .into_task_run_arguments(),
                )
                .await
                .int_err()?;

                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
