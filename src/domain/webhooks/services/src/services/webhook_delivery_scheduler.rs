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
use kamu_task_system::{LogicalPlan, LogicalPlanDeliverWebhook, TaskScheduler};
use kamu_webhooks::*;
use messaging_outbox::*;

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
    task_scheduler: Arc<dyn TaskScheduler>,
    webhook_event_builder: Arc<dyn WebhookEventBuilder>,
    webhook_subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WebhookDeliveryScheduler {
    async fn schedule_dataset_webhook_event_delivery(
        &self,
        dataset_id: &odf::DatasetID,
        event: WebhookEvent,
    ) -> Result<(), InternalError> {
        // Match subscriptions for this event type
        let subscription_ids = self
            .list_enabled_webhook_subscriptions_for_dataset_event(dataset_id, &event.event_type)
            .await?;

        // Schedule webhook delivery tasks for each subscription
        for subscription_id in subscription_ids {
            self.schedule_webhook_delivery_task(subscription_id, event.id)
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

    async fn schedule_webhook_delivery_task(
        &self,
        subscription_id: WebhookSubscriptionID,
        event_id: WebhookEventID,
    ) -> Result<(), InternalError> {
        let task = self
            .task_scheduler
            .create_task(
                LogicalPlan::DeliverWebhook(LogicalPlanDeliverWebhook {
                    dataset_id: None, // TODO: why?
                    webhook_subscription_id: subscription_id.into_inner(),
                    webhook_event_id: event_id.into_inner(),
                }),
                None,
            )
            .await
            .int_err()?;

        tracing::debug!(task_id = ?task.task_id, %event_id, %subscription_id, "Scheduled webhook delivery task");

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
                // Build the event
                let event = self
                    .webhook_event_builder
                    .build_dataset_ref_updated(msg)
                    .await
                    .int_err()?;

                // Schedule the delivery of the event to all subscriptions
                self.schedule_dataset_webhook_event_delivery(&msg.dataset_id, event)
                    .await
                    .int_err()?;

                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
