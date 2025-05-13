// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use dill::*;
use internal_error::InternalError;
use kamu_datasets::{
    DatasetEntryService,
    DatasetReferenceMessage,
    DatasetReferenceMessageUpdated,
    MESSAGE_CONSUMER_KAMU_DATASET_REFERENCE_SERVICE,
};
use kamu_task_system::{LogicalPlan, LogicalPlanSendWebhook, TaskScheduler};
use kamu_webhooks::*;
use messaging_outbox::*;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MESSAGE_CONSUMER_KAMU_WEBHOOK_OUTBOX_BRIDGE: &str =
    "dev.kamu.domain.webhooks.WebhookOutboxBridge";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const WEBHOOK_DATASET_HEAD_UPDATED_VERSION: &str = "1";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetReferenceMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASET_REFERENCE_SERVICE,
    feeding_producers: &[
        MESSAGE_CONSUMER_KAMU_DATASET_REFERENCE_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct WebhookOutboxBridge {
    dataset_entry_svc: Arc<dyn DatasetEntryService>,
    task_scheduler: Arc<dyn TaskScheduler>,
    webhook_event_repo: Arc<dyn WebhookEventRepository>,
    webhook_subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WebhookOutboxBridge {
    async fn build_dataset_head_updated_payload(
        &self,
        msg: &DatasetReferenceMessageUpdated,
    ) -> Result<serde_json::Value, InternalError> {
        // Find out who is the owner of the dataset
        let dataset_entry = self
            .dataset_entry_svc
            .get_entry(&msg.dataset_id)
            .await
            .int_err()?;

        // Form event payload
        let mut payload = json!({
            "version": WEBHOOK_DATASET_HEAD_UPDATED_VERSION,
            "datasetId": msg.dataset_id.to_string(),
            "ownerAccountId": dataset_entry.owner_id.to_string(),
            "newHash": msg.new_block_hash.to_string(),
        });

        // Add optional fields
        if let Some(prev_block_hash) = msg.maybe_prev_block_hash.as_ref() {
            if let serde_json::Value::Object(ref mut map) = payload {
                map.insert("oldHash".to_string(), json!(prev_block_hash.to_string()));
            } else {
                unreachable!("Expected a JSON object");
            }
        }

        tracing::debug!(?payload, "Formed payload for dataset head updated event");

        Ok(payload)
    }

    async fn list_enabled_webhook_subscriptions_for_dataset_event(
        &self,
        dataset_id: &odf::DatasetID,
        event_type: &WebhookEventType,
    ) -> Result<Vec<WebhookSubscriptionId>, InternalError> {
        let subscriptions = self
            .webhook_subscription_event_store
            .list_enabled_subscription_ids_by_dataset_and_event_type(dataset_id, event_type)
            .await
            .map_err(|e| match e {
                ListWebhookSubscriptionsError::Internal(e) => e,
            })?;

        Ok(subscriptions)
    }

    async fn schedule_webhook_task(
        &self,
        subscription_id: WebhookSubscriptionId,
        event_id: WebhookEventId,
    ) -> Result<(), InternalError> {
        let task = self
            .task_scheduler
            .create_task(
                LogicalPlan::SendWebhook(LogicalPlanSendWebhook {
                    dataset_id: None,
                    webhook_subscription_id: subscription_id.into_inner(),
                    webhook_event_id: event_id.into_inner(),
                }),
                None,
            )
            .await
            .int_err()?;

        tracing::debug!(task_id = ?task.task_id, %event_id, %subscription_id, "Scheduled webhook task");

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for WebhookOutboxBridge {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetReferenceMessage> for WebhookOutboxBridge {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "WebhookOutboxBridge[DatasetReferenceMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetReferenceMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset reference update message");

        match message {
            DatasetReferenceMessage::Updated(msg) => {
                // Ignore updates that are not for HEAD
                if msg.block_ref != odf::BlockRef::Head {
                    tracing::debug!(?msg, "Ignoring non-HEAD dataset reference update");
                    return Ok(());
                }

                // Form event payload
                let payload = self.build_dataset_head_updated_payload(msg).await?;

                // Create and register a webhook event
                let event_id = WebhookEventId::new(uuid::Uuid::new_v4());
                let event = WebhookEvent::new(
                    event_id,
                    WebhookEventTypeCatalog::dataset_head_updated(),
                    payload,
                    Utc::now(),
                );
                self.webhook_event_repo
                    .create_event(&event)
                    .await
                    .int_err()?;

                // Match subscriptions and schedule individual delivery tasks
                let subscription_ids = self
                    .list_enabled_webhook_subscriptions_for_dataset_event(
                        &msg.dataset_id,
                        &event.event_type,
                    )
                    .await?;
                for subscription_id in subscription_ids {
                    self.schedule_webhook_task(subscription_id, event_id)
                        .await?;
                }

                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
