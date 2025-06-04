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
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_DATASET_SERVICE};
use kamu_webhooks::{
    MESSAGE_CONSUMER_KAMU_WEBHOOK_DATASET_REMOVAL_HANDLER,
    WebhookSubscription,
    WebhookSubscriptionEventStore,
};
use messaging_outbox::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_WEBHOOK_DATASET_REMOVAL_HANDLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct WebhookDatasetRemovalHandler {
    webhook_subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for WebhookDatasetRemovalHandler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for WebhookDatasetRemovalHandler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "WebhookDatasetRemovalHandler[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        match message {
            DatasetLifecycleMessage::Deleted(msg) => {
                // Find all subscriptions for the removed dataset
                let subscription_ids = self
                    .webhook_subscription_event_store
                    .list_subscription_ids_by_dataset(&msg.dataset_id)
                    .await
                    .int_err()?;
                if subscription_ids.is_empty() {
                    tracing::debug!("No subscriptions found for removed dataset");
                    return Ok(());
                }

                tracing::debug!(subscription_ids = ?subscription_ids, "Found subscriptions for removed dataset");

                // Load these subscriptions
                let subscriptions = WebhookSubscription::load_multi(
                    subscription_ids,
                    self.webhook_subscription_event_store.as_ref(),
                )
                .await
                .int_err()?;

                tracing::debug!(subscriptions = ?subscriptions, "Loaded subscriptions");

                // Remove each subscription automatically
                for subscription in subscriptions {
                    let mut subscription = subscription.int_err()?;

                    subscription.remove().int_err()?;
                    subscription
                        .save(self.webhook_subscription_event_store.as_ref())
                        .await
                        .int_err()?;

                    tracing::debug!(subscription_id = ?subscription.id(), "Removed subscription");
                }

                Ok(())
            }

            DatasetLifecycleMessage::Created(_) | DatasetLifecycleMessage::Renamed(_) => {
                // Ignore these messages
                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
