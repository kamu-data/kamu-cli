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
use kamu_datasets::DatasetLifecycleMessage;
use kamu_webhooks::*;
use kamu_webhooks_inmem::{
    InMemoryWebhookSubscriptionEventStore,
    InMemoryWebhookWatermarkRepository,
};
use kamu_webhooks_services::{WebhookDatasetRemovalHandler, WebhookSecretGeneratorImpl};
use messaging_outbox::{MessageConsumerT, MockOutbox, Outbox};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_subscriptions_removed_with_dataset() {
    let dataset_id_1 = odf::DatasetID::new_seeded_ed25519(b"foo");
    let dataset_id_2 = odf::DatasetID::new_seeded_ed25519(b"bar");

    let subscription_1_1 = WebhookSubscriptionID::new(uuid::Uuid::from_u128(11));
    let subscription_1_2 = WebhookSubscriptionID::new(uuid::Uuid::from_u128(12));
    let subscription_2 = WebhookSubscriptionID::new(uuid::Uuid::from_u128(2));

    // Announced like a removal by the user, so their flows are cleaned up too
    let mut mock_outbox = MockOutbox::new();
    for subscription_id in [subscription_1_1, subscription_1_2] {
        TestWebhookDatasetRemovalHandlerHarness::expect_subscription_deleted_message(
            &mut mock_outbox,
            subscription_id,
            &dataset_id_1,
        );
    }

    let harness = TestWebhookDatasetRemovalHandlerHarness::new(mock_outbox);
    harness
        .create_subscription(subscription_1_1, &dataset_id_1)
        .await;
    harness
        .create_subscription(subscription_1_2, &dataset_id_1)
        .await;
    harness
        .create_subscription(subscription_2, &dataset_id_2)
        .await;

    assert_eq!(harness.count_subscriptions(&dataset_id_1).await, 2);
    assert_eq!(harness.count_subscriptions(&dataset_id_2).await, 1);

    harness.delete_dataset(&dataset_id_1).await;

    assert_eq!(harness.count_subscriptions(&dataset_id_1).await, 0);
    assert_eq!(harness.count_subscriptions(&dataset_id_2).await, 1);

    assert!(!harness.has_watermark(subscription_1_1).await);
    assert!(!harness.has_watermark(subscription_1_2).await);
    assert!(harness.has_watermark(subscription_2).await);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestWebhookDatasetRemovalHandlerHarness {
    catalog: Catalog,
    handler: Arc<WebhookDatasetRemovalHandler>,
    event_store: Arc<dyn WebhookSubscriptionEventStore>,
    watermark_repo: Arc<dyn WebhookWatermarkRepository>,
    webhook_secret_generator: Arc<dyn WebhookSecretGenerator>,
}

impl TestWebhookDatasetRemovalHandlerHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let mut b = CatalogBuilder::new();
        b.add::<WebhookDatasetRemovalHandler>()
            .add::<WebhookSecretGeneratorImpl>()
            .add_value(WebhooksConfig::default())
            .add_value(mock_outbox)
            .bind::<dyn Outbox, MockOutbox>()
            .add::<InMemoryWebhookSubscriptionEventStore>()
            .add::<InMemoryWebhookWatermarkRepository>();

        let catalog = b.build();

        Self {
            handler: catalog.get_one().unwrap(),
            event_store: catalog.get_one().unwrap(),
            watermark_repo: catalog.get_one().unwrap(),
            webhook_secret_generator: catalog.get_one().unwrap(),
            catalog,
        }
    }

    fn expect_subscription_deleted_message(
        mock_outbox: &mut MockOutbox,
        subscription_id: WebhookSubscriptionID,
        dataset_id: &odf::DatasetID,
    ) {
        use mockall::predicate::{eq, function};

        let expected_message = WebhookSubscriptionLifecycleMessage::deleted(
            subscription_id,
            Some(dataset_id.clone()),
            vec![WebhookEventTypeCatalog::dataset_ref_updated()],
        );

        mock_outbox
            .expect_post_message_as_json()
            .times(1)
            .with(
                eq(MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_SERVICE),
                function(move |message_as_json: &serde_json::Value| {
                    serde_json::from_value::<WebhookSubscriptionLifecycleMessage>(
                        message_as_json.clone(),
                    )
                    .unwrap()
                        == expected_message
                }),
                eq(1),
            )
            .returning(|_, _, _| Ok(()));
    }

    /// A subscription to the dataset's updates, with a watermark
    async fn create_subscription(
        &self,
        subscription_id: WebhookSubscriptionID,
        dataset_id: &odf::DatasetID,
    ) {
        let mut subscription = WebhookSubscription::new(
            subscription_id,
            url::Url::parse(&format!("https://example.com/webhook/{subscription_id}")).unwrap(),
            WebhookSubscriptionLabel::try_new("").unwrap(),
            Some(dataset_id.clone()),
            vec![WebhookEventTypeCatalog::dataset_ref_updated()],
            self.webhook_secret_generator.generate_secret().unwrap(),
        );
        subscription.save(self.event_store.as_ref()).await.unwrap();

        self.watermark_repo
            .set_watermark(
                subscription_id,
                &WebhookEventTypeCatalog::dataset_ref_updated(),
                WebhookWatermark::from(serde_json::json!({ "position": 1 })),
            )
            .await
            .unwrap();
    }

    async fn delete_dataset(&self, dataset_id: &odf::DatasetID) {
        self.handler
            .consume_message(
                &self.catalog,
                &DatasetLifecycleMessage::deleted(Utc::now(), dataset_id.clone()),
            )
            .await
            .unwrap();
    }

    async fn count_subscriptions(&self, dataset_id: &odf::DatasetID) -> usize {
        self.event_store
            .count_subscriptions_by_dataset(dataset_id)
            .await
            .unwrap()
    }

    async fn has_watermark(&self, subscription_id: WebhookSubscriptionID) -> bool {
        self.watermark_repo
            .find_watermark(
                subscription_id,
                &WebhookEventTypeCatalog::dataset_ref_updated(),
            )
            .await
            .unwrap()
            .is_some()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
