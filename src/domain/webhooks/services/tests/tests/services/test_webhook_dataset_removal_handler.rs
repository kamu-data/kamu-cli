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
use kamu_datasets::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_DATASET_SERVICE};
use kamu_webhooks::*;
use kamu_webhooks_inmem::InMemoryWebhookSubscriptionEventStore;
use kamu_webhooks_services::{WebhookDatasetRemovalHandler, WebhookSecretGeneratorImpl};
use messaging_outbox::{Outbox, OutboxExt, OutboxImmediateImpl, register_message_dispatcher};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_subscriptions_removed_with_dataset() {
    let harness = TestWebhookDatasetRemovalHandlerHarness::new();

    let dataset_id_1 = odf::DatasetID::new_seeded_ed25519(b"foo");
    let dataset_id_2 = odf::DatasetID::new_seeded_ed25519(b"bar");

    {
        let mut subscription_id_1_1 = WebhookSubscription::new(
            WebhookSubscriptionID::new(uuid::Uuid::new_v4()),
            url::Url::parse("https://example.com/webhook/1/1").unwrap(),
            WebhookSubscriptionLabel::try_new("").unwrap(),
            Some(dataset_id_1.clone()),
            vec![WebhookEventTypeCatalog::dataset_ref_updated()],
            harness.webhook_secret_generator.generate_secret().unwrap(),
        );
        subscription_id_1_1
            .save(harness.event_store.as_ref())
            .await
            .unwrap();
    }

    {
        let mut subscription_id_1_2 = WebhookSubscription::new(
            WebhookSubscriptionID::new(uuid::Uuid::new_v4()),
            url::Url::parse("https://example.com/webhook/1/2").unwrap(),
            WebhookSubscriptionLabel::try_new("").unwrap(),
            Some(dataset_id_1.clone()),
            vec![WebhookEventTypeCatalog::dataset_ref_updated()],
            harness.webhook_secret_generator.generate_secret().unwrap(),
        );
        subscription_id_1_2
            .save(harness.event_store.as_ref())
            .await
            .unwrap();
    }

    {
        let mut subscription_id_2 = WebhookSubscription::new(
            WebhookSubscriptionID::new(uuid::Uuid::new_v4()),
            url::Url::parse("https://example.com/webhook/2").unwrap(),
            WebhookSubscriptionLabel::try_new("").unwrap(),
            Some(dataset_id_2.clone()),
            vec![WebhookEventTypeCatalog::dataset_ref_updated()],
            harness.webhook_secret_generator.generate_secret().unwrap(),
        );
        subscription_id_2
            .save(harness.event_store.as_ref())
            .await
            .unwrap();
    };

    let res = harness
        .event_store
        .count_subscriptions_by_dataset(&dataset_id_1)
        .await;
    assert_eq!(res.unwrap(), 2);

    let res = harness
        .event_store
        .count_subscriptions_by_dataset(&dataset_id_2)
        .await;
    assert_eq!(res.unwrap(), 1);

    harness
        .outbox
        .post_message(
            MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            DatasetLifecycleMessage::deleted(Utc::now(), dataset_id_1.clone()),
        )
        .await
        .unwrap();

    let res = harness
        .event_store
        .count_subscriptions_by_dataset(&dataset_id_1)
        .await;
    assert_eq!(res.unwrap(), 0);

    let res = harness
        .event_store
        .count_subscriptions_by_dataset(&dataset_id_2)
        .await;
    assert_eq!(res.unwrap(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestWebhookDatasetRemovalHandlerHarness {
    event_store: Arc<dyn WebhookSubscriptionEventStore>,
    webhook_secret_generator: Arc<dyn WebhookSecretGenerator>,
    outbox: Arc<dyn Outbox>,
}

impl TestWebhookDatasetRemovalHandlerHarness {
    fn new() -> Self {
        let mut b = CatalogBuilder::new();
        b.add::<WebhookDatasetRemovalHandler>()
            .add::<WebhookSecretGeneratorImpl>()
            .add_value(WebhooksConfig::default())
            .add_builder(messaging_outbox::OutboxImmediateImpl::builder(
                messaging_outbox::ConsumerFilter::AllConsumers,
            ))
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add::<InMemoryWebhookSubscriptionEventStore>();

        register_message_dispatcher::<DatasetLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        );

        let catalog = b.build();

        Self {
            event_store: catalog.get_one().unwrap(),
            webhook_secret_generator: catalog.get_one().unwrap(),
            outbox: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
