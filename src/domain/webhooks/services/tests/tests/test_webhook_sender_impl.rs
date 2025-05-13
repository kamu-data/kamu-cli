// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use database_common::NoOpDatabasePlugin;
use kamu_task_system as ts;
use kamu_webhooks::*;
use kamu_webhooks_inmem::{
    InMemoryWebhookDeliveryRepository,
    InMemoryWebhookEventRepository,
    InMemoryWebhookSubscriptionEventStore,
};
use kamu_webhooks_services::{WebhookSenderImpl, WebhookSignerImpl};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[ignore]
#[test_log::test(tokio::test)]
async fn test_send_webhook() {
    let mut b = dill::CatalogBuilder::new();
    b.add::<WebhookSenderImpl>()
        .add::<WebhookSignerImpl>()
        .add::<InMemoryWebhookSubscriptionEventStore>()
        .add::<InMemoryWebhookDeliveryRepository>()
        .add::<InMemoryWebhookEventRepository>();

    NoOpDatabasePlugin::init_database_components(&mut b);

    let catalog = b.build();

    let webhook_event_repo = catalog.get_one::<dyn WebhookEventRepository>().unwrap();
    let webhook_subscription_event_store = catalog
        .get_one::<dyn WebhookSubscriptionEventStore>()
        .unwrap();

    let task_attempt_id = ts::TaskAttemptID::new(ts::TaskID::new(153), 0);

    let webhook_event_id = WebhookEventId::new(uuid::Uuid::new_v4());
    let webhook_event = WebhookEvent::new(
        webhook_event_id,
        WebhookEventTypeCatalog::dataset_head_updated(),
        serde_json::json!({
          "version": "1",
          "datasetId": odf::DatasetID::new_seeded_ed25519(b"test_dataset_id").to_string(),
          "ownerAccountId": odf::AccountID::new_seeded_ed25519(b"test_account_id").to_string(),
          "ref": "head",
          "oldHash": odf::Multihash::from_digest_sha3_256(b"old_hash").to_string(),
          "newHash": odf::Multihash::from_digest_sha3_256(b"new_hash").to_string(),
        }),
        Utc::now(),
    );
    webhook_event_repo
        .create_event(&webhook_event)
        .await
        .unwrap();

    let webhook_subscription_id = WebhookSubscriptionId::new(uuid::Uuid::new_v4());

    let mut webhook_subscription = WebhookSubscription::new(
        webhook_subscription_id,
        Url::parse("https://webhook.site/b353da00-c73b-4436-a367-a8fbfe4ec800").unwrap(),
        WebhookSubscriptionLabel::new("test".to_string()),
        None,
        vec![WebhookEventTypeCatalog::test()],
        WebhookSubscriptionSecret::try_new("some-secret").unwrap(),
    );
    webhook_subscription
        .save(webhook_subscription_event_store.as_ref())
        .await
        .unwrap();

    let webhook_sender = catalog.get_one::<dyn WebhookSender>().unwrap();
    webhook_sender
        .send_webhook(
            task_attempt_id,
            webhook_subscription_id.into_inner(),
            webhook_event_id.into_inner(),
        )
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
