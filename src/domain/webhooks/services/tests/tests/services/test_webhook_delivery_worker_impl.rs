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
use database_common::NoOpDatabasePlugin;
use kamu_webhooks::*;
use kamu_webhooks_inmem::{
    InMemoryWebhookDeliveryRepository,
    InMemoryWebhookSubscriptionEventStore,
};
use kamu_webhooks_services::*;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deliver_webhook() {
    let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
    let webhook_delivery_id = WebhookDeliveryID::new(uuid::Uuid::new_v4());

    let mut mock_webhook_sender = MockWebhookSender::new();
    TestWebhookDeliveryWorkerHarness::add_success_sender_expectation(
        &mut mock_webhook_sender,
        url::Url::parse("https://example.com/webhook").unwrap(),
        webhook_delivery_id,
        subscription_id,
    );

    let harness = TestWebhookDeliveryWorkerHarness::new(mock_webhook_sender);

    harness.new_webhook_subscription(subscription_id).await;

    harness
        .webhook_delivery_worker
        .deliver_webhook(
            webhook_delivery_id,
            subscription_id,
            WebhookEventTypeCatalog::dataset_ref_updated(),
            serde_json::json!({"key": "value"}),
        )
        .await
        .unwrap();

    let delivery = harness
        .webhook_delivery_repo
        .get_by_webhook_delivery_id(webhook_delivery_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.webhook_subscription_id, subscription_id);
    assert_eq!(
        delivery.event_type,
        WebhookEventTypeCatalog::dataset_ref_updated()
    );
    assert_eq!(delivery.webhook_delivery_id, webhook_delivery_id);
    assert_eq!(
        delivery.request.payload,
        serde_json::json!({"key": "value"})
    );

    assert!(delivery.is_successful());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deliver_webhook_failed() {
    let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
    let webhook_delivery_id = WebhookDeliveryID::new(uuid::Uuid::new_v4());

    let mut mock_webhook_sender = MockWebhookSender::new();
    TestWebhookDeliveryWorkerHarness::add_failing_sender_expectation(
        &mut mock_webhook_sender,
        url::Url::parse("https://example.com/webhook").unwrap(),
        webhook_delivery_id,
        subscription_id,
    );

    let harness = TestWebhookDeliveryWorkerHarness::new(mock_webhook_sender);

    harness.new_webhook_subscription(subscription_id).await;

    harness
        .webhook_delivery_worker
        .deliver_webhook(
            webhook_delivery_id,
            subscription_id,
            WebhookEventTypeCatalog::dataset_ref_updated(),
            serde_json::json!({"key": "value"}),
        )
        .await
        .unwrap();

    let delivery = harness
        .webhook_delivery_repo
        .get_by_webhook_delivery_id(webhook_delivery_id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(delivery.webhook_subscription_id, subscription_id);
    assert_eq!(delivery.webhook_delivery_id, webhook_delivery_id);
    assert_eq!(
        delivery.request.payload,
        serde_json::json!({"key": "value"})
    );
    assert_eq!(
        delivery.event_type,
        WebhookEventTypeCatalog::dataset_ref_updated()
    );

    assert!(!delivery.is_successful());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestWebhookDeliveryWorkerHarness {
    webhook_subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
    webhook_delivery_repo: Arc<dyn WebhookDeliveryRepository>,
    webhook_delivery_worker: Arc<dyn WebhookDeliveryWorker>,
}

impl TestWebhookDeliveryWorkerHarness {
    fn new(mock_sender: MockWebhookSender) -> Self {
        let mut b = dill::CatalogBuilder::new();
        b.add::<WebhookDeliveryWorkerImpl>()
            .add::<WebhookSignerImpl>()
            .add_value(mock_sender)
            .bind::<dyn WebhookSender, MockWebhookSender>()
            .add::<InMemoryWebhookSubscriptionEventStore>()
            .add::<InMemoryWebhookDeliveryRepository>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        Self {
            webhook_subscription_event_store: catalog.get_one().unwrap(),
            webhook_delivery_repo: catalog.get_one().unwrap(),
            webhook_delivery_worker: catalog.get_one().unwrap(),
        }
    }

    async fn new_webhook_subscription(&self, subscription_id: WebhookSubscriptionID) {
        let mut subscription = WebhookSubscription::new(
            subscription_id,
            Url::parse("https://example.com/webhook").unwrap(),
            WebhookSubscriptionLabel::try_new("test".to_string()).unwrap(),
            None,
            vec![WebhookEventTypeCatalog::test()],
            WebhookSubscriptionSecret::try_new("some-secret").unwrap(),
        );

        subscription
            .save(self.webhook_subscription_event_store.as_ref())
            .await
            .unwrap();
    }

    fn add_success_sender_expectation(
        mock_webhook_sender: &mut MockWebhookSender,
        target_url: Url,
        delivery_id: WebhookDeliveryID,
        subscription_id: WebhookSubscriptionID,
    ) {
        mock_webhook_sender
            .expect_send_webhook()
            .withf(move |url, _, headers| {
                assert_eq!(target_url, *url);

                assert_eq!(
                    headers.get("Content-Type").map(|h| h.to_str().unwrap()),
                    Some("application/json")
                );

                assert_eq!(
                    headers
                        .get(HEADER_WEBHOOK_DELIVERY_ID)
                        .map(|h| h.to_str().unwrap()),
                    Some(delivery_id.into_inner().to_string().as_str())
                );
                assert_eq!(
                    headers
                        .get(HEADER_WEBHOOK_SUBSCRIPTION_ID)
                        .map(|h| h.to_str().unwrap()),
                    Some(subscription_id.into_inner().to_string().as_str())
                );
                assert_eq!(
                    headers
                        .get(HEADER_WEBHOOK_EVENT_TYPE)
                        .map(|h| h.to_str().unwrap()),
                    Some(WebhookEventTypeCatalog::DATASET_REF_UPDATED)
                );
                assert_eq!(
                    headers
                        .get(HEADER_WEBHOOK_DELIVERY_ATTEMPT)
                        .map(|h| h.to_str().unwrap()),
                    Some("1")
                );
                assert!(headers.contains_key(HEADER_WEBHOOK_TIMESTAMP));
                assert!(headers.contains_key(HEADER_CONTENT_DIGEST));
                assert!(headers.contains_key(HEADER_SIGNATURE));
                assert!(headers.contains_key(HEADER_SIGNATURE_INPUT));

                true
            })
            .returning(|_, _, _| {
                Ok(WebhookResponse::new(
                    http::StatusCode::OK,
                    http::HeaderMap::new(),
                    "ok".to_string(),
                    Utc::now(),
                ))
            });
    }

    fn add_failing_sender_expectation(
        mock_webhook_sender: &mut MockWebhookSender,
        target_url: Url,
        delivery_id: WebhookDeliveryID,
        subscription_id: WebhookSubscriptionID,
    ) {
        mock_webhook_sender
            .expect_send_webhook()
            .withf(move |url, _, headers| {
                assert_eq!(target_url, *url);

                assert_eq!(
                    headers.get("Content-Type").map(|h| h.to_str().unwrap()),
                    Some("application/json")
                );

                assert_eq!(
                    headers
                        .get(HEADER_WEBHOOK_DELIVERY_ID)
                        .map(|h| h.to_str().unwrap()),
                    Some(delivery_id.into_inner().to_string().as_str())
                );
                assert_eq!(
                    headers
                        .get(HEADER_WEBHOOK_SUBSCRIPTION_ID)
                        .map(|h| h.to_str().unwrap()),
                    Some(subscription_id.into_inner().to_string().as_str())
                );

                true
            })
            .returning(|_, _, _| {
                Ok(WebhookResponse::new(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    http::HeaderMap::new(),
                    "Internal error".to_string(),
                    Utc::now(),
                ))
            });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
