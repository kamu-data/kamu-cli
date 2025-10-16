// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
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

    assert_matches!(
        delivery.response,
        Some(WebhookResponse {
            status_code: http::StatusCode::OK,
            ..
        })
    );

    assert!(delivery.is_successful());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deliver_webhook_timeout_failure() {
    let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
    let webhook_delivery_id = WebhookDeliveryID::new(uuid::Uuid::new_v4());

    let mut mock_webhook_sender = MockWebhookSender::new();
    TestWebhookDeliveryWorkerHarness::add_timeout_sender_expectation(
        &mut mock_webhook_sender,
        url::Url::parse("https://example.com/webhook").unwrap(),
        webhook_delivery_id,
        subscription_id,
    );

    let harness = TestWebhookDeliveryWorkerHarness::new(mock_webhook_sender);

    harness.new_webhook_subscription(subscription_id).await;

    let res = harness
        .webhook_delivery_worker
        .deliver_webhook(
            webhook_delivery_id,
            subscription_id,
            WebhookEventTypeCatalog::dataset_ref_updated(),
            serde_json::json!({"key": "value"}),
        )
        .await;
    assert_matches!(
        res,
        Err(WebhookDeliveryError::SendError(
            WebhookSendError::ConnectionTimeout(WebhookSendConnectionTimeoutError {
                target_url,
                timeout: _,
            })
        )) if target_url == Url::parse("https://example.com/webhook").unwrap()
    );

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

    assert_eq!(delivery.response, None);

    assert!(!delivery.is_successful());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deliver_webhook_connection_failure() {
    let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
    let webhook_delivery_id = WebhookDeliveryID::new(uuid::Uuid::new_v4());

    let mut mock_webhook_sender = MockWebhookSender::new();
    TestWebhookDeliveryWorkerHarness::add_connection_failure_sender_expectation(
        &mut mock_webhook_sender,
        url::Url::parse("https://example.com/webhook").unwrap(),
        webhook_delivery_id,
        subscription_id,
    );

    let harness = TestWebhookDeliveryWorkerHarness::new(mock_webhook_sender);

    harness.new_webhook_subscription(subscription_id).await;

    let res = harness
        .webhook_delivery_worker
        .deliver_webhook(
            webhook_delivery_id,
            subscription_id,
            WebhookEventTypeCatalog::dataset_ref_updated(),
            serde_json::json!({"key": "value"}),
        )
        .await;
    assert_matches!(
        res,
        Err(WebhookDeliveryError::SendError(WebhookSendError::FailedToConnect(WebhookSendFailedToConnectError { target_url, .. })))
            if target_url == Url::parse("https://example.com/webhook").unwrap()
    );

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

    assert_eq!(delivery.response, None);

    assert!(!delivery.is_successful());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deliver_webhook_bad_status_failure() {
    let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
    let webhook_delivery_id = WebhookDeliveryID::new(uuid::Uuid::new_v4());

    let mut mock_webhook_sender = MockWebhookSender::new();
    let target_url = url::Url::parse("https://example.com/webhook").unwrap();
    TestWebhookDeliveryWorkerHarness::add_bad_status_sender_expectation(
        &mut mock_webhook_sender,
        &target_url,
        webhook_delivery_id,
        subscription_id,
        http::StatusCode::BAD_REQUEST,
    );

    let harness = TestWebhookDeliveryWorkerHarness::new(mock_webhook_sender);

    harness.new_webhook_subscription(subscription_id).await;

    let res = harness
        .webhook_delivery_worker
        .deliver_webhook(
            webhook_delivery_id,
            subscription_id,
            WebhookEventTypeCatalog::dataset_ref_updated(),
            serde_json::json!({"key": "value"}),
        )
        .await;
    assert_matches!(
        res,
        Err(WebhookDeliveryError::UnsuccessfulResponse(
            WebhookUnsuccessfulResponseError {
                status_code,
                target_url: a_target_url
            }
        )) if status_code == http::StatusCode::BAD_REQUEST &&
            a_target_url == target_url
    );

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

    assert_matches!(
        delivery.response,
        Some(WebhookResponse {
            status_code: http::StatusCode::BAD_REQUEST,
            ..
        })
    );

    assert!(!delivery.is_successful());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestWebhookDeliveryWorkerHarness {
    webhook_subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
    webhook_delivery_repo: Arc<dyn WebhookDeliveryRepository>,
    webhook_delivery_worker: Arc<dyn WebhookDeliveryWorker>,
    webhook_secret_generator: Arc<dyn WebhookSecretGenerator>,
}

impl TestWebhookDeliveryWorkerHarness {
    fn new(mock_sender: MockWebhookSender) -> Self {
        let mut b = dill::CatalogBuilder::new();
        b.add::<WebhookDeliveryWorkerImpl>()
            .add::<WebhookSignerImpl>()
            .add::<WebhookSecretGeneratorImpl>()
            .add_value(WebhooksConfig::default())
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
            webhook_secret_generator: catalog.get_one().unwrap(),
        }
    }

    async fn new_webhook_subscription(&self, subscription_id: WebhookSubscriptionID) {
        let mut subscription = WebhookSubscription::new(
            subscription_id,
            Url::parse("https://example.com/webhook").unwrap(),
            WebhookSubscriptionLabel::try_new("test".to_string()).unwrap(),
            None,
            vec![WebhookEventTypeCatalog::test()],
        );
        subscription
            .create_secret(self.webhook_secret_generator.generate_secret().unwrap())
            .unwrap();

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
        Self::add_webhook_sender_expectation(
            mock_webhook_sender,
            target_url,
            delivery_id,
            subscription_id,
            Ok(WebhookResponse::new(
                http::StatusCode::OK,
                http::HeaderMap::new(),
                "OK".to_string(),
                Utc::now(),
            )),
        );
    }

    fn add_connection_failure_sender_expectation(
        mock_webhook_sender: &mut MockWebhookSender,
        target_url: Url,
        delivery_id: WebhookDeliveryID,
        subscription_id: WebhookSubscriptionID,
    ) {
        Self::add_webhook_sender_expectation(
            mock_webhook_sender,
            target_url.clone(),
            delivery_id,
            subscription_id,
            Err(WebhookSendError::FailedToConnect(
                WebhookSendFailedToConnectError { target_url },
            )),
        );
    }

    fn add_timeout_sender_expectation(
        mock_webhook_sender: &mut MockWebhookSender,
        target_url: Url,
        delivery_id: WebhookDeliveryID,
        subscription_id: WebhookSubscriptionID,
    ) {
        Self::add_webhook_sender_expectation(
            mock_webhook_sender,
            target_url.clone(),
            delivery_id,
            subscription_id,
            Err(WebhookSendError::ConnectionTimeout(
                WebhookSendConnectionTimeoutError {
                    target_url,
                    timeout: std::time::Duration::from_secs(10),
                },
            )),
        );
    }

    fn add_bad_status_sender_expectation(
        mock_webhook_sender: &mut MockWebhookSender,
        target_url: &Url,
        delivery_id: WebhookDeliveryID,
        subscription_id: WebhookSubscriptionID,
        status_code: http::StatusCode,
    ) {
        Self::add_webhook_sender_expectation(
            mock_webhook_sender,
            target_url.clone(),
            delivery_id,
            subscription_id,
            Ok(WebhookResponse::new(
                status_code,
                http::HeaderMap::new(),
                "Some Bad Response".to_string(),
                Utc::now(),
            )),
        );
    }

    fn add_webhook_sender_expectation(
        mock_webhook_sender: &mut MockWebhookSender,
        target_url: Url,
        delivery_id: WebhookDeliveryID,
        subscription_id: WebhookSubscriptionID,
        expected_result: Result<WebhookResponse, WebhookSendError>,
    ) {
        mock_webhook_sender
            .expect_send_webhook()
            .times(1)
            .withf(move |url, _, headers| {
                assert_eq!(target_url, *url);
                Self::assert_webhook_sender_headers(headers, delivery_id, subscription_id);
                true
            })
            .return_once(move |_, _, _| expected_result);
    }

    fn assert_webhook_sender_headers(
        headers: &http::HeaderMap,
        delivery_id: WebhookDeliveryID,
        subscription_id: WebhookSubscriptionID,
    ) {
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
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
