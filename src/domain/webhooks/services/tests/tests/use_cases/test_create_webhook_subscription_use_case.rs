// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use dill::CatalogBuilder;
use kamu_webhooks::{
    CreateWebhookSubscriptionUseCase,
    WebhookEventTypeCatalog,
    WebhookSubscriptionLabel,
    WebhookSubscriptionQueryMode,
    WebhooksConfig,
};
use kamu_webhooks_services::{CreateWebhookSubscriptionUseCaseImpl, WebhookSecretGeneratorImpl};
use messaging_outbox::{MockOutbox, Outbox};

use super::WebhookSubscriptionUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_in_dataset_success() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mut mock_outbox = MockOutbox::new();
    WebhookSubscriptionUseCaseHarness::expect_webhook_event_enabled_message(
        &mut mock_outbox,
        &WebhookEventTypeCatalog::test(),
        Some(&dataset_id),
        1,
    );

    let harness = CreateWebhookSubscriptionUseCaseHarness::new(mock_outbox);
    let res = harness
        .use_case
        .execute(
            Some(dataset_id),
            url::Url::parse("https://example.com").unwrap(),
            vec![WebhookEventTypeCatalog::test()],
            WebhookSubscriptionLabel::try_new("test_label").unwrap(),
        )
        .await;
    assert!(res.is_ok(), "Failed to create subscription: {res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_invalid_target_url_rejected() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = CreateWebhookSubscriptionUseCaseHarness::new(MockOutbox::new());

    let invalid_urls = vec![
        "http://example.com",
        "https://localhost",
        "https://127.0.0.1",
        "https://[::1]",
        "https://[0000:0000:0000:0000:0000:0000:0000:0001]",
    ];

    for invalid_url in invalid_urls {
        let res = harness
            .use_case
            .execute(
                Some(dataset_id.clone()),
                url::Url::parse(invalid_url).unwrap(),
                vec![WebhookEventTypeCatalog::test()],
                WebhookSubscriptionLabel::try_new("test_label").unwrap(),
            )
            .await;
        assert_matches!(
            res,
            Err(kamu_webhooks::CreateWebhookSubscriptionError::InvalidTargetUrl(_))
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_loopback_target_url_accepted_when_loopback_only() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mut mock_outbox = MockOutbox::new();
    WebhookSubscriptionUseCaseHarness::expect_webhook_event_enabled_message(
        &mut mock_outbox,
        &WebhookEventTypeCatalog::test(),
        Some(&dataset_id),
        2,
    );

    let harness = CreateWebhookSubscriptionUseCaseHarness::with_config(
        mock_outbox,
        WebhooksConfig {
            loopback_targets_only: true,
            ..WebhooksConfig::default()
        },
    );

    for (label, loopback_url) in [
        ("http_ip", "http://127.0.0.1:8080/hook"),
        ("https_localhost", "https://localhost/hook"),
    ] {
        let res = harness
            .use_case
            .execute(
                Some(dataset_id.clone()),
                url::Url::parse(loopback_url).unwrap(),
                vec![WebhookEventTypeCatalog::test()],
                WebhookSubscriptionLabel::try_new(label).unwrap(),
            )
            .await;
        assert!(res.is_ok(), "Failed to create subscription: {res:?}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_external_target_url_rejected_when_loopback_only() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = CreateWebhookSubscriptionUseCaseHarness::with_config(
        MockOutbox::new(),
        WebhooksConfig {
            loopback_targets_only: true,
            ..WebhooksConfig::default()
        },
    );

    // Rejected whether or not the host resolves where the test runs, and on a
    // scheme other than `http` or `https`
    for rejected_url in [
        "https://example.com/hook",
        "http://192.0.2.1/hook",
        "https://unresolvable.invalid/hook",
        "ftp://127.0.0.1/hook",
    ] {
        let res = harness
            .use_case
            .execute(
                Some(dataset_id.clone()),
                url::Url::parse(rejected_url).unwrap(),
                vec![WebhookEventTypeCatalog::test()],
                WebhookSubscriptionLabel::try_new("test_label").unwrap(),
            )
            .await;
        assert_matches!(
            res,
            Err(kamu_webhooks::CreateWebhookSubscriptionError::InvalidTargetUrl(_))
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_no_event_types_rejected() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = CreateWebhookSubscriptionUseCaseHarness::new(MockOutbox::new());

    let res = harness
        .use_case
        .execute(
            Some(dataset_id.clone()),
            url::Url::parse("https://example.com/webhook").unwrap(),
            vec![],
            WebhookSubscriptionLabel::try_new("test_label").unwrap(),
        )
        .await;
    assert_matches!(
        res,
        Err(kamu_webhooks::CreateWebhookSubscriptionError::NoEventTypesProvided(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_event_types_deduplicated() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mut mock_outbox = MockOutbox::new();
    WebhookSubscriptionUseCaseHarness::expect_webhook_event_enabled_message(
        &mut mock_outbox,
        &WebhookEventTypeCatalog::test(),
        Some(&dataset_id),
        1,
    );

    let harness = CreateWebhookSubscriptionUseCaseHarness::new(mock_outbox);

    let res = harness
        .use_case
        .execute(
            Some(dataset_id.clone()),
            url::Url::parse("https://example.com/webhook").unwrap(),
            vec![
                WebhookEventTypeCatalog::test(),
                WebhookEventTypeCatalog::test(),
            ],
            WebhookSubscriptionLabel::try_new("test_label").unwrap(),
        )
        .await;
    assert!(res.is_ok(), "Failed to create subscription: {res:?}");

    // Find the subscription and ensure it has only one event type

    let subscription = harness
        .find_subscription(
            res.unwrap().subscription_id,
            WebhookSubscriptionQueryMode::Active,
        )
        .await
        .unwrap();

    assert_eq!(subscription.event_types().len(), 1);
    assert_eq!(
        subscription.event_types()[0],
        WebhookEventTypeCatalog::test()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_label_unique_in_dataset() {
    let dataset_id_1 = odf::DatasetID::new_seeded_ed25519(b"foo");
    let dataset_id_2 = odf::DatasetID::new_seeded_ed25519(b"bar");

    let mut mock_outbox = MockOutbox::new();
    WebhookSubscriptionUseCaseHarness::expect_webhook_event_enabled_message(
        &mut mock_outbox,
        &WebhookEventTypeCatalog::test(),
        Some(&dataset_id_1),
        1,
    );
    WebhookSubscriptionUseCaseHarness::expect_webhook_event_enabled_message(
        &mut mock_outbox,
        &WebhookEventTypeCatalog::test(),
        Some(&dataset_id_2),
        1,
    );

    let harness = CreateWebhookSubscriptionUseCaseHarness::new(mock_outbox);

    // First subscription
    let res = harness
        .use_case
        .execute(
            Some(dataset_id_1.clone()),
            url::Url::parse("https://example.com/webhook/1").unwrap(),
            vec![WebhookEventTypeCatalog::test()],
            WebhookSubscriptionLabel::try_new("test_label").unwrap(),
        )
        .await;
    assert!(res.is_ok(), "Failed to create subscription: {res:?}");

    // Second subscription with the same label in the same dataset => error
    let res = harness
        .use_case
        .execute(
            Some(dataset_id_1),
            url::Url::parse("https://example.com/webhook/2").unwrap(),
            vec![WebhookEventTypeCatalog::test()],
            WebhookSubscriptionLabel::try_new("test_label").unwrap(),
        )
        .await;
    assert_matches!(
        res,
        Err(kamu_webhooks::CreateWebhookSubscriptionError::DuplicateLabel(e))
            if e.label.as_ref() == "test_label",
    );

    // Third subscription with the same label in a different dataset => fine
    let res = harness
        .use_case
        .execute(
            Some(dataset_id_2),
            url::Url::parse("https://example.com/webhook/3").unwrap(),
            vec![WebhookEventTypeCatalog::test()],
            WebhookSubscriptionLabel::try_new("test_label").unwrap(),
        )
        .await;
    assert!(res.is_ok(), "Failed to create subscription: {res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(WebhookSubscriptionUseCaseHarness, base_harness)]
struct CreateWebhookSubscriptionUseCaseHarness {
    base_harness: WebhookSubscriptionUseCaseHarness,
    use_case: Arc<dyn CreateWebhookSubscriptionUseCase>,
}

impl CreateWebhookSubscriptionUseCaseHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        Self::with_config(mock_outbox, WebhooksConfig::default())
    }

    fn with_config(mock_outbox: MockOutbox, webhooks_config: WebhooksConfig) -> Self {
        let base_harness = WebhookSubscriptionUseCaseHarness::new();

        let mut b = CatalogBuilder::new_chained(base_harness.catalog());
        b.add::<CreateWebhookSubscriptionUseCaseImpl>();
        b.add::<WebhookSecretGeneratorImpl>();
        b.add_value(webhooks_config);
        b.add_value(mock_outbox);
        b.bind::<dyn Outbox, MockOutbox>();

        let catalog = b.build();

        Self {
            base_harness,
            use_case: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
