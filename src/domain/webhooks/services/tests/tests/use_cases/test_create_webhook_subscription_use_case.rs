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

use dill::CatalogBuilder;
use kamu_webhooks::{
    CreateWebhookSubscriptionUseCase,
    WebhookEventTypeCatalog,
    WebhookSubscriptionLabel,
};
use kamu_webhooks_services::{CreateWebhookSubscriptionUseCaseImpl, WebhookSecretGeneratorImpl};

use super::WebhookSubscriptionUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_in_dataset_success() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = CreateWebhookSubscriptionUseCaseHarness::new();

    let res = harness
        .use_case
        .execute(
            Some(dataset_id),
            url::Url::parse("https://example.com").unwrap(),
            vec![WebhookEventTypeCatalog::test()],
            WebhookSubscriptionLabel::new("test_label"),
        )
        .await;
    assert!(res.is_ok(), "Failed to create subscription: {res:?}",);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_invalid_target_url_rejected() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = CreateWebhookSubscriptionUseCaseHarness::new();

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
                WebhookSubscriptionLabel::new("test_label"),
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

    let harness = CreateWebhookSubscriptionUseCaseHarness::new();

    let res = harness
        .use_case
        .execute(
            Some(dataset_id.clone()),
            url::Url::parse("https://example.com/webhook").unwrap(),
            vec![],
            WebhookSubscriptionLabel::new("test_label"),
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

    let harness = CreateWebhookSubscriptionUseCaseHarness::new();

    let res = harness
        .use_case
        .execute(
            Some(dataset_id.clone()),
            url::Url::parse("https://example.com/webhook").unwrap(),
            vec![
                WebhookEventTypeCatalog::test(),
                WebhookEventTypeCatalog::test(),
            ],
            WebhookSubscriptionLabel::new("test_label"),
        )
        .await;
    assert!(res.is_ok(), "Failed to create subscription: {res:?}",);

    // Find the subscription and ensure it has only one event type

    let subscription = harness
        .find_subscription(res.unwrap().subscription_id)
        .await
        .unwrap();

    assert_eq!(subscription.event_types().len(), 1,);
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

    let harness = CreateWebhookSubscriptionUseCaseHarness::new();

    // First subscription
    let res = harness
        .use_case
        .execute(
            Some(dataset_id_1.clone()),
            url::Url::parse("https://example.com/webhook/1").unwrap(),
            vec![WebhookEventTypeCatalog::test()],
            WebhookSubscriptionLabel::new("test_label"),
        )
        .await;
    assert!(res.is_ok(), "Failed to create subscription: {res:?}",);

    // Second subscription with the same label in the same dataset => error
    let res = harness
        .use_case
        .execute(
            Some(dataset_id_1),
            url::Url::parse("https://example.com/webhook/2").unwrap(),
            vec![WebhookEventTypeCatalog::test()],
            WebhookSubscriptionLabel::new("test_label"),
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
            WebhookSubscriptionLabel::new("test_label"),
        )
        .await;
    assert!(res.is_ok(), "Failed to create subscription: {res:?}",);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(WebhookSubscriptionUseCaseHarness, base_harness)]
struct CreateWebhookSubscriptionUseCaseHarness {
    base_harness: WebhookSubscriptionUseCaseHarness,
    use_case: Arc<dyn CreateWebhookSubscriptionUseCase>,
}

impl CreateWebhookSubscriptionUseCaseHarness {
    fn new() -> Self {
        let base_harness = WebhookSubscriptionUseCaseHarness::new();

        let mut b = CatalogBuilder::new_chained(base_harness.catalog());
        b.add::<CreateWebhookSubscriptionUseCaseImpl>();
        b.add::<WebhookSecretGeneratorImpl>();

        let catalog = b.build();

        Self {
            base_harness,
            use_case: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
