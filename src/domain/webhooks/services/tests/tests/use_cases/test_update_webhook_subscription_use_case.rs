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
    UpdateWebhookSubscriptionUseCase,
    WebhookEventTypeCatalog,
    WebhookSubscriptionLabel,
    WebhookSubscriptionQueryMode,
};
use kamu_webhooks_services::UpdateWebhookSubscriptionUseCaseImpl;
use messaging_outbox::{MockOutbox, Outbox};

use super::WebhookSubscriptionUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_in_dataset_success() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mut mock_outbox = MockOutbox::new();
    WebhookSubscriptionUseCaseHarness::expect_webhook_event_disabled_message(
        &mut mock_outbox,
        &WebhookEventTypeCatalog::test(),
        Some(&foo_id),
        1,
    );
    WebhookSubscriptionUseCaseHarness::expect_webhook_event_enabled_message(
        &mut mock_outbox,
        &WebhookEventTypeCatalog::dataset_ref_updated(),
        Some(&foo_id),
        1,
    );

    let harness = UpdateWebhookSubscriptionUseCaseHarness::new(mock_outbox);
    let mut subscription = harness.create_subscription_in_dataset(foo_id).await;

    let res = harness
        .use_case
        .execute(
            &mut subscription,
            url::Url::parse("https://example.com/updated").unwrap(),
            vec![WebhookEventTypeCatalog::dataset_ref_updated()],
            WebhookSubscriptionLabel::try_new("test_label_updated").unwrap(),
        )
        .await;
    assert!(res.is_ok(), "Failed to update subscription: {res:?}",);

    assert_eq!(
        subscription.target_url().to_string(),
        "https://example.com/updated"
    );
    assert_eq!(
        subscription.event_types(),
        vec![WebhookEventTypeCatalog::dataset_ref_updated()]
    );
    assert_eq!(subscription.label().as_ref(), "test_label_updated");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_invalid_target_url_rejected() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = UpdateWebhookSubscriptionUseCaseHarness::new(MockOutbox::new());
    let mut subscription = harness.create_subscription_in_dataset(foo_id).await;

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
                &mut subscription,
                url::Url::parse(invalid_url).unwrap(),
                vec![WebhookEventTypeCatalog::test()],
                WebhookSubscriptionLabel::try_new("test_label").unwrap(),
            )
            .await;

        assert_matches!(
            res,
            Err(kamu_webhooks::UpdateWebhookSubscriptionError::InvalidTargetUrl(_))
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_no_event_types_rejected() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = UpdateWebhookSubscriptionUseCaseHarness::new(MockOutbox::new());
    let mut subscription = harness.create_subscription_in_dataset(foo_id).await;

    let res = harness
        .use_case
        .execute(
            &mut subscription,
            url::Url::parse("https://example.com").unwrap(),
            vec![],
            WebhookSubscriptionLabel::try_new("test_label").unwrap(),
        )
        .await;

    assert_matches!(
        res,
        Err(kamu_webhooks::UpdateWebhookSubscriptionError::NoEventTypesProvided(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_event_types_deduplicated() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = UpdateWebhookSubscriptionUseCaseHarness::new(MockOutbox::new());
    let mut subscription = harness.create_subscription_in_dataset(foo_id).await;

    let res = harness
        .use_case
        .execute(
            &mut subscription,
            url::Url::parse("https://example.com").unwrap(),
            vec![
                WebhookEventTypeCatalog::test(),
                WebhookEventTypeCatalog::test(),
            ],
            WebhookSubscriptionLabel::try_new("test_label").unwrap(),
        )
        .await;
    assert!(res.is_ok(), "Failed to update subscription: {res:?}",);

    // Find the subscription and ensure it has only one event type

    let subscription = harness
        .find_subscription(subscription.id(), WebhookSubscriptionQueryMode::Active)
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

    let harness = UpdateWebhookSubscriptionUseCaseHarness::new(MockOutbox::new());

    let _subscription_1_1 = harness
        .create_subscription_in_dataset_with_label(
            dataset_id_1.clone(),
            Some(WebhookSubscriptionLabel::try_new("test-label-1").unwrap()),
        )
        .await;
    let mut subscription_1_2 = harness
        .create_subscription_in_dataset_with_label(
            dataset_id_1,
            Some(WebhookSubscriptionLabel::try_new("test-label-2").unwrap()),
        )
        .await;
    let _subscription_2 = harness
        .create_subscription_in_dataset_with_label(
            dataset_id_2,
            Some(WebhookSubscriptionLabel::try_new("test-label-another").unwrap()),
        )
        .await;

    let res = harness
        .use_case
        .execute(
            &mut subscription_1_2,
            url::Url::parse("https://example.com/webhook/2").unwrap(),
            vec![WebhookEventTypeCatalog::test()],
            WebhookSubscriptionLabel::try_new("test-label-1").unwrap(),
        )
        .await;
    assert_matches!(
        res,
        Err(kamu_webhooks::UpdateWebhookSubscriptionError::DuplicateLabel(e))
            if e.label.as_ref() == "test-label-1"
    );

    let res = harness
        .use_case
        .execute(
            &mut subscription_1_2,
            url::Url::parse("https://example.com/webhook/2").unwrap(),
            vec![WebhookEventTypeCatalog::test()],
            WebhookSubscriptionLabel::try_new("test-label-another").unwrap(),
        )
        .await;
    assert!(res.is_ok(), "Failed to update subscription: {res:?}",);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_unexpected() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = UpdateWebhookSubscriptionUseCaseHarness::new(MockOutbox::new());
    let mut subscription = harness.create_subscription_in_dataset(foo_id).await;
    subscription.remove().unwrap();

    let res = harness
        .use_case
        .execute(
            &mut subscription,
            url::Url::parse("https://example.com").unwrap(),
            vec![WebhookEventTypeCatalog::test()],
            WebhookSubscriptionLabel::try_new("test_label").unwrap(),
        )
        .await;

    assert_matches!(
        res,
        Err(kamu_webhooks::UpdateWebhookSubscriptionError::UpdateUnexpected(e))
            if e.status == kamu_webhooks::WebhookSubscriptionStatus::Removed,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(WebhookSubscriptionUseCaseHarness, base_harness)]
struct UpdateWebhookSubscriptionUseCaseHarness {
    base_harness: WebhookSubscriptionUseCaseHarness,
    use_case: Arc<dyn UpdateWebhookSubscriptionUseCase>,
}

impl UpdateWebhookSubscriptionUseCaseHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let base_harness = WebhookSubscriptionUseCaseHarness::new();

        let mut b = CatalogBuilder::new_chained(base_harness.catalog());
        b.add::<UpdateWebhookSubscriptionUseCaseImpl>();
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
