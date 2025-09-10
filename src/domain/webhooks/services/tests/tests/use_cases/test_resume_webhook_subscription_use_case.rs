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

use dill::*;
use kamu_webhooks::{
    ResumeWebhookSubscriptionError,
    ResumeWebhookSubscriptionUseCase,
    WebhookEventTypeCatalog,
    WebhookSubscriptionStatus,
};
use kamu_webhooks_services::ResumeWebhookSubscriptionUseCaseImpl;
use messaging_outbox::{MockOutbox, Outbox};

use super::WebhookSubscriptionUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_resume_success() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mut mock_outbox = MockOutbox::new();
    WebhookSubscriptionUseCaseHarness::expect_webhook_event_enabled_message(
        &mut mock_outbox,
        &WebhookEventTypeCatalog::test(),
        Some(&foo_id),
        1,
    );

    let harness = ResumeWebhookSubscriptionUseCaseHarness::new(mock_outbox);
    let mut subscription = harness.create_subscription_in_dataset(foo_id).await;
    subscription.enable().unwrap();
    subscription.pause().unwrap();

    let res = harness.use_case.execute(&mut subscription).await;
    assert!(res.is_ok(), "Failed to resume subscription: {res:?}",);
    assert_eq!(subscription.status(), WebhookSubscriptionStatus::Enabled);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_resume_idempotence() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mut mock_outbox = MockOutbox::new();
    WebhookSubscriptionUseCaseHarness::expect_webhook_event_enabled_message(
        &mut mock_outbox,
        &WebhookEventTypeCatalog::test(),
        Some(&foo_id),
        1,
    );

    let harness = ResumeWebhookSubscriptionUseCaseHarness::new(mock_outbox);
    let mut subscription = harness.create_subscription_in_dataset(foo_id).await;
    subscription.enable().unwrap();
    subscription.pause().unwrap();

    let res = harness.use_case.execute(&mut subscription).await;
    assert!(res.is_ok(), "Failed to resume subscription: {res:?}",);

    let res = harness.use_case.execute(&mut subscription).await;
    assert!(res.is_ok(), "Second resume not ignored: {res:?}",);
    assert_eq!(subscription.status(), WebhookSubscriptionStatus::Enabled);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_resume_unexpected() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = ResumeWebhookSubscriptionUseCaseHarness::new(MockOutbox::new());
    let mut subscription = harness.create_subscription_in_dataset(foo_id).await;

    let res = harness.use_case.execute(&mut subscription).await;
    assert_matches!(
        res,
        Err(ResumeWebhookSubscriptionError::ResumeUnexpected(e))
            if e.status == WebhookSubscriptionStatus::Unverified, "Unexpected error: {res:?}",
    );

    subscription.enable().unwrap();
    subscription.mark_unreachable().unwrap();

    let res = harness.use_case.execute(&mut subscription).await;
    assert_matches!(
        res,
        Err(ResumeWebhookSubscriptionError::ResumeUnexpected(e))
            if e.status == WebhookSubscriptionStatus::Unreachable, "Unexpected error: {res:?}",
    );

    subscription.remove().unwrap();
    let res = harness.use_case.execute(&mut subscription).await;
    assert_matches!(
        res,
        Err(ResumeWebhookSubscriptionError::ResumeUnexpected(e))
            if e.status == WebhookSubscriptionStatus::Removed, "Unexpected error: {res:?}",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(WebhookSubscriptionUseCaseHarness, base_harness)]
struct ResumeWebhookSubscriptionUseCaseHarness {
    base_harness: WebhookSubscriptionUseCaseHarness,
    use_case: Arc<dyn ResumeWebhookSubscriptionUseCase>,
}

impl ResumeWebhookSubscriptionUseCaseHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let base_harness = WebhookSubscriptionUseCaseHarness::new();

        let mut b = CatalogBuilder::new_chained(base_harness.catalog());
        b.add::<ResumeWebhookSubscriptionUseCaseImpl>();
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
