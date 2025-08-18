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
use kamu_webhooks::{
    RemoveWebhookSubscriptionUseCase,
    WebhookEventTypeCatalog,
    WebhookSubscriptionQueryMode,
};
use kamu_webhooks_services::RemoveWebhookSubscriptionUseCaseImpl;
use messaging_outbox::{MockOutbox, Outbox};

use super::WebhookSubscriptionUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_remove_subscription_success() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mut mock_outbox = MockOutbox::new();
    WebhookSubscriptionUseCaseHarness::expect_webhook_subscription_deleted_message(
        &mut mock_outbox,
        &WebhookEventTypeCatalog::test(),
        Some(&foo_id),
        1,
    );

    let harness = RemoveWebhookSubscriptionUseCaseHarness::new(mock_outbox);
    let mut subscription = harness.create_subscription_in_dataset(foo_id).await;

    let res = harness.use_case.execute(&mut subscription).await;
    assert!(res.is_ok(), "Failed to remove subscription: {res:?}",);

    let res = harness
        .find_subscription(subscription.id(), WebhookSubscriptionQueryMode::Active)
        .await;
    assert!(res.is_none(), "Subscription was not removed: {res:?}",);

    let res = harness
        .find_subscription(
            subscription.id(),
            WebhookSubscriptionQueryMode::IncludingRemoved,
        )
        .await;
    assert!(
        res.is_some(),
        "Subscription should be found in including removed mode: {res:?}",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_remove_idempotence() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mut mock_outbox = MockOutbox::new();
    WebhookSubscriptionUseCaseHarness::expect_webhook_subscription_deleted_message(
        &mut mock_outbox,
        &WebhookEventTypeCatalog::test(),
        Some(&foo_id),
        1,
    );

    let harness = RemoveWebhookSubscriptionUseCaseHarness::new(mock_outbox);
    let mut subscription = harness.create_subscription_in_dataset(foo_id).await;

    let res = harness.use_case.execute(&mut subscription).await;
    assert!(res.is_ok(), "Failed to remove subscription: {res:?}",);

    let res = harness.use_case.execute(&mut subscription).await;
    assert!(res.is_ok(), "Second removal not ignored: {res:?}",);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(WebhookSubscriptionUseCaseHarness, base_harness)]
struct RemoveWebhookSubscriptionUseCaseHarness {
    base_harness: WebhookSubscriptionUseCaseHarness,
    use_case: Arc<dyn RemoveWebhookSubscriptionUseCase>,
}

impl RemoveWebhookSubscriptionUseCaseHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let base_harness = WebhookSubscriptionUseCaseHarness::new();

        let mut b = CatalogBuilder::new_chained(base_harness.catalog());
        b.add::<RemoveWebhookSubscriptionUseCaseImpl>();
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
