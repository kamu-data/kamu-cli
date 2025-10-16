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
use kamu_webhooks::{RotateWebhookSubscriptionSecretUseCase, WebhooksConfig};
use kamu_webhooks_services::{
    RotateWebhookSubscriptionSecretUseCaseImpl,
    WebhookSecretGeneratorImpl,
};
use messaging_outbox::{MockOutbox, Outbox};

use super::WebhookSubscriptionUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_rotate_secret() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mut mock_outbox = MockOutbox::new();
    WebhookSubscriptionUseCaseHarness::expect_webhook_secret_rotated_message(&mut mock_outbox, 1);

    let harness = RotateWebhookSubscriptionSecretUseCaseHarness::new(mock_outbox);
    let mut subscription = harness.create_subscription_in_dataset(foo_id).await;
    subscription.enable().unwrap();
    let old_secret = subscription.secret().clone();

    let res = harness.use_case.execute(&mut subscription).await;
    assert!(res.is_ok(), "Failed to rotate subscription secret: {res:?}",);
    assert_ne!(subscription.secret(), &old_secret);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(WebhookSubscriptionUseCaseHarness, base_harness)]
struct RotateWebhookSubscriptionSecretUseCaseHarness {
    base_harness: WebhookSubscriptionUseCaseHarness,
    use_case: Arc<dyn RotateWebhookSubscriptionSecretUseCase>,
}

impl RotateWebhookSubscriptionSecretUseCaseHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let base_harness = WebhookSubscriptionUseCaseHarness::new();

        let mut b = CatalogBuilder::new_chained(base_harness.catalog());
        b.add::<RotateWebhookSubscriptionSecretUseCaseImpl>();
        b.add::<WebhookSecretGeneratorImpl>();
        b.add_value(WebhooksConfig::sample());
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
