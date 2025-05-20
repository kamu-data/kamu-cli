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
use kamu_webhooks::RemoveWebhookSubscriptionUseCase;
use kamu_webhooks_services::RemoveWebhookSubscriptionUseCaseImpl;

use super::WebhookSubscriptionUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_remove_subscription_success() {
    let harness = RemoveWebhookSubscriptionUseCaseHarness::new();
    let mut subscription = harness.create_subscription().await;

    let res = harness.use_case.execute(&mut subscription).await;
    assert!(res.is_ok(), "Failed to remove subscription: {res:?}",);

    let res = harness.find_subscription(subscription.id()).await;
    assert!(res.is_none(), "Subscription was not removed: {res:?}",);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_remove_idempotence() {
    let harness = RemoveWebhookSubscriptionUseCaseHarness::new();
    let mut subscription = harness.create_subscription().await;

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
    fn new() -> Self {
        let base_harness = WebhookSubscriptionUseCaseHarness::new();

        let mut b = CatalogBuilder::new_chained(base_harness.catalog());
        b.add::<RemoveWebhookSubscriptionUseCaseImpl>();

        let catalog = b.build();

        Self {
            base_harness,
            use_case: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
