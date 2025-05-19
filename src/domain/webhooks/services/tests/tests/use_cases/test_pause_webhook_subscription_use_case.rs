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
    PauseWebhookSubscriptionError,
    PauseWebhookSubscriptionUseCase,
    WebhookSubscriptionStatus,
};
use kamu_webhooks_services::PauseWebhookSubscriptionUseCaseImpl;

use super::WebhookSubscriptionUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_success() {
    let harness = PauseWebhookSubscriptionUseCaseHarness::new();
    let mut subscription = harness.create_subscription().await;
    subscription.enable().unwrap();

    let res = harness.use_case.execute(&mut subscription).await;
    assert!(res.is_ok(), "Failed to pause subscription: {res:?}",);
    assert_eq!(subscription.status(), WebhookSubscriptionStatus::Paused);

    subscription.resume().unwrap();

    let res = harness.use_case.execute(&mut subscription).await;
    assert!(res.is_ok(), "Failed to pause subscription: {res:?}",);
    assert_eq!(subscription.status(), WebhookSubscriptionStatus::Paused);

    subscription.mark_unreachable().unwrap();
    subscription.reactivate().unwrap();

    let res = harness.use_case.execute(&mut subscription).await;
    assert!(res.is_ok(), "Failed to pause subscription: {res:?}",);
    assert_eq!(subscription.status(), WebhookSubscriptionStatus::Paused);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_idempotence() {
    let harness = PauseWebhookSubscriptionUseCaseHarness::new();
    let mut subscription = harness.create_subscription().await;
    subscription.enable().unwrap();

    let res = harness.use_case.execute(&mut subscription).await;
    assert!(res.is_ok(), "Failed to pause subscription: {res:?}",);

    let res = harness.use_case.execute(&mut subscription).await;
    assert!(res.is_ok(), "Second pause not ignored: {res:?}",);
    assert_eq!(subscription.status(), WebhookSubscriptionStatus::Paused);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_unexpected() {
    let harness = PauseWebhookSubscriptionUseCaseHarness::new();
    let mut subscription = harness.create_subscription().await;

    let res = harness.use_case.execute(&mut subscription).await;
    assert_matches!(
        res,
        Err(PauseWebhookSubscriptionError::PauseUnexpected(e))
            if e.status == WebhookSubscriptionStatus::Unverified, "Unexpected error: {res:?}",
    );

    subscription.enable().unwrap();
    subscription.mark_unreachable().unwrap();

    let res = harness.use_case.execute(&mut subscription).await;
    assert_matches!(
        res,
        Err(PauseWebhookSubscriptionError::PauseUnexpected(e))
            if e.status == WebhookSubscriptionStatus::Unreachable, "Unexpected error: {res:?}",
    );

    subscription.remove().unwrap();
    let res = harness.use_case.execute(&mut subscription).await;
    assert_matches!(
        res,
        Err(PauseWebhookSubscriptionError::PauseUnexpected(e))
            if e.status == WebhookSubscriptionStatus::Removed, "Unexpected error: {res:?}",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(WebhookSubscriptionUseCaseHarness, base_harness)]
struct PauseWebhookSubscriptionUseCaseHarness {
    base_harness: WebhookSubscriptionUseCaseHarness,
    use_case: Arc<dyn PauseWebhookSubscriptionUseCase>,
}

impl PauseWebhookSubscriptionUseCaseHarness {
    fn new() -> Self {
        let base_harness = WebhookSubscriptionUseCaseHarness::new();

        let mut b = CatalogBuilder::new_chained(base_harness.catalog());
        b.add::<PauseWebhookSubscriptionUseCaseImpl>();

        let catalog = b.build();

        Self {
            base_harness,
            use_case: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
