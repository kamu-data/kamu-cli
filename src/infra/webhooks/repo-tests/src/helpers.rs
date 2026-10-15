// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_webhooks::{
    WebhookEventTypeCatalog,
    WebhookSubscription,
    WebhookSubscriptionEventStore,
    WebhookSubscriptionID,
    WebhookSubscriptionLabel,
    WebhookSubscriptionSecret,
};
use secrecy::SecretString;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn new_webhook_subscription(catalog: &dill::Catalog) -> WebhookSubscriptionID {
    let webhook_subscription_event_store = catalog
        .get_one::<dyn WebhookSubscriptionEventStore>()
        .unwrap();

    let webhook_subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());

    let mut webhook_subscription = WebhookSubscription::new(
        webhook_subscription_id,
        Url::parse("https://example.com").unwrap(),
        WebhookSubscriptionLabel::try_new("test".to_string()).unwrap(),
        None,
        vec![WebhookEventTypeCatalog::test()],
        WebhookSubscriptionSecret::try_new(None, &SecretString::from("some-secret")).unwrap(),
    );

    webhook_subscription
        .save(webhook_subscription_event_store.as_ref())
        .await
        .unwrap();

    webhook_subscription_id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
