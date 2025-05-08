// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_task_system as ts;
use kamu_webhooks::{
    WebhookEvent,
    WebhookEventId,
    WebhookEventRepository,
    WebhookEventTypeCatalog,
    WebhookSubscription,
    WebhookSubscriptionEventStore,
    WebhookSubscriptionId,
    WebhookSubscriptionLabel,
};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn new_task(catalog: &dill::Catalog) -> ts::TaskID {
    let task_event_store = catalog.get_one::<dyn ts::TaskEventStore>().unwrap();

    let task_id = task_event_store.new_task_id().await.unwrap();

    let mut task = ts::Task::new(
        Utc::now(),
        task_id,
        ts::LogicalPlan::Probe(ts::LogicalPlanProbe::default()),
        None,
        ts::TaskRetryPolicy::default(),
    );

    task.save(task_event_store.as_ref()).await.unwrap();

    task_id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn new_webhook_event(catalog: &dill::Catalog) -> WebhookEventId {
    let webhook_event_repo = catalog.get_one::<dyn WebhookEventRepository>().unwrap();

    let webhook_event_id = WebhookEventId::new(uuid::Uuid::new_v4());
    let webhook_event = WebhookEvent::new(
        webhook_event_id,
        WebhookEventTypeCatalog::test(),
        serde_json::json!({
            "test": "test",
        }),
        Utc::now(),
    );

    webhook_event_repo
        .create_event(&webhook_event)
        .await
        .unwrap();

    webhook_event_id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn new_webhook_subscription(catalog: &dill::Catalog) -> WebhookSubscriptionId {
    let webhook_subscription_event_store = catalog
        .get_one::<dyn WebhookSubscriptionEventStore>()
        .unwrap();

    let webhook_subscription_id = WebhookSubscriptionId::new(uuid::Uuid::new_v4());

    let mut webhook_subscription = WebhookSubscription::new(
        webhook_subscription_id,
        Url::parse("https://example.com").unwrap(),
        WebhookSubscriptionLabel::new("test".to_string()),
        None,
        vec![WebhookEventTypeCatalog::test()],
        "some-secret".to_string(),
    );

    webhook_subscription
        .save(webhook_subscription_event_store.as_ref())
        .await
        .unwrap();

    webhook_subscription_id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
