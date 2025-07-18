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
    WebhookEventID,
    WebhookEventRepository,
    WebhookEventTypeCatalog,
    WebhookSubscription,
    WebhookSubscriptionEventStore,
    WebhookSubscriptionID,
    WebhookSubscriptionLabel,
    WebhookSubscriptionSecret,
};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn new_task(catalog: &dill::Catalog) -> ts::TaskID {
    let task_event_store = catalog.get_one::<dyn ts::TaskEventStore>().unwrap();

    let task_id = task_event_store.new_task_id().await.unwrap();

    let plan = ts::LogicalPlanProbe::default().into_logical_plan();

    let mut task = ts::Task::new(Utc::now(), task_id, plan, None);

    task.save(task_event_store.as_ref()).await.unwrap();

    task_id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn new_webhook_event(catalog: &dill::Catalog) -> WebhookEventID {
    let webhook_event_repo = catalog.get_one::<dyn WebhookEventRepository>().unwrap();

    let webhook_event_id = WebhookEventID::new(uuid::Uuid::new_v4());
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
        WebhookSubscriptionSecret::try_new("some-secret").unwrap(),
    );

    webhook_subscription
        .save(webhook_subscription_event_store.as_ref())
        .await
        .unwrap();

    webhook_subscription_id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
