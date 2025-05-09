// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use futures::TryStreamExt;
use kamu_webhooks::*;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_no_events_initially(catalog: &dill::Catalog) {
    let event_store = catalog
        .get_one::<dyn WebhookSubscriptionEventStore>()
        .unwrap();

    let len = event_store.len().await.unwrap();
    assert_eq!(len, 0);

    let non_existent_id = WebhookSubscriptionId::new(uuid::Uuid::new_v4());

    let res = event_store
        .get_events(&non_existent_id, GetEventsOpts::default())
        .try_collect::<Vec<_>>()
        .await;
    assert_matches!(res, Ok(events) if events.is_empty());

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"dummy");

    let res = event_store
        .count_subscriptions_by_dataset(&dataset_id)
        .await;
    assert_matches!(res, Ok(count) if count == 0);

    let res = event_store
        .list_subscription_ids_by_dataset(&dataset_id)
        .await;
    assert_matches!(res, Ok(ids) if ids.is_empty());

    let res = event_store
        .find_subscription_id_by_dataset_and_label(
            &dataset_id,
            &WebhookSubscriptionLabel::new("test-label"),
        )
        .await;
    assert_matches!(res, Ok(id) if id.is_none());

    let res = event_store
        .list_subscription_ids_by_dataset_and_event_type(
            &dataset_id,
            WebhookEventTypeCatalog::dataset_head_updated(),
        )
        .await;
    assert_matches!(res, Ok(ids) if ids.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_store_single_aggregate(catalog: &dill::Catalog) {
    let event_store = catalog
        .get_one::<dyn WebhookSubscriptionEventStore>()
        .unwrap();

    let subscription_id = WebhookSubscriptionId::new(uuid::Uuid::new_v4());

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"dummy");

    let mut subscription = WebhookSubscription::new(
        subscription_id,
        Url::parse("https://example.com").unwrap(),
        WebhookSubscriptionLabel::new("test-label"),
        Some(dataset_id.clone()),
        vec![WebhookEventTypeCatalog::dataset_head_updated()],
        WebhookSubscriptionSecret::try_new("secret").unwrap(),
    );
    subscription.save(event_store.as_ref()).await.unwrap();

    subscription.enable();
    subscription.pause();
    subscription.resume();

    subscription.save(event_store.as_ref()).await.unwrap();

    let len = event_store.len().await.unwrap();
    assert_eq!(len, 4);

    let res = event_store
        .get_events(&subscription_id, GetEventsOpts::default())
        .try_collect::<Vec<_>>()
        .await;
    assert!(res.is_ok());

    let events = res
        .unwrap()
        .into_iter()
        .map(|(__, e)| e)
        .collect::<Vec<_>>();

    assert_eq!(events.len(), 4);
    assert_matches!(events[0], WebhookSubscriptionEvent::Created(_));
    assert_matches!(events[1], WebhookSubscriptionEvent::Enabled(_));
    assert_matches!(events[2], WebhookSubscriptionEvent::Paused(_));
    assert_matches!(events[3], WebhookSubscriptionEvent::Resumed(_));

    let res = event_store
        .count_subscriptions_by_dataset(&dataset_id)
        .await;
    assert_matches!(res, Ok(count) if count == 1);

    let res = event_store
        .list_subscription_ids_by_dataset(&dataset_id)
        .await;
    assert_matches!(res, Ok(ids) if ids.len() == 1 && ids[0] == subscription_id);

    let res = event_store
        .find_subscription_id_by_dataset_and_label(
            &dataset_id,
            &WebhookSubscriptionLabel::new("test-label"),
        )
        .await;
    assert_matches!(res, Ok(id) if id.is_some() && id.unwrap() == subscription_id);

    let res = event_store
        .find_subscription_id_by_dataset_and_label(
            &dataset_id,
            &WebhookSubscriptionLabel::new("wrong-label"),
        )
        .await;
    assert_matches!(res, Ok(id) if id.is_none());

    let res = event_store
        .list_subscription_ids_by_dataset_and_event_type(
            &dataset_id,
            WebhookEventTypeCatalog::dataset_head_updated(),
        )
        .await;
    assert_matches!(res, Ok(ids) if ids.len() == 1 && ids[0] == subscription_id);

    let res = event_store
        .list_subscription_ids_by_dataset_and_event_type(
            &dataset_id,
            WebhookEventTypeCatalog::test(),
        )
        .await;
    assert_matches!(res, Ok(ids) if ids.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_store_multiple_aggregates(catalog: &dill::Catalog) {
    let event_store = catalog
        .get_one::<dyn WebhookSubscriptionEventStore>()
        .unwrap();

    let subscription_id_1 = WebhookSubscriptionId::new(uuid::Uuid::new_v4());
    let subscription_id_2 = WebhookSubscriptionId::new(uuid::Uuid::new_v4());

    let target_url_2 = Url::parse("https://example.com/2").unwrap();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"dummy");

    let secret_2 = WebhookSubscriptionSecret::try_new("secret_2").unwrap();

    let mut subscription_1 = WebhookSubscription::new(
        subscription_id_1,
        Url::parse("https://example.com/1").unwrap(),
        WebhookSubscriptionLabel::new("label-1"),
        Some(dataset_id.clone()),
        vec![WebhookEventTypeCatalog::dataset_head_updated()],
        WebhookSubscriptionSecret::try_new("secret_1").unwrap(),
    );
    subscription_1.enable();
    subscription_1.save(event_store.as_ref()).await.unwrap();

    let mut subscription_2 = WebhookSubscription::new(
        subscription_id_2,
        target_url_2,
        WebhookSubscriptionLabel::new("label-2"),
        Some(dataset_id.clone()),
        vec![WebhookEventTypeCatalog::dataset_head_updated()],
        secret_2.clone(),
    );
    subscription_2.enable();
    subscription_2.pause();
    subscription_2.save(event_store.as_ref()).await.unwrap();

    let len = event_store.len().await.unwrap();
    assert_eq!(len, 5);

    let events_1 = event_store
        .get_events(&subscription_id_1, GetEventsOpts::default())
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|(__, e)| e)
        .collect::<Vec<_>>();

    assert_eq!(events_1.len(), 2);
    assert_matches!(events_1[0], WebhookSubscriptionEvent::Created(_));
    assert_matches!(events_1[1], WebhookSubscriptionEvent::Enabled(_));

    let events_2 = event_store
        .get_events(&subscription_id_2, GetEventsOpts::default())
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|(__, e)| e)
        .collect::<Vec<_>>();

    assert_eq!(events_2.len(), 3);
    assert_matches!(events_2[0], WebhookSubscriptionEvent::Created(_));
    assert_matches!(events_2[1], WebhookSubscriptionEvent::Enabled(_));
    assert_matches!(events_2[2], WebhookSubscriptionEvent::Paused(_));

    let res = event_store
        .count_subscriptions_by_dataset(&dataset_id)
        .await;
    assert_matches!(res, Ok(count) if count == 2);

    let res = event_store
        .list_subscription_ids_by_dataset(&dataset_id)
        .await;
    assert_matches!(res, Ok(ids) if ids.len() == 2 && ids[0] == subscription_id_1 && ids[1] == subscription_id_2);

    let res = event_store
        .find_subscription_id_by_dataset_and_label(
            &dataset_id,
            &WebhookSubscriptionLabel::new("label-1"),
        )
        .await;
    assert_matches!(res, Ok(id) if id.is_some() && id.unwrap() == subscription_id_1);

    let res = event_store
        .find_subscription_id_by_dataset_and_label(
            &dataset_id,
            &WebhookSubscriptionLabel::new("label-2"),
        )
        .await;
    assert_matches!(res, Ok(id) if id.is_some() && id.unwrap() == subscription_id_2);

    let res = event_store
        .find_subscription_id_by_dataset_and_label(
            &dataset_id,
            &WebhookSubscriptionLabel::new("wrong-label"),
        )
        .await;
    assert_matches!(res, Ok(id) if id.is_none());

    let res = event_store
        .list_subscription_ids_by_dataset_and_event_type(
            &dataset_id,
            WebhookEventTypeCatalog::dataset_head_updated(),
        )
        .await;
    assert_matches!(res, Ok(ids) if ids.len() == 2 && ids[0] == subscription_id_1 && ids[1] == subscription_id_2);

    let res = event_store
        .list_subscription_ids_by_dataset_and_event_type(
            &dataset_id,
            WebhookEventTypeCatalog::test(),
        )
        .await;
    assert_matches!(res, Ok(ids) if ids.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_non_unique_labels(catalog: &dill::Catalog) {
    let event_store = catalog
        .get_one::<dyn WebhookSubscriptionEventStore>()
        .unwrap();

    let subscription_id_1 = WebhookSubscriptionId::new(uuid::Uuid::new_v4());
    let subscription_id_2_1 = WebhookSubscriptionId::new(uuid::Uuid::new_v4());
    let subscription_id_2_2 = WebhookSubscriptionId::new(uuid::Uuid::new_v4());

    let dataset_id_1 = odf::DatasetID::new_seeded_ed25519(b"dummy-1");
    let dataset_id_2 = odf::DatasetID::new_seeded_ed25519(b"dummy-2");

    let mut subscription_1 = WebhookSubscription::new(
        subscription_id_1,
        Url::parse("https://example.com/1").unwrap(),
        WebhookSubscriptionLabel::new("test-label"),
        Some(dataset_id_1.clone()),
        vec![WebhookEventTypeCatalog::dataset_head_updated()],
        WebhookSubscriptionSecret::try_new("secret_1").unwrap(),
    );
    subscription_1.save(event_store.as_ref()).await.unwrap();

    let mut subscription_2_1 = WebhookSubscription::new(
        subscription_id_2_1,
        Url::parse("https://example.com/2/1").unwrap(),
        WebhookSubscriptionLabel::new("test-label"), // same label, but different dataset
        Some(dataset_id_2.clone()),
        vec![WebhookEventTypeCatalog::dataset_head_updated()],
        WebhookSubscriptionSecret::try_new("secret_2_1").unwrap(),
    );
    subscription_2_1.save(event_store.as_ref()).await.unwrap();

    let mut subscription_2_2 = WebhookSubscription::new(
        subscription_id_2_2,
        Url::parse("https://example.com/2/2").unwrap(),
        WebhookSubscriptionLabel::new("test-label"), // same label in the same dataset
        Some(dataset_id_2.clone()),
        vec![WebhookEventTypeCatalog::dataset_head_updated()],
        WebhookSubscriptionSecret::try_new("secret_2_2").unwrap(),
    );
    let res = subscription_2_2.save(event_store.as_ref()).await;
    assert_matches!(res, Err(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO
// - encrypt secret!

// - test multiple event types filtered properly for same dataset
// - test non-unique labels in edit event
// - test get_events_multi
// - test removed subscriptions are filtered out
