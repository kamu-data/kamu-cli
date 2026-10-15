// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use kamu_webhooks::*;
use serde_json::json;

use crate::helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_no_watermark_initially(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookWatermarkRepository>().unwrap();
    let subscription_id = helpers::new_webhook_subscription(catalog).await;

    let watermark = repo
        .find_watermark(subscription_id, &WebhookEventTypeCatalog::test())
        .await
        .unwrap();
    assert_eq!(watermark, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_set_and_overwrite_watermark(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookWatermarkRepository>().unwrap();
    let subscription_id = helpers::new_webhook_subscription(catalog).await;
    let event_type = WebhookEventTypeCatalog::test();

    for position in [1, 2] {
        let watermark = WebhookWatermark::from(json!({ "position": position }));
        repo.set_watermark(subscription_id, &event_type, watermark.clone())
            .await
            .unwrap();

        let found = repo
            .find_watermark(subscription_id, &event_type)
            .await
            .unwrap();
        assert_eq!(found, Some(watermark));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_watermarks_are_per_subscription_and_event_type(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookWatermarkRepository>().unwrap();
    let subscription_id_1 = helpers::new_webhook_subscription(catalog).await;
    let subscription_id_2 = helpers::new_webhook_subscription(catalog).await;
    let test_event_type = WebhookEventTypeCatalog::test();
    let dataset_event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    let watermark = WebhookWatermark::from(json!({ "position": 1 }));
    repo.set_watermark(subscription_id_1, &test_event_type, watermark.clone())
        .await
        .unwrap();

    assert_eq!(
        repo.find_watermark(subscription_id_1, &test_event_type)
            .await
            .unwrap(),
        Some(watermark)
    );
    assert_eq!(
        repo.find_watermark(subscription_id_1, &dataset_event_type)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        repo.find_watermark(subscription_id_2, &test_event_type)
            .await
            .unwrap(),
        None
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_remove_watermark_of_one_event_type(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookWatermarkRepository>().unwrap();
    let subscription_id = helpers::new_webhook_subscription(catalog).await;
    let test_event_type = WebhookEventTypeCatalog::test();
    let dataset_event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    let watermark = WebhookWatermark::from(json!({ "position": 1 }));
    for event_type in [&test_event_type, &dataset_event_type] {
        repo.set_watermark(subscription_id, event_type, watermark.clone())
            .await
            .unwrap();
    }

    repo.remove_watermark(subscription_id, &test_event_type)
        .await
        .unwrap();

    assert_eq!(
        repo.find_watermark(subscription_id, &test_event_type)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        repo.find_watermark(subscription_id, &dataset_event_type)
            .await
            .unwrap(),
        Some(watermark)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_remove_watermarks_of_subscription(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookWatermarkRepository>().unwrap();
    let removed_subscription_id = helpers::new_webhook_subscription(catalog).await;
    let kept_subscription_id = helpers::new_webhook_subscription(catalog).await;
    let test_event_type = WebhookEventTypeCatalog::test();
    let dataset_event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    let watermark = WebhookWatermark::from(json!({ "position": 1 }));
    for subscription_id in [removed_subscription_id, kept_subscription_id] {
        for event_type in [&test_event_type, &dataset_event_type] {
            repo.set_watermark(subscription_id, event_type, watermark.clone())
                .await
                .unwrap();
        }
    }

    repo.remove_watermarks(removed_subscription_id)
        .await
        .unwrap();

    for event_type in [&test_event_type, &dataset_event_type] {
        assert_eq!(
            repo.find_watermark(removed_subscription_id, event_type)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repo.find_watermark(kept_subscription_id, event_type)
                .await
                .unwrap(),
            Some(watermark.clone())
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
