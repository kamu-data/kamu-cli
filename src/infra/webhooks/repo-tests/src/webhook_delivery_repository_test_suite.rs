// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use chrono::{SubsecRound, Utc};
use database_common::PaginationOpts;
use dill::Catalog;
use kamu_task_system as ts;
use kamu_webhooks::*;

use crate::helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_no_webhook_deliveries_initially(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookDeliveryRepository>().unwrap();

    let task_id = helpers::new_task(catalog).await;

    let res = repo.list_by_task_id(task_id).await;
    assert_matches!(res, Ok(webhook_deliveries) if webhook_deliveries.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_save_webhook_delivery_start_and_success_response(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookDeliveryRepository>().unwrap();

    let webhook_subscription_id = helpers::new_webhook_subscription(catalog).await;
    let webhook_event_id = helpers::new_webhook_event(catalog).await;

    let task_id = helpers::new_task(catalog).await;
    let task_attempt_id = ts::TaskAttemptID::new(task_id, 0);

    let mut webhook_delivery =
        new_delivery(task_attempt_id, webhook_subscription_id, webhook_event_id);

    let res = repo.create(webhook_delivery.clone()).await;
    assert_matches!(res, Ok(_));

    let res = repo.list_by_task_id(task_id).await;
    assert_matches!(
        res,
        Ok(webhook_deliveries) if webhook_deliveries.len() == 1 && webhook_deliveries[0] == webhook_delivery
    );

    webhook_delivery.set_response(WebhookResponse {
        status_code: http::StatusCode::OK,
        headers: vec![(
            http::header::HeaderName::from_bytes(b"x-client-debug-Id").unwrap(),
            String::from("12345"),
        )],
        body: String::from("{ status: \"Success\" }"),
        finished_at: Utc::now().round_subsecs(6),
    });

    let res = repo
        .update_response(task_attempt_id, webhook_delivery.response.clone().unwrap())
        .await;
    assert_matches!(res, Ok(_));

    let res = repo.get_by_task_attempt_id(task_attempt_id).await;
    assert_matches!(res, Ok(Some(webhook_delivery_from_db)) if webhook_delivery_from_db == webhook_delivery);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_save_webhook_delivery_start_and_failure_response(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookDeliveryRepository>().unwrap();

    let webhook_subscription_id = helpers::new_webhook_subscription(catalog).await;
    let webhook_event_id = helpers::new_webhook_event(catalog).await;

    let task_id = helpers::new_task(catalog).await;
    let task_attempt_id = ts::TaskAttemptID::new(task_id, 0);

    let mut webhook_delivery =
        new_delivery(task_attempt_id, webhook_subscription_id, webhook_event_id);

    let res = repo.create(webhook_delivery.clone()).await;
    assert_matches!(res, Ok(_));

    webhook_delivery.set_response(WebhookResponse {
        status_code: http::StatusCode::INTERNAL_SERVER_ERROR,
        headers: vec![(
            http::header::HeaderName::from_bytes(b"x-client-debug-Id").unwrap(),
            String::from("12345"),
        )],
        body: String::from("{ status: \"Internal Error\" }"),
        finished_at: Utc::now().round_subsecs(6),
    });

    let res = repo
        .update_response(task_attempt_id, webhook_delivery.response.clone().unwrap())
        .await;
    assert_matches!(res, Ok(_));

    let res = repo.get_by_task_attempt_id(task_attempt_id).await;
    assert_matches!(res, Ok(Some(webhook_delivery_from_db)) if webhook_delivery_from_db == webhook_delivery);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_filter_webhook_deliveries_by_task_id(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookDeliveryRepository>().unwrap();

    let task_id_1 = helpers::new_task(catalog).await;
    let task_attempt_id_1_1 = ts::TaskAttemptID::new(task_id_1, 0);
    let task_attempt_id_1_2 = ts::TaskAttemptID::new(task_id_1, 1);

    let task_id_2 = helpers::new_task(catalog).await;
    let task_attempt_id_2_1 = ts::TaskAttemptID::new(task_id_2, 0);
    let task_attempt_id_2_2 = ts::TaskAttemptID::new(task_id_2, 1);

    let webhook_subscription_id = helpers::new_webhook_subscription(catalog).await;
    let webhook_event_id_1 = helpers::new_webhook_event(catalog).await;
    let webhook_event_id_2 = helpers::new_webhook_event(catalog).await;

    let webhook_delivery_1_1 = new_delivery(
        task_attempt_id_1_1,
        webhook_subscription_id,
        webhook_event_id_1,
    );
    let webhook_delivery_1_2 = new_delivery(
        task_attempt_id_1_2,
        webhook_subscription_id,
        webhook_event_id_1,
    );
    let webhook_delivery_2_1 = new_delivery(
        task_attempt_id_2_1,
        webhook_subscription_id,
        webhook_event_id_2,
    );
    let webhook_delivery_2_2 = new_delivery(
        task_attempt_id_2_2,
        webhook_subscription_id,
        webhook_event_id_2,
    );

    repo.create(webhook_delivery_1_1.clone()).await.unwrap();
    repo.create(webhook_delivery_1_2.clone()).await.unwrap();
    repo.create(webhook_delivery_2_1.clone()).await.unwrap();
    repo.create(webhook_delivery_2_2.clone()).await.unwrap();

    let res = repo.list_by_task_id(task_id_1).await;
    assert_matches!(
        res,
        Ok(webhook_deliveries) if webhook_deliveries.len() == 2
            && webhook_deliveries[0] == webhook_delivery_1_1
            && webhook_deliveries[1] == webhook_delivery_1_2
    );

    let res = repo.list_by_task_id(task_id_2).await;
    assert_matches!(
        res,
        Ok(webhook_deliveries) if webhook_deliveries.len() == 2
            && webhook_deliveries[0] == webhook_delivery_2_1
            && webhook_deliveries[1] == webhook_delivery_2_2
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_filter_webhook_deliveries_by_webhook_event_or_subscription_id(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookDeliveryRepository>().unwrap();

    let task_id_1 = helpers::new_task(catalog).await;
    let task_id_2 = helpers::new_task(catalog).await;
    let task_id_3 = helpers::new_task(catalog).await;
    let task_id_4 = helpers::new_task(catalog).await;

    let task_attempt_id_1 = ts::TaskAttemptID::new(task_id_1, 0);
    let task_attempt_id_2 = ts::TaskAttemptID::new(task_id_2, 0);
    let task_attempt_id_3 = ts::TaskAttemptID::new(task_id_3, 0);
    let task_attempt_id_4 = ts::TaskAttemptID::new(task_id_4, 0);

    let webhook_subscription_id_1 = helpers::new_webhook_subscription(catalog).await;
    let webhook_subscription_id_2 = helpers::new_webhook_subscription(catalog).await;

    let webhook_event_id_1 = helpers::new_webhook_event(catalog).await;
    let webhook_event_id_2 = helpers::new_webhook_event(catalog).await;

    let webhook_delivery_1_1 = new_delivery(
        task_attempt_id_1,
        webhook_subscription_id_1,
        webhook_event_id_1,
    );
    let webhook_delivery_1_2 = new_delivery(
        task_attempt_id_2,
        webhook_subscription_id_2,
        webhook_event_id_1,
    );
    let webhook_delivery_2_1 = new_delivery(
        task_attempt_id_3,
        webhook_subscription_id_1,
        webhook_event_id_2,
    );
    let webhook_delivery_2_2 = new_delivery(
        task_attempt_id_4,
        webhook_subscription_id_2,
        webhook_event_id_2,
    );

    repo.create(webhook_delivery_1_1.clone()).await.unwrap();
    repo.create(webhook_delivery_1_2.clone()).await.unwrap();
    repo.create(webhook_delivery_2_1.clone()).await.unwrap();
    repo.create(webhook_delivery_2_2.clone()).await.unwrap();

    let res = repo.list_by_event_id(webhook_event_id_1).await;
    assert_matches!(
        res,
        Ok(webhook_deliveries) if webhook_deliveries.len() == 2
            && webhook_deliveries[0] == webhook_delivery_1_1
            && webhook_deliveries[1] == webhook_delivery_1_2
    );

    let res = repo.list_by_event_id(webhook_event_id_2).await;
    assert_matches!(
        res,
        Ok(webhook_deliveries) if webhook_deliveries.len() == 2
            && webhook_deliveries[0] == webhook_delivery_2_1
            && webhook_deliveries[1] == webhook_delivery_2_2
    );

    let res = repo
        .list_by_subscription_id(webhook_subscription_id_1, PaginationOpts::from_page(0, 10))
        .await;
    assert_matches!(
        res,
        Ok(webhook_deliveries) if webhook_deliveries.len() == 2
            && webhook_deliveries[0] == webhook_delivery_2_1
            && webhook_deliveries[1] == webhook_delivery_1_1
    );

    let res = repo
        .list_by_subscription_id(webhook_subscription_id_2, PaginationOpts::from_page(0, 10))
        .await;
    assert_matches!(
        res,
        Ok(webhook_deliveries) if webhook_deliveries.len() == 2
            && webhook_deliveries[0] == webhook_delivery_2_2
            && webhook_deliveries[1] == webhook_delivery_1_2
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn new_delivery(
    task_attempt_id: ts::TaskAttemptID,
    webhook_subscription_id: WebhookSubscriptionId,
    webhook_event_id: WebhookEventId,
) -> WebhookDelivery {
    WebhookDelivery::new(
        task_attempt_id,
        webhook_subscription_id,
        webhook_event_id,
        WebhookRequest::new(
            vec![
                (
                    http::header::HeaderName::from_static("content-type"),
                    "application/json".to_string(),
                ),
                (
                    http::header::HeaderName::from_bytes(b"x-webhook-event-Id").unwrap(),
                    webhook_event_id.to_string(),
                ),
            ],
            Utc::now().round_subsecs(6),
        ),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
