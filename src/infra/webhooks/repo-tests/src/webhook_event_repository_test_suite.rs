// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use chrono::SubsecRound;
use database_common::PaginationOpts;
use dill::Catalog;
use kamu_webhooks::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_no_webhook_events_initially(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookEventRepository>().unwrap();

    let get_res = repo
        .get_event_by_id(WebhookEventID::new(uuid::Uuid::new_v4()))
        .await;
    assert_matches!(get_res, Err(GetWebhookEventError::NotFound(_)));

    let events = repo
        .list_recent_events(PaginationOpts {
            limit: 100,
            offset: 0,
        })
        .await
        .unwrap();
    assert!(events.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_single_webhook_event(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookEventRepository>().unwrap();

    let webhook_event = WebhookEvent {
        id: WebhookEventID::new(uuid::Uuid::new_v4()),
        event_type: WebhookEventTypeCatalog::test(),
        payload: serde_json::to_value("test_payload").unwrap(),
        created_at: chrono::Utc::now().round_subsecs(6),
    };

    let res = repo.create_event(&webhook_event).await;
    assert_matches!(res, Ok(()));

    let get_res = repo.get_event_by_id(webhook_event.id).await;
    assert_matches!(get_res, Ok(event) if event == webhook_event);

    let events = repo
        .list_recent_events(PaginationOpts {
            limit: 100,
            offset: 0,
        })
        .await
        .unwrap();
    assert_eq!(events, vec![webhook_event]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_multiple_webhook_events(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookEventRepository>().unwrap();

    let webhook_event_1 = WebhookEvent {
        id: WebhookEventID::new(uuid::Uuid::new_v4()),
        event_type: WebhookEventTypeCatalog::test(),
        payload: serde_json::to_value("test_payload_1").unwrap(),
        created_at: chrono::Utc::now().round_subsecs(6),
    };

    let webhook_event_2 = WebhookEvent {
        id: WebhookEventID::new(uuid::Uuid::new_v4()),
        event_type: WebhookEventTypeCatalog::test(),
        payload: serde_json::to_value("test_payload_2").unwrap(),
        created_at: chrono::Utc::now().round_subsecs(6),
    };

    let res = repo.create_event(&webhook_event_1).await;
    assert_matches!(res, Ok(()));

    let res = repo.create_event(&webhook_event_2).await;
    assert_matches!(res, Ok(()));

    let get_res = repo.get_event_by_id(webhook_event_1.id).await;
    assert_matches!(get_res, Ok(event) if event == webhook_event_1);

    let get_res = repo.get_event_by_id(webhook_event_2.id).await;
    assert_matches!(get_res, Ok(event) if event == webhook_event_2);

    let events = repo
        .list_recent_events(PaginationOpts {
            limit: 100,
            offset: 0,
        })
        .await
        .unwrap();
    assert_eq!(events, vec![webhook_event_2, webhook_event_1]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_duplicate_webhook_events(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookEventRepository>().unwrap();

    let webhook_event = WebhookEvent {
        id: WebhookEventID::new(uuid::Uuid::new_v4()),
        event_type: WebhookEventTypeCatalog::test(),
        payload: serde_json::to_value("test_payload").unwrap(),
        created_at: chrono::Utc::now().round_subsecs(6),
    };

    let res = repo.create_event(&webhook_event).await;
    assert_matches!(res, Ok(()));

    let res = repo.create_event(&webhook_event).await;
    assert_matches!(
        res,
        Err(CreateWebhookEventError::DuplicateId(
            WebhookEventDuplicateIdError { event_id }
        )) if event_id == webhook_event.id
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_webhook_events_pagination(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn WebhookEventRepository>().unwrap();

    let mut webhook_events = Vec::new();
    for i in 0..10 {
        let webhook_event = WebhookEvent {
            id: WebhookEventID::new(uuid::Uuid::new_v4()),
            event_type: WebhookEventTypeCatalog::test(),
            payload: serde_json::to_value(format!("test_payload_{i}")).unwrap(),
            created_at: chrono::Utc::now().round_subsecs(6),
        };
        webhook_events.push(webhook_event.clone());
        repo.create_event(&webhook_event).await.unwrap();
    }

    let events = repo
        .list_recent_events(PaginationOpts {
            limit: 3,
            offset: 0,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(
        events[0].payload,
        serde_json::to_value("test_payload_9").unwrap()
    );
    assert_eq!(
        events[1].payload,
        serde_json::to_value("test_payload_8").unwrap()
    );
    assert_eq!(
        events[2].payload,
        serde_json::to_value("test_payload_7").unwrap()
    );

    let events = repo
        .list_recent_events(PaginationOpts {
            limit: 3,
            offset: 3,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(
        events[0].payload,
        serde_json::to_value("test_payload_6").unwrap()
    );
    assert_eq!(
        events[1].payload,
        serde_json::to_value("test_payload_5").unwrap()
    );
    assert_eq!(
        events[2].payload,
        serde_json::to_value("test_payload_4").unwrap()
    );

    let events = repo
        .list_recent_events(PaginationOpts {
            limit: 2,
            offset: 9,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].payload,
        serde_json::to_value("test_payload_0").unwrap()
    );

    let events = repo
        .list_recent_events(PaginationOpts {
            limit: 2,
            offset: 10,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
