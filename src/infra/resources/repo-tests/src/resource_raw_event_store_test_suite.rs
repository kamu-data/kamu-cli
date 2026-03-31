// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use chrono::Utc;
use dill::Catalog;
use event_sourcing::{EventID, GetEventsOpts, SaveEventsError};
use futures::TryStreamExt;
use kamu_resources::{
    ResourceMetadata,
    ResourceRawEvent,
    ResourceRawEventQuery,
    ResourceRawEventStore,
    ResourceRepository,
    ResourceSnapshot,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn make_resource(catalog: &Catalog, kind: &str) -> ResourceRawEventQuery {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let id = uuid::Uuid::new_v4();
    let now = Utc::now();

    let snapshot = ResourceSnapshot {
        uid: id,
        kind: kind.to_string(),
        api_version: "v1".to_string(),
        metadata: ResourceMetadata {
            account: odf::AccountID::new_seeded_ed25519(b"test-account"),
            name: id.to_string(),
            description: None,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            generation: 0,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        },
        spec: serde_json::json!({}),
        status: None,
        last_reconciled_at: None,
        last_event_id: None,
    };

    repo.create_resource(&snapshot).await.unwrap();

    ResourceRawEventQuery {
        kind: kind.to_string(),
        uid: id,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_raw_event(
    query: &ResourceRawEventQuery,
    event_type: &str,
    payload: serde_json::Value,
) -> ResourceRawEvent {
    ResourceRawEvent {
        event_id: EventID::new(0), // placeholder – assigned by the store on save
        query: query.clone(),
        event_time: Utc::now(),
        event_type: event_type.to_string(),
        payload,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_empty(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn ResourceRawEventStore>().unwrap();

    let total = event_store.total_events_stored().await.unwrap();
    assert_eq!(0, total);

    let query = make_resource(catalog, "TestKind").await;
    let events: Vec<_> = event_store
        .get_events(&query, GetEventsOpts::default())
        .try_collect()
        .await
        .unwrap();
    assert!(events.is_empty());

    let total_by_kind = event_store
        .total_events_stored_by_kind("TestKind")
        .await
        .unwrap();
    assert_eq!(0, total_by_kind);

    let events_by_kind: Vec<_> = event_store
        .get_all_events_by_kind("TestKind", GetEventsOpts::default())
        .try_collect()
        .await
        .unwrap();
    assert!(events_by_kind.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_save_and_get_events(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn ResourceRawEventStore>().unwrap();

    let query = make_resource(catalog, "TestKind").await;
    let event_1 = make_raw_event(&query, "Created", serde_json::json!({"spec": "a"}));
    let event_2 = make_raw_event(&query, "SpecUpdated", serde_json::json!({"spec": "b"}));
    let event_3 = make_raw_event(&query, "Deleted", serde_json::json!({}));

    let last_event_id = event_store
        .save_events(
            &query,
            None,
            vec![event_1.clone(), event_2.clone(), event_3.clone()],
        )
        .await
        .unwrap();

    let total = event_store.total_events_stored().await.unwrap();
    assert_eq!(3, total);

    let events: Vec<_> = event_store
        .get_events(&query, GetEventsOpts::default())
        .map_ok(|(_, e)| e)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(3, events.len());
    assert_eq!(events[0].event_type, "Created");
    assert_eq!(events[0].payload, serde_json::json!({"spec": "a"}));
    assert_eq!(events[1].event_type, "SpecUpdated");
    assert_eq!(events[2].event_type, "Deleted");
    assert_eq!(events[2].event_id, last_event_id);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_events_isolated_by_query(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn ResourceRawEventStore>().unwrap();

    let query_a = make_resource(catalog, "TestKind").await;
    let query_b = make_resource(catalog, "TestKind").await;

    event_store
        .save_events(
            &query_a,
            None,
            vec![
                make_raw_event(&query_a, "Created", serde_json::json!({"owner": "a"})),
                make_raw_event(&query_a, "SpecUpdated", serde_json::json!({"owner": "a"})),
            ],
        )
        .await
        .unwrap();

    event_store
        .save_events(
            &query_b,
            None,
            vec![make_raw_event(
                &query_b,
                "Created",
                serde_json::json!({"owner": "b"}),
            )],
        )
        .await
        .unwrap();

    let events_a: Vec<_> = event_store
        .get_events(&query_a, GetEventsOpts::default())
        .map_ok(|(_, e)| e)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(2, events_a.len());
    assert!(
        events_a
            .iter()
            .all(|e| e.payload["owner"] == serde_json::json!("a"))
    );

    let events_b: Vec<_> = event_store
        .get_events(&query_b, GetEventsOpts::default())
        .map_ok(|(_, e)| e)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(1, events_b.len());
    assert_eq!(events_b[0].payload["owner"], serde_json::json!("b"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_all_events(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn ResourceRawEventStore>().unwrap();

    let query_a = make_resource(catalog, "TestKind").await;
    let query_b = make_resource(catalog, "TestKind").await;

    event_store
        .save_events(
            &query_a,
            None,
            vec![
                make_raw_event(&query_a, "Created", serde_json::json!({})),
                make_raw_event(&query_a, "SpecUpdated", serde_json::json!({})),
            ],
        )
        .await
        .unwrap();

    event_store
        .save_events(
            &query_b,
            None,
            vec![make_raw_event(&query_b, "Created", serde_json::json!({}))],
        )
        .await
        .unwrap();

    let total = event_store.total_events_stored().await.unwrap();
    assert_eq!(3, total);

    let all_events: Vec<_> = event_store
        .get_all_events(GetEventsOpts::default())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(3, all_events.len());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_events_filtered_by_kind(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn ResourceRawEventStore>().unwrap();

    let query_kind_a = make_resource(catalog, "KindA").await;
    let query_kind_b = make_resource(catalog, "KindB").await;

    event_store
        .save_events(
            &query_kind_a,
            None,
            vec![
                make_raw_event(&query_kind_a, "Created", serde_json::json!({"kind": "a"})),
                make_raw_event(
                    &query_kind_a,
                    "SpecUpdated",
                    serde_json::json!({"kind": "a"}),
                ),
            ],
        )
        .await
        .unwrap();

    event_store
        .save_events(
            &query_kind_b,
            None,
            vec![make_raw_event(
                &query_kind_b,
                "Created",
                serde_json::json!({"kind": "b"}),
            )],
        )
        .await
        .unwrap();

    let total_kind_a = event_store
        .total_events_stored_by_kind("KindA")
        .await
        .unwrap();
    assert_eq!(2, total_kind_a);

    let total_kind_b = event_store
        .total_events_stored_by_kind("KindB")
        .await
        .unwrap();
    assert_eq!(1, total_kind_b);

    let total_kind_c = event_store
        .total_events_stored_by_kind("KindC")
        .await
        .unwrap();
    assert_eq!(0, total_kind_c);

    let events_kind_a: Vec<_> = event_store
        .get_all_events_by_kind("KindA", GetEventsOpts::default())
        .map_ok(|(_, e)| e)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(2, events_kind_a.len());
    assert!(
        events_kind_a
            .iter()
            .all(|e| e.query.kind == "KindA" && e.payload["kind"] == serde_json::json!("a"))
    );

    let events_kind_b: Vec<_> = event_store
        .get_all_events_by_kind("KindB", GetEventsOpts::default())
        .map_ok(|(_, e)| e)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(1, events_kind_b.len());
    assert_eq!(events_kind_b[0].query.kind, "KindB");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_events_with_windowing(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn ResourceRawEventStore>().unwrap();

    let query = make_resource(catalog, "TestKind").await;
    event_store
        .save_events(
            &query,
            None,
            vec![
                make_raw_event(&query, "Created", serde_json::json!({"n": 1})),
                make_raw_event(&query, "SpecUpdated", serde_json::json!({"n": 2})),
                make_raw_event(&query, "Deleted", serde_json::json!({"n": 3})),
            ],
        )
        .await
        .unwrap();

    // Retrieve all three events to capture their assigned event IDs
    let all: Vec<(EventID, ResourceRawEvent)> = event_store
        .get_events(&query, GetEventsOpts::default())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(3, all.len());
    let (id_1, _) = all[0];
    let (id_2, _) = all[1];
    let (id_3, _) = all[2];

    // from = id_1 (exclusive lower bound): should return events 2 and 3
    let from_1: Vec<_> = event_store
        .get_events(
            &query,
            GetEventsOpts {
                from: Some(id_1),
                to: None,
            },
        )
        .map_ok(|(_, e)| e)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(2, from_1.len());
    assert_eq!(from_1[0].payload["n"], serde_json::json!(2));
    assert_eq!(from_1[1].payload["n"], serde_json::json!(3));

    // to = id_2 (inclusive upper bound): should return events 1 and 2
    let to_2: Vec<_> = event_store
        .get_events(
            &query,
            GetEventsOpts {
                from: None,
                to: Some(id_2),
            },
        )
        .map_ok(|(_, e)| e)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(2, to_2.len());
    assert_eq!(to_2[0].payload["n"], serde_json::json!(1));
    assert_eq!(to_2[1].payload["n"], serde_json::json!(2));

    // from = id_1, to = id_2: should return only event 2
    let window: Vec<_> = event_store
        .get_events(
            &query,
            GetEventsOpts {
                from: Some(id_1),
                to: Some(id_2),
            },
        )
        .map_ok(|(_, e)| e)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(1, window.len());
    assert_eq!(window[0].payload["n"], serde_json::json!(2));

    // from = id_3 (last event): should return nothing
    let after_end: Vec<_> = event_store
        .get_events(
            &query,
            GetEventsOpts {
                from: Some(id_3),
                to: None,
            },
        )
        .map_ok(|(_, e)| e)
        .try_collect()
        .await
        .unwrap();
    assert!(after_end.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_concurrent_modification_rejected(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn ResourceRawEventStore>().unwrap();

    let query = make_resource(catalog, "TestKind").await;

    // First save succeeds: returns an event ID
    let last_event_id = event_store
        .save_events(
            &query,
            None,
            vec![make_raw_event(&query, "Created", serde_json::json!({}))],
        )
        .await
        .unwrap();

    // Retry with the same None as prev_event_id should fail (expected = None,
    // actual = Some)
    let result = event_store
        .save_events(
            &query,
            None,
            vec![make_raw_event(&query, "SpecUpdated", serde_json::json!({}))],
        )
        .await;
    assert!(
        matches!(result, Err(SaveEventsError::ConcurrentModification(_))),
        "expected ConcurrentModification, got {result:?}"
    );

    // Using correct expected event ID succeeds
    event_store
        .save_events(
            &query,
            Some(last_event_id),
            vec![make_raw_event(&query, "SpecUpdated", serde_json::json!({}))],
        )
        .await
        .unwrap();

    // Retry with the now-stale event ID fails
    let result = event_store
        .save_events(
            &query,
            Some(last_event_id),
            vec![make_raw_event(&query, "Deleted", serde_json::json!({}))],
        )
        .await;
    assert!(
        matches!(result, Err(SaveEventsError::ConcurrentModification(_))),
        "expected ConcurrentModification on stale ID, got {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
