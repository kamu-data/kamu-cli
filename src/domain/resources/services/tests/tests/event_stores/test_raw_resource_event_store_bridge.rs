// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use database_common::NoOpDatabasePlugin;
use dill::CatalogBuilder;
use event_sourcing::{GetEventsOpts, SaveEventsItem};
use kamu_resources::{ResourceRawEvent, ResourceRawEventQuery, ResourceRawEventStore, ResourceUID};
use kamu_resources_inmem::InMemoryRawResourceEventStore;
use tokio_stream::StreamExt;

use crate::tests::utils::{
    TestEvent,
    TestResourceEventStore,
    make_created_event,
    make_spec_updated_event,
    make_uid,
    register_test_resource_resource_service_layer,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_save_and_get_events_round_trip() {
    let harness = RawResourceEventStoreBridgeHarness::new();
    let uid = make_uid();

    let created = make_created_event(uid, "res-a", "hello");
    let updated = make_spec_updated_event(uid, "world");

    harness.save_events(uid, vec![created.clone()]).await;

    let prev_id = harness.get_last_event_id(uid).await;
    harness
        .save_events_after(uid, prev_id, vec![updated.clone()])
        .await;

    let events = harness.get_events_for(uid).await;

    assert_eq!(events.len(), 2);
    assert_eq!(events[0], created);
    assert_eq!(events[1], updated);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_kind_filtering_excludes_unrelated_raw_events() {
    let harness = RawResourceEventStoreBridgeHarness::new();
    let raw_store = harness.raw_store();
    let uid = make_uid();

    let created = make_created_event(uid, "res-b", "value");
    harness.save_events(uid, vec![created]).await;

    let wrong_query = ResourceRawEventQuery {
        kind: "OtherResource".to_string(),
        uid,
    };
    let wrong_raw = ResourceRawEvent {
        event_id: event_sourcing::EventID::new(0),
        query: wrong_query.clone(),
        event_time: Utc::now(),
        event_type: "Created".to_string(),
        payload: serde_json::json!({}),
    };
    raw_store
        .save_events(&wrong_query, None, vec![wrong_raw])
        .await
        .unwrap();

    let events = harness.get_events_for(uid).await;
    assert_eq!(events.len(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_all_events_streams_all_uids() {
    let harness = RawResourceEventStoreBridgeHarness::new();

    let uid_a = make_uid();
    let uid_b = make_uid();
    let uid_c = make_uid();

    harness
        .save_events(uid_a, vec![make_created_event(uid_a, "res-a", "a")])
        .await;
    harness
        .save_events(uid_b, vec![make_created_event(uid_b, "res-b", "b")])
        .await;
    harness
        .save_events(uid_c, vec![make_created_event(uid_c, "res-c", "c")])
        .await;

    let all_events: Vec<TestEvent> = harness
        .bridge()
        .get_all_events(GetEventsOpts::default())
        .map(|r| r.unwrap().1)
        .collect()
        .await;

    assert_eq!(all_events.len(), 3);

    let uids: std::collections::HashSet<ResourceUID> =
        all_events.iter().map(|e| *e.uid()).collect();
    assert!(uids.contains(&uid_a));
    assert!(uids.contains(&uid_b));
    assert!(uids.contains(&uid_c));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_save_events_multi_batch() {
    let harness = RawResourceEventStoreBridgeHarness::new();

    let uid_a = make_uid();
    let uid_b = make_uid();

    harness
        .bridge()
        .save_events_multi(vec![
            SaveEventsItem {
                query: uid_a,
                maybe_prev_stored_event_id: None,
                events: vec![make_created_event(uid_a, "res-a", "va")],
            },
            SaveEventsItem {
                query: uid_b,
                maybe_prev_stored_event_id: None,
                events: vec![make_created_event(uid_b, "res-b", "vb")],
            },
        ])
        .await
        .unwrap();

    let events_a = harness.get_events_for(uid_a).await;
    let events_b = harness.get_events_for(uid_b).await;

    assert_eq!(events_a.len(), 1);
    assert_eq!(*events_a[0].uid(), uid_a);

    assert_eq!(events_b.len(), 1);
    assert_eq!(*events_b[0].uid(), uid_b);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_total_events_stored_counts_correctly() {
    let harness = RawResourceEventStoreBridgeHarness::new();

    let uid = make_uid();
    let created = make_created_event(uid, "res-count", "v1");
    let updated = make_spec_updated_event(uid, "v2");

    harness.save_events(uid, vec![created]).await;

    let prev_id = harness.get_last_event_id(uid).await;
    harness.save_events_after(uid, prev_id, vec![updated]).await;

    let total = harness.bridge().total_events_stored().await.unwrap();
    assert_eq!(total, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_events_multi_streams_per_uid() {
    let harness = RawResourceEventStoreBridgeHarness::new();

    let uid_a = make_uid();
    let uid_b = make_uid();

    harness
        .save_events(uid_a, vec![make_created_event(uid_a, "res-a", "a1")])
        .await;
    let prev_a = harness.get_last_event_id(uid_a).await;
    harness
        .save_events_after(uid_a, prev_a, vec![make_spec_updated_event(uid_a, "a2")])
        .await;

    harness
        .save_events(uid_b, vec![make_created_event(uid_b, "res-b", "b1")])
        .await;
    let prev_b = harness.get_last_event_id(uid_b).await;
    harness
        .save_events_after(uid_b, prev_b, vec![make_spec_updated_event(uid_b, "b2")])
        .await;
    let prev_b2 = harness.get_last_event_id(uid_b).await;
    harness
        .save_events_after(uid_b, prev_b2, vec![make_spec_updated_event(uid_b, "b3")])
        .await;

    let mut by_uid: std::collections::HashMap<ResourceUID, Vec<TestEvent>> =
        std::collections::HashMap::new();
    let bridge = harness.bridge();
    let mut multi = bridge.get_events_multi(&[uid_a, uid_b]);
    while let Some(result) = multi.next().await {
        let (uid, _event_id, event) = result.unwrap();
        by_uid.entry(uid).or_default().push(event);
    }

    assert_eq!(by_uid[&uid_a].len(), 2);
    assert_eq!(by_uid[&uid_b].len(), 3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct RawResourceEventStoreBridgeHarness {
    catalog: dill::Catalog,
}

impl RawResourceEventStoreBridgeHarness {
    fn new() -> Self {
        let mut b = CatalogBuilder::new();
        b.add::<InMemoryRawResourceEventStore>();
        NoOpDatabasePlugin::init_database_components(&mut b);
        register_test_resource_resource_service_layer(&mut b);
        Self { catalog: b.build() }
    }

    fn bridge(&self) -> Arc<dyn TestResourceEventStore> {
        self.catalog.get_one().unwrap()
    }

    fn raw_store(&self) -> Arc<dyn ResourceRawEventStore> {
        self.catalog.get_one().unwrap()
    }

    async fn get_events_for(&self, uid: ResourceUID) -> Vec<TestEvent> {
        self.bridge()
            .get_events(&uid, GetEventsOpts::default())
            .map(|r| r.unwrap().1)
            .collect()
            .await
    }

    async fn get_last_event_id(&self, uid: ResourceUID) -> event_sourcing::EventID {
        self.bridge()
            .get_events(&uid, GetEventsOpts::default())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .last()
            .unwrap()
            .unwrap()
            .0
    }

    async fn save_events(
        &self,
        uid: ResourceUID,
        events: Vec<TestEvent>,
    ) -> event_sourcing::EventID {
        self.bridge().save_events(&uid, None, events).await.unwrap()
    }

    async fn save_events_after(
        &self,
        uid: ResourceUID,
        prev_id: event_sourcing::EventID,
        events: Vec<TestEvent>,
    ) -> event_sourcing::EventID {
        self.bridge()
            .save_events(&uid, Some(prev_id), events)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
