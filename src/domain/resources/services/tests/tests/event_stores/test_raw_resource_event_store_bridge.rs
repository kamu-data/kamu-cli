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
use kamu_resources::{ResourceID, ResourceRawEvent, ResourceRawEventQuery, ResourceRawEventStore};
use kamu_resources_inmem::InMemoryRawResourceEventStore;
use tokio_stream::StreamExt;

use crate::tests::utils::{
    TestEvent,
    TestResourceEventStore,
    make_created_event,
    make_id,
    make_spec_updated_event,
    register_test_resource_resource_service_layer,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_save_and_get_events_round_trip() {
    let harness = RawResourceEventStoreBridgeHarness::new();
    let id = make_id();

    let created = make_created_event(id, "res-a", "hello");
    let updated = make_spec_updated_event(id, "world", 2);

    harness.save_events(id, vec![created.clone()]).await;

    let prev_id = harness.get_last_event_id(id).await;
    harness
        .save_events_after(id, prev_id, vec![updated.clone()])
        .await;

    let events = harness.get_events_for(id).await;

    assert_eq!(events.len(), 2);
    assert_eq!(events[0], created);
    assert_eq!(events[1], updated);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_kind_filtering_excludes_unrelated_raw_events() {
    let harness = RawResourceEventStoreBridgeHarness::new();
    let raw_store = harness.raw_store();
    let id = make_id();

    let created = make_created_event(id, "res-b", "value");
    harness.save_events(id, vec![created]).await;

    let wrong_query = ResourceRawEventQuery {
        kind: "OtherResource".to_string(),
        id,
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

    let events = harness.get_events_for(id).await;
    assert_eq!(events.len(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_all_events_streams_all_ids() {
    let harness = RawResourceEventStoreBridgeHarness::new();

    let id_a = make_id();
    let id_b = make_id();
    let id_c = make_id();

    harness
        .save_events(id_a, vec![make_created_event(id_a, "res-a", "a")])
        .await;
    harness
        .save_events(id_b, vec![make_created_event(id_b, "res-b", "b")])
        .await;
    harness
        .save_events(id_c, vec![make_created_event(id_c, "res-c", "c")])
        .await;

    let all_events: Vec<TestEvent> = harness
        .bridge()
        .get_all_events(GetEventsOpts::default())
        .map(|r| r.unwrap().1)
        .collect()
        .await;

    assert_eq!(all_events.len(), 3);

    let ids: std::collections::HashSet<ResourceID> = all_events.iter().map(|e| *e.id()).collect();
    assert!(ids.contains(&id_a));
    assert!(ids.contains(&id_b));
    assert!(ids.contains(&id_c));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_save_events_multi_batch() {
    let harness = RawResourceEventStoreBridgeHarness::new();

    let id_a = make_id();
    let id_b = make_id();

    harness
        .bridge()
        .save_events_multi(vec![
            SaveEventsItem {
                query: id_a,
                maybe_prev_stored_event_id: None,
                events: vec![make_created_event(id_a, "res-a", "va")],
            },
            SaveEventsItem {
                query: id_b,
                maybe_prev_stored_event_id: None,
                events: vec![make_created_event(id_b, "res-b", "vb")],
            },
        ])
        .await
        .unwrap();

    let events_a = harness.get_events_for(id_a).await;
    let events_b = harness.get_events_for(id_b).await;

    assert_eq!(events_a.len(), 1);
    assert_eq!(*events_a[0].id(), id_a);

    assert_eq!(events_b.len(), 1);
    assert_eq!(*events_b[0].id(), id_b);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_total_events_stored_counts_correctly() {
    let harness = RawResourceEventStoreBridgeHarness::new();

    let id = make_id();
    let created = make_created_event(id, "res-count", "v1");
    let updated = make_spec_updated_event(id, "v2", 2);

    harness.save_events(id, vec![created]).await;

    let prev_id = harness.get_last_event_id(id).await;
    harness.save_events_after(id, prev_id, vec![updated]).await;

    let total = harness.bridge().total_events_stored().await.unwrap();
    assert_eq!(total, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_events_multi_streams_per_id() {
    let harness = RawResourceEventStoreBridgeHarness::new();

    let id_a = make_id();
    let id_b = make_id();

    harness
        .save_events(id_a, vec![make_created_event(id_a, "res-a", "a1")])
        .await;
    let prev_a = harness.get_last_event_id(id_a).await;
    harness
        .save_events_after(id_a, prev_a, vec![make_spec_updated_event(id_a, "a2", 2)])
        .await;

    harness
        .save_events(id_b, vec![make_created_event(id_b, "res-b", "b1")])
        .await;
    let prev_b = harness.get_last_event_id(id_b).await;
    harness
        .save_events_after(id_b, prev_b, vec![make_spec_updated_event(id_b, "b2", 2)])
        .await;
    let prev_b2 = harness.get_last_event_id(id_b).await;
    harness
        .save_events_after(id_b, prev_b2, vec![make_spec_updated_event(id_b, "b3", 3)])
        .await;

    let mut by_id: std::collections::HashMap<ResourceID, Vec<TestEvent>> =
        std::collections::HashMap::new();
    let bridge = harness.bridge();
    let mut multi = bridge.get_events_multi(&[id_a, id_b]);
    while let Some(result) = multi.next().await {
        let (id, _event_id, event) = result.unwrap();
        by_id.entry(id).or_default().push(event);
    }

    assert_eq!(by_id[&id_a].len(), 2);
    assert_eq!(by_id[&id_b].len(), 3);
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

    async fn get_events_for(&self, id: ResourceID) -> Vec<TestEvent> {
        self.bridge()
            .get_events(&id, GetEventsOpts::default())
            .map(|r| r.unwrap().1)
            .collect()
            .await
    }

    async fn get_last_event_id(&self, id: ResourceID) -> event_sourcing::EventID {
        self.bridge()
            .get_events(&id, GetEventsOpts::default())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .last()
            .unwrap()
            .unwrap()
            .0
    }

    async fn save_events(&self, id: ResourceID, events: Vec<TestEvent>) -> event_sourcing::EventID {
        self.bridge().save_events(&id, None, events).await.unwrap()
    }

    async fn save_events_after(
        &self,
        id: ResourceID,
        prev_id: event_sourcing::EventID,
        events: Vec<TestEvent>,
    ) -> event_sourcing::EventID {
        self.bridge()
            .save_events(&id, Some(prev_id), events)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
