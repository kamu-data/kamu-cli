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
use dill::CatalogBuilder;
use kamu_resources::{
    DeclarativeResourceState,
    ReconcilableResource,
    ResourceAggregateLoader,
    ResourcePersistenceError,
    ResourcePersistenceService,
    ResourceRawEventQuery,
    ResourceSnapshot,
    ResourceType,
    ResourceUID,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

use crate::tests::utils::{
    TestResource,
    TestResourceReconciler,
    TestResourceSpec,
    make_account_id,
    make_fresh_aggregate,
    register_test_resource_resource_service_layer,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_resource_persists_snapshot_and_events() {
    let harness = ResourcePersistenceServiceHarness::new();
    let account_id = make_account_id();
    let (uid, mut agg) = make_fresh_aggregate(account_id, "res-a");

    harness.persistence_svc().create(&mut agg).await.unwrap();

    let loaded = harness.aggregate_loader().load(&uid).await.unwrap();
    assert_eq!(*loaded.uid(), uid);
    assert_eq!(loaded.spec().value, "res-a");
    assert_eq!(loaded.metadata().name.as_str(), "res-a");

    let snapshot = harness.find_snapshot(&uid).await;
    assert_eq!(snapshot.uid, uid);
    assert_eq!(snapshot.metadata.name.as_str(), "res-a");
    pretty_assertions::assert_eq!(snapshot.spec, serde_json::json!({ "value": "res-a" }));
    assert!(
        snapshot.last_event_id.is_some(),
        "snapshot must record last event id"
    );
    assert_eq!(snapshot.last_event_id, loaded.last_stored_event_id());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_duplicate_uid_returns_error() {
    let harness = ResourcePersistenceServiceHarness::new();
    let account_id = make_account_id();

    let (_, mut agg) = make_fresh_aggregate(account_id.clone(), "res-a");
    harness.persistence_svc().create(&mut agg).await.unwrap();

    // Same account + kind + name but a fresh UID — repository duplicate check fires
    let (_, mut agg2) = make_fresh_aggregate(account_id, "res-a");
    let result = harness.persistence_svc().create(&mut agg2).await;

    assert!(
        matches!(result, Err(ResourcePersistenceError::Duplicate(_))),
        "expected Duplicate error, got {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_save_resource_updates_snapshot() {
    let harness = ResourcePersistenceServiceHarness::new();
    let account_id = make_account_id();
    let (uid, mut agg) = make_fresh_aggregate(account_id, "res-a");
    harness.persistence_svc().create(&mut agg).await.unwrap();

    let mut loaded = harness.aggregate_loader().load(&uid).await.unwrap();
    loaded
        .try_update_spec(
            Utc::now(),
            TestResourceSpec {
                value: "updated".to_string(),
            },
        )
        .unwrap();
    harness.persistence_svc().save(&mut loaded).await.unwrap();

    let reloaded = harness.aggregate_loader().load(&uid).await.unwrap();
    assert_eq!(reloaded.spec().value, "updated");

    let snapshot = harness.find_snapshot(&uid).await;
    pretty_assertions::assert_eq!(snapshot.spec, serde_json::json!({ "value": "updated" }));
    assert_eq!(snapshot.last_event_id, reloaded.last_stored_event_id());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_resource_marks_deleted() {
    let harness = ResourcePersistenceServiceHarness::new();
    let account_id = make_account_id();
    let (uid, mut agg) = make_fresh_aggregate(account_id, "res-a");
    harness.persistence_svc().create(&mut agg).await.unwrap();

    let mut loaded = harness.aggregate_loader().load(&uid).await.unwrap();
    let now = Utc::now();
    harness
        .persistence_svc()
        .delete(&mut loaded, now)
        .await
        .unwrap();

    let reloaded = harness.aggregate_loader().load(&uid).await.unwrap();
    assert!(
        reloaded.metadata().deleted_at.is_some(),
        "expected deleted_at to be set after deletion"
    );

    // Deleted snapshots are hidden from live lookups — finding None is correct
    assert!(
        harness.snapshot_not_found(&uid).await,
        "deleted resource must not appear in live snapshot lookup"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_many_resources_batch() {
    let harness = ResourcePersistenceServiceHarness::new();
    let account_id = make_account_id();

    let (uid_a, mut agg_a) = make_fresh_aggregate(account_id.clone(), "res-a");
    let (uid_b, mut agg_b) = make_fresh_aggregate(account_id.clone(), "res-b");
    let (uid_c, mut agg_c) = make_fresh_aggregate(account_id, "res-c");

    harness.persistence_svc().create(&mut agg_a).await.unwrap();
    harness.persistence_svc().create(&mut agg_b).await.unwrap();
    harness.persistence_svc().create(&mut agg_c).await.unwrap();

    let loaded_a = harness.aggregate_loader().load(&uid_a).await.unwrap();
    let loaded_b = harness.aggregate_loader().load(&uid_b).await.unwrap();
    let loaded_c = harness.aggregate_loader().load(&uid_c).await.unwrap();

    let now = Utc::now();
    harness
        .persistence_svc()
        .delete_many(&mut [loaded_a, loaded_b, loaded_c], now)
        .await
        .unwrap();

    for uid in [&uid_a, &uid_b, &uid_c] {
        let reloaded = harness.aggregate_loader().load(uid).await.unwrap();
        assert!(
            reloaded.metadata().deleted_at.is_some(),
            "expected deleted_at set in aggregate for {uid:?}"
        );

        // Deleted snapshots are hidden from live lookups — finding None is correct
        assert!(
            harness.snapshot_not_found(uid).await,
            "deleted resource must not appear in live snapshot lookup for {uid:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_concurrent_modification_detected() {
    let harness = ResourcePersistenceServiceHarness::new();
    let account_id = make_account_id();
    let (uid, mut agg) = make_fresh_aggregate(account_id, "res-a");
    harness.persistence_svc().create(&mut agg).await.unwrap();

    // Load the same aggregate into two independent instances
    let mut agg_1 = harness.aggregate_loader().load(&uid).await.unwrap();
    let mut agg_2 = harness.aggregate_loader().load(&uid).await.unwrap();

    let now = Utc::now();

    // Save agg_1 first — this advances the event cursor in the store
    agg_1
        .try_update_spec(
            now,
            TestResourceSpec {
                value: "from-agg-1".to_string(),
            },
        )
        .unwrap();
    harness.persistence_svc().save(&mut agg_1).await.unwrap();

    // agg_2 still carries the old last_stored_event_id — save must fail
    agg_2
        .try_update_spec(
            now,
            TestResourceSpec {
                value: "from-agg-2".to_string(),
            },
        )
        .unwrap();
    let result = harness.persistence_svc().save(&mut agg_2).await;

    assert!(
        matches!(
            result,
            Err(ResourcePersistenceError::ConcurrentModification(_))
        ),
        "expected ConcurrentModification error, got {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseResourceServiceHarness, base)]
struct ResourcePersistenceServiceHarness {
    base: BaseResourceServiceHarness,
    catalog: dill::Catalog,
}

impl ResourcePersistenceServiceHarness {
    fn new() -> Self {
        let base = BaseResourceServiceHarness::new();

        let mut b = CatalogBuilder::new_chained(base.catalog());
        register_test_resource_resource_service_layer(&mut b);
        b.add::<TestResourceReconciler>();

        Self {
            base,
            catalog: b.build(),
        }
    }

    fn persistence_svc(&self) -> Arc<dyn ResourcePersistenceService<TestResource>> {
        self.catalog.get_one().unwrap()
    }

    fn aggregate_loader(&self) -> Arc<dyn ResourceAggregateLoader<TestResource>> {
        self.catalog.get_one().unwrap()
    }

    async fn find_snapshot(&self, uid: &ResourceUID) -> ResourceSnapshot {
        self.resource_repo()
            .find_resource_snapshot(&ResourceRawEventQuery {
                kind: TestResource::RESOURCE_TYPE.to_string(),
                uid: *uid,
            })
            .await
            .unwrap()
            .expect("snapshot must exist")
    }

    async fn snapshot_not_found(&self, uid: &ResourceUID) -> bool {
        self.resource_repo()
            .find_resource_snapshot(&ResourceRawEventQuery {
                kind: TestResource::RESOURCE_TYPE.to_string(),
                uid: *uid,
            })
            .await
            .unwrap()
            .is_none()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
