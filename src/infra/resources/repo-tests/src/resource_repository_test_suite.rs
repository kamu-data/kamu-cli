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
use database_common::PaginationOpts;
use dill::Catalog;
use event_sourcing::EventID;
use futures::TryStreamExt;
use kamu_resources::{
    CreateResourceError,
    ResourceMetadata,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    UpdateResourceError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_test_snapshot(account_id: odf::AccountID, kind: &str, name: &str) -> ResourceSnapshot {
    let now = Utc::now();
    ResourceSnapshot {
        resource_id: uuid::Uuid::new_v4(),
        kind: kind.to_string(),
        api_version: "v1".to_string(),
        metadata: ResourceMetadata {
            account: account_id,
            name: name.to_string(),
            description: None,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            generation: 0,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        },
        spec: serde_json::json!({"key": "value"}),
        status: None,
        last_reconciled_at: None,
        last_event_id: None,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_no_resources_initially(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let count = repo
        .count_resources(account_id.clone(), "TestKind")
        .await
        .unwrap();
    assert_eq!(0, count);

    let ids: Vec<_> = repo
        .list_resource_ids(
            account_id.clone(),
            "TestKind",
            PaginationOpts {
                limit: 100,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert!(ids.is_empty());

    let snapshots: Vec<_> = repo
        .list_all_resource_snapshots(
            account_id,
            PaginationOpts {
                limit: 100,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert!(snapshots.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_and_find_resource(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let mut snapshot = make_test_snapshot(account_id.clone(), "TestKind", "my-resource");
    snapshot.resource_id = repo.new_resource_id().await.unwrap();
    let resource_id = snapshot.resource_id;

    repo.create_resource(&snapshot).await.unwrap();

    // find by id
    let found = repo
        .find_resource_snapshot_by_id(&resource_id)
        .await
        .unwrap();
    assert!(found.is_some());
    let found = found.unwrap();
    assert_eq!(found.resource_id, resource_id);
    assert_eq!(found.kind, "TestKind");
    assert_eq!(found.metadata.name, "my-resource");
    assert_eq!(found.last_event_id, None);

    // find by name
    let found_id = repo
        .find_resource_id_by_name(account_id.clone(), "TestKind", &"my-resource".to_string())
        .await
        .unwrap();
    assert_eq!(found_id, Some(resource_id));

    // find via raw event query
    let found = repo
        .find_resource_snapshot(&ResourceRawEventQuery {
            kind: "TestKind".to_string(),
            id: resource_id,
        })
        .await
        .unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().resource_id, resource_id);

    // wrong kind returns nothing
    let not_found = repo
        .find_resource_snapshot(&ResourceRawEventQuery {
            kind: "OtherKind".to_string(),
            id: resource_id,
        })
        .await
        .unwrap();
    assert!(not_found.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_resource_duplicate_fails(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let mut first = make_test_snapshot(account_id.clone(), "TestKind", "duplicate-resource");
    first.resource_id = repo.new_resource_id().await.unwrap();
    repo.create_resource(&first).await.unwrap();

    let mut second = make_test_snapshot(account_id, "TestKind", "duplicate-resource");
    second.resource_id = repo.new_resource_id().await.unwrap();
    let result = repo.create_resource(&second).await;

    assert!(matches!(result, Err(CreateResourceError::Duplicate(_))));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_resource(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let mut snapshot = make_test_snapshot(account_id.clone(), "TestKind", "update-me");
    snapshot.resource_id = repo.new_resource_id().await.unwrap();
    let resource_id = snapshot.resource_id;
    repo.create_resource(&snapshot).await.unwrap();

    let event_id = EventID::new(1);
    let updated = ResourceSnapshot {
        metadata: ResourceMetadata {
            description: Some("Updated description".to_string()),
            generation: 1,
            updated_at: Utc::now(),
            ..snapshot.metadata.clone()
        },
        last_event_id: Some(event_id),
        ..snapshot.clone()
    };

    repo.update_resource(&updated, None).await.unwrap();

    let found = repo
        .find_resource_snapshot_by_id(&resource_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        found.metadata.description,
        Some("Updated description".to_string())
    );
    assert_eq!(found.metadata.generation, 1);
    assert_eq!(found.last_event_id, Some(event_id));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_resource_wrong_event_id_fails(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let mut snapshot = make_test_snapshot(account_id, "TestKind", "concurrent-resource");
    snapshot.resource_id = repo.new_resource_id().await.unwrap();
    repo.create_resource(&snapshot).await.unwrap();

    // Resource has last_event_id = None, but we pass Some(...)
    let result = repo
        .update_resource(&snapshot, Some(EventID::new(99)))
        .await;
    assert!(matches!(
        result,
        Err(UpdateResourceError::ConcurrentModification(_))
    ));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_resource_optimistic_locking(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let mut snapshot = make_test_snapshot(account_id, "TestKind", "locked-resource");
    snapshot.resource_id = repo.new_resource_id().await.unwrap();
    repo.create_resource(&snapshot).await.unwrap();

    // First update: sets last_event_id to Some(1)
    let event_id_v1 = EventID::new(1);
    let v1 = ResourceSnapshot {
        metadata: ResourceMetadata {
            generation: 1,
            ..snapshot.metadata.clone()
        },
        last_event_id: Some(event_id_v1),
        ..snapshot.clone()
    };
    repo.update_resource(&v1, None).await.unwrap();

    // Second update using correct expected_last_event_id
    let event_id_v2 = EventID::new(2);
    let v2 = ResourceSnapshot {
        metadata: ResourceMetadata {
            generation: 2,
            ..v1.metadata.clone()
        },
        last_event_id: Some(event_id_v2),
        ..v1.clone()
    };
    repo.update_resource(&v2, Some(event_id_v1)).await.unwrap();

    // Stale update using the old event id should fail
    let result = repo.update_resource(&v2, Some(event_id_v1)).await;
    assert!(matches!(
        result,
        Err(UpdateResourceError::ConcurrentModification(_))
    ));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_resource_ids_with_pagination(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    for i in 1..=5_u32 {
        let mut snapshot =
            make_test_snapshot(account_id.clone(), "TestKind", &format!("resource-{i}"));
        snapshot.resource_id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }

    let first_page: Vec<_> = repo
        .list_resource_ids(
            account_id.clone(),
            "TestKind",
            PaginationOpts {
                limit: 3,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(first_page.len(), 3);

    let second_page: Vec<_> = repo
        .list_resource_ids(
            account_id.clone(),
            "TestKind",
            PaginationOpts {
                limit: 3,
                offset: 3,
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(second_page.len(), 2);

    // Ensure no overlap
    let all_ids: std::collections::HashSet<_> = first_page
        .iter()
        .chain(second_page.iter())
        .copied()
        .collect();
    assert_eq!(all_ids.len(), 5);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_resource_snapshots_by_kind(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    for i in 1..=3_u32 {
        let mut snapshot =
            make_test_snapshot(account_id.clone(), "KindA", &format!("resource-a-{i}"));
        snapshot.resource_id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }
    for i in 1..=2_u32 {
        let mut snapshot =
            make_test_snapshot(account_id.clone(), "KindB", &format!("resource-b-{i}"));
        snapshot.resource_id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }

    let kind_a: Vec<_> = repo
        .list_resource_snapshots_by_kind(
            account_id.clone(),
            "KindA",
            PaginationOpts {
                limit: 100,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(kind_a.len(), 3);
    assert!(kind_a.iter().all(|s| s.kind == "KindA"));

    let kind_b: Vec<_> = repo
        .list_resource_snapshots_by_kind(
            account_id.clone(),
            "KindB",
            PaginationOpts {
                limit: 100,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(kind_b.len(), 2);
    assert!(kind_b.iter().all(|s| s.kind == "KindB"));

    let kind_c: Vec<_> = repo
        .list_resource_snapshots_by_kind(
            account_id,
            "KindC",
            PaginationOpts {
                limit: 100,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert!(kind_c.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_all_resource_snapshots(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");
    let other_account_id = odf::AccountID::new_seeded_ed25519(b"other-account");

    for i in 1..=2_u32 {
        let mut snapshot =
            make_test_snapshot(account_id.clone(), "KindA", &format!("resource-a-{i}"));
        snapshot.resource_id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }
    for i in 1..=2_u32 {
        let mut snapshot =
            make_test_snapshot(account_id.clone(), "KindB", &format!("resource-b-{i}"));
        snapshot.resource_id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }

    // Resources for a different account — must not appear in results
    let mut other = make_test_snapshot(other_account_id, "KindA", "other-resource");
    other.resource_id = repo.new_resource_id().await.unwrap();
    repo.create_resource(&other).await.unwrap();

    let all: Vec<_> = repo
        .list_all_resource_snapshots(
            account_id,
            PaginationOpts {
                limit: 100,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(all.len(), 4);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_count_resources(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    for i in 1..=3_u32 {
        let mut snapshot =
            make_test_snapshot(account_id.clone(), "TestKind", &format!("resource-{i}"));
        snapshot.resource_id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }

    let count = repo
        .count_resources(account_id.clone(), "TestKind")
        .await
        .unwrap();
    assert_eq!(3, count);

    let count_other = repo.count_resources(account_id, "OtherKind").await.unwrap();
    assert_eq!(0, count_other);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_deleted_resource_not_returned(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let mut snapshot = make_test_snapshot(account_id.clone(), "TestKind", "to-delete");
    snapshot.resource_id = repo.new_resource_id().await.unwrap();
    let resource_id = snapshot.resource_id;
    repo.create_resource(&snapshot).await.unwrap();

    // Mark as deleted
    let deleted = ResourceSnapshot {
        metadata: ResourceMetadata {
            deleted_at: Some(Utc::now()),
            ..snapshot.metadata.clone()
        },
        ..snapshot
    };
    repo.update_resource(&deleted, None).await.unwrap();

    let by_id = repo
        .find_resource_snapshot_by_id(&resource_id)
        .await
        .unwrap();
    assert!(by_id.is_none());

    let by_query = repo
        .find_resource_snapshot(&ResourceRawEventQuery {
            kind: "TestKind".to_string(),
            id: resource_id,
        })
        .await
        .unwrap();
    assert!(by_query.is_none());

    let by_name = repo
        .find_resource_id_by_name(account_id.clone(), "TestKind", &"to-delete".to_string())
        .await
        .unwrap();
    assert!(by_name.is_none());

    let count = repo
        .count_resources(account_id.clone(), "TestKind")
        .await
        .unwrap();
    assert_eq!(0, count);

    let ids: Vec<_> = repo
        .list_resource_ids(
            account_id.clone(),
            "TestKind",
            PaginationOpts {
                limit: 100,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert!(ids.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
