// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use database_common::PaginationOpts;
use dill::Catalog;
use event_sourcing::EventID;
use futures::TryStreamExt;
use kamu_resources::{
    CreateResourceError,
    ResourceHeaders,
    ResourcePhaseCounts,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceSnapshotUpdate,
    ResourceSummaryRow,
    ResourceUID,
    UpdateResourceError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_test_snapshot(account_id: odf::AccountID, kind: &str, name: &str) -> ResourceSnapshot {
    ResourceSnapshot {
        uid: ResourceUID::new(uuid::Uuid::new_v4()),
        kind: kind.to_string(),
        api_version: "v1".to_string(),
        headers: ResourceHeaders::simple(Utc::now(), account_id, name.to_ascii_lowercase()),
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
        .list_resource_uids(
            account_id.clone(),
            "TestKind",
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert!(ids.is_empty());

    let snapshots: Vec<_> = repo
        .list_all_resource_snapshots(account_id, PaginationOpts::from_max_results(100))
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
    snapshot.uid = repo.new_resource_uid().await.unwrap();
    let uid = snapshot.uid;

    repo.create_resource(&snapshot).await.unwrap();

    // find by id
    let found = repo.find_resource_snapshot_by_uid(&uid).await.unwrap();
    assert!(found.is_some());
    let found = found.unwrap();
    assert_eq!(found.uid, uid);
    assert_eq!(found.kind, "TestKind");
    assert_eq!(found.headers.name, "my-resource");
    assert_eq!(found.last_event_id, None);

    // find by name
    let found_id = repo
        .find_resource_uid_by_name(&account_id, "TestKind", &"my-resource".to_string())
        .await
        .unwrap();
    assert_eq!(found_id, Some(uid));

    // find via raw event query
    let found = repo
        .find_resource_snapshot(&ResourceRawEventQuery {
            kind: "TestKind".to_string(),
            uid,
        })
        .await
        .unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().uid, uid);

    // wrong kind returns nothing
    let not_found = repo
        .find_resource_snapshot(&ResourceRawEventQuery {
            kind: "OtherKind".to_string(),
            uid,
        })
        .await
        .unwrap();
    assert!(not_found.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_resource_snapshots_by_uids(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");
    let other_account_id = odf::AccountID::new_seeded_ed25519(b"other-account");

    let mut first = make_test_snapshot(account_id.clone(), "TestKind", "first");
    first.uid = repo.new_resource_uid().await.unwrap();
    let mut second = make_test_snapshot(account_id.clone(), "OtherKind", "second");
    second.uid = repo.new_resource_uid().await.unwrap();
    let mut other_account = make_test_snapshot(other_account_id, "TestKind", "other-account");
    other_account.uid = repo.new_resource_uid().await.unwrap();
    let missing_uid = repo.new_resource_uid().await.unwrap();

    repo.create_resource(&first).await.unwrap();
    repo.create_resource(&second).await.unwrap();
    repo.create_resource(&other_account).await.unwrap();

    let found = repo
        .find_resource_snapshots_by_uids(
            &account_id,
            &[second.uid, missing_uid, first.uid, other_account.uid],
        )
        .await
        .unwrap();

    let found_uids = found
        .into_iter()
        .map(|snapshot| snapshot.uid)
        .collect::<Vec<_>>();
    assert_eq!(found_uids, vec![second.uid, first.uid]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_resource_snapshots_by_kind_and_uids(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");
    let other_account_id = odf::AccountID::new_seeded_ed25519(b"other-account");

    let mut first = make_test_snapshot(account_id.clone(), "TestKind", "first");
    first.uid = repo.new_resource_uid().await.unwrap();
    let mut second = make_test_snapshot(account_id.clone(), "OtherKind", "second");
    second.uid = repo.new_resource_uid().await.unwrap();
    let mut third = make_test_snapshot(other_account_id, "TestKind", "third");
    third.uid = repo.new_resource_uid().await.unwrap();
    let missing_uid = repo.new_resource_uid().await.unwrap();

    repo.create_resource(&first).await.unwrap();
    repo.create_resource(&second).await.unwrap();
    repo.create_resource(&third).await.unwrap();

    let found = repo
        .find_resource_snapshots_by_kind_and_uids(
            "TestKind",
            &[second.uid, missing_uid, third.uid, first.uid],
        )
        .await
        .unwrap();

    let found_uids = found
        .into_iter()
        .map(|snapshot| snapshot.uid)
        .collect::<Vec<_>>();
    assert_eq!(found_uids, vec![third.uid, first.uid]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_resource_identities(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");
    seed_search_resource_identities(repo.as_ref(), &account_id).await;

    // --- name_pattern: prefix wildcard matches only TestKind items for this
    // account ---
    let rows = repo
        .search_resource_identities(
            &account_id,
            &["TestKind".to_string()],
            None,
            Some("app-%"),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["app-beta", "app-alpha"]);

    // --- name_pattern is case-insensitive ---
    let rows = repo
        .search_resource_identities(
            &account_id,
            &["TestKind".to_string()],
            None,
            Some("APP-%"),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["app-beta", "app-alpha"]);

    // --- exact_names filter ---
    let rows = repo
        .search_resource_identities(
            &account_id,
            &["TestKind".to_string()],
            Some(&["app-alpha".to_string(), "db-alpha".to_string()]),
            None,
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["app-alpha", "db-alpha"]);

    // --- exact_names filter is case-insensitive ---
    let rows = repo
        .search_resource_identities(
            &account_id,
            &["TestKind".to_string()],
            Some(&["App-Alpha".to_string(), "DB-ALPHA".to_string()]),
            None,
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["app-alpha", "db-alpha"]);

    // --- multi-kind search ---
    let rows = repo
        .search_resource_identities(
            &account_id,
            &["TestKind".to_string(), "OtherKind".to_string()],
            None,
            Some("app-%"),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        vec!["app-alpha", "app-beta", "app-delta", "app-gamma"]
    );

    // --- no name_pattern and no exact_names returns all for the kind ---
    let rows = repo
        .search_resource_identities(
            &account_id,
            &["TestKind".to_string()],
            None,
            None,
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    // --- other account's resources are never returned ---
    let rows = repo
        .search_resource_identities(
            &account_id,
            &["TestKind".to_string()],
            None,
            Some("app-other-%"),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_count_search_resource_identities(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    seed_search_resource_identities(repo.as_ref(), &account_id).await;

    let count = repo
        .count_search_resource_identities(
            &account_id,
            &["TestKind".to_string()],
            None,
            Some("app-%"),
        )
        .await
        .unwrap();
    assert_eq!(count, 2);

    let count = repo
        .count_search_resource_identities(
            &account_id,
            &["TestKind".to_string()],
            Some(&["App-Alpha".to_string(), "DB-ALPHA".to_string()]),
            None,
        )
        .await
        .unwrap();
    assert_eq!(count, 2);

    let count = repo
        .count_search_resource_identities(
            &account_id,
            &["TestKind".to_string(), "OtherKind".to_string()],
            None,
            Some("app-%"),
        )
        .await
        .unwrap();
    assert_eq!(count, 4);

    let count = repo
        .count_search_resource_identities(
            &account_id,
            &["TestKind".to_string()],
            None,
            Some("app-other-%"),
        )
        .await
        .unwrap();
    assert_eq!(count, 0);

    let count = repo
        .count_search_resource_identities(&account_id, &["TestKind".to_string()], Some(&[]), None)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resource_name_case_insensitive(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    // Stored names are always lowercase (as produced by try_new).
    let mut alpha = make_test_snapshot(account_id.clone(), "TestKind", "my-resource");
    alpha.uid = repo.new_resource_uid().await.unwrap();
    let uid = alpha.uid;
    repo.create_resource(&alpha).await.unwrap();

    let mut beta = make_test_snapshot(account_id.clone(), "TestKind", "other-resource");
    beta.uid = repo.new_resource_uid().await.unwrap();
    repo.create_resource(&beta).await.unwrap();

    // --- find_resource_uid_by_name is case-insensitive ---
    let found = repo
        .find_resource_uid_by_name(&account_id, "TestKind", &"My-Resource".to_string())
        .await
        .unwrap();
    assert_eq!(found, Some(uid));

    let found = repo
        .find_resource_uid_by_name(&account_id, "TestKind", &"MY-RESOURCE".to_string())
        .await
        .unwrap();
    assert_eq!(found, Some(uid));

    // --- find_resource_identities_by_names is case-insensitive ---
    let rows = repo
        .find_resource_identities_by_names(
            &account_id,
            "TestKind",
            &["My-Resource".to_string(), "OTHER-RESOURCE".to_string()],
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|r| r.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["my-resource", "other-resource"]);

    // --- search_resource_identities exact_names is case-insensitive ---
    let rows = repo
        .search_resource_identities(
            &account_id,
            &["TestKind".to_string()],
            Some(&["MY-RESOURCE".to_string()]),
            None,
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|r| r.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["my-resource"]);

    // --- name_pattern search is case-insensitive ---
    let rows = repo
        .search_resource_identities(
            &account_id,
            &["TestKind".to_string()],
            None,
            Some("MY-%"),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|r| r.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["my-resource"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn seed_search_resource_identities(
    repo: &dyn ResourceRepository,
    account_id: &odf::AccountID,
) {
    let other_account_id = odf::AccountID::new_seeded_ed25519(b"other-account");

    for (kind, name, account) in [
        ("TestKind", "app-alpha", account_id.clone()),
        ("TestKind", "app-beta", account_id.clone()),
        ("TestKind", "db-alpha", account_id.clone()),
        ("OtherKind", "app-gamma", account_id.clone()),
        ("OtherKind", "app-delta", account_id.clone()),
        ("TestKind", "app-other-account", other_account_id),
    ] {
        let mut snapshot = make_test_snapshot(account, kind, name);
        snapshot.uid = repo.new_resource_uid().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_resource_duplicate_fails(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let mut first = make_test_snapshot(account_id.clone(), "TestKind", "duplicate-resource");
    first.uid = repo.new_resource_uid().await.unwrap();
    repo.create_resource(&first).await.unwrap();

    // Same name - should be considered duplicate
    let mut second = make_test_snapshot(account_id.clone(), "TestKind", "duplicate-resource");
    second.uid = repo.new_resource_uid().await.unwrap();

    let result = repo.create_resource(&second).await;
    assert!(matches!(result, Err(CreateResourceError::Duplicate(_))));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_resource_duplicate_ignore_case_fails(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let mut first = make_test_snapshot(account_id.clone(), "TestKind", "duplicate-resource");
    first.uid = repo.new_resource_uid().await.unwrap();
    repo.create_resource(&first).await.unwrap();

    // Same name but different case - should still be considered duplicate
    let mut second = make_test_snapshot(account_id, "TestKind", "Duplicate-Resource");
    second.uid = repo.new_resource_uid().await.unwrap();

    let result = repo.create_resource(&second).await;
    assert!(matches!(result, Err(CreateResourceError::Duplicate(_))));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_resource(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let mut snapshot = make_test_snapshot(account_id.clone(), "TestKind", "update-me");
    snapshot.uid = repo.new_resource_uid().await.unwrap();
    let uid = snapshot.uid;
    repo.create_resource(&snapshot).await.unwrap();

    let event_id = EventID::new(1);
    let updated = ResourceSnapshot {
        headers: ResourceHeaders {
            description: Some("Updated description".to_string()),
            generation: 1,
            updated_at: Utc::now(),
            ..snapshot.headers.clone()
        },
        last_event_id: Some(event_id),
        ..snapshot.clone()
    };

    repo.update_resource(&updated, None).await.unwrap();

    let found = repo
        .find_resource_snapshot_by_uid(&uid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        found.headers.description,
        Some("Updated description".to_string())
    );
    assert_eq!(found.headers.generation, 1);
    assert_eq!(found.last_event_id, Some(event_id));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_resource_wrong_event_id_fails(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let mut snapshot = make_test_snapshot(account_id, "TestKind", "concurrent-resource");
    snapshot.uid = repo.new_resource_uid().await.unwrap();
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
    snapshot.uid = repo.new_resource_uid().await.unwrap();
    repo.create_resource(&snapshot).await.unwrap();

    // First update: sets last_event_id to Some(1)
    let event_id_v1 = EventID::new(1);
    let v1 = ResourceSnapshot {
        headers: ResourceHeaders {
            generation: 1,
            ..snapshot.headers.clone()
        },
        last_event_id: Some(event_id_v1),
        ..snapshot.clone()
    };
    repo.update_resource(&v1, None).await.unwrap();

    // Second update using correct expected_last_event_id
    let event_id_v2 = EventID::new(2);
    let v2 = ResourceSnapshot {
        headers: ResourceHeaders {
            generation: 2,
            ..v1.headers.clone()
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

pub async fn test_update_resources(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let mut first = make_test_snapshot(account_id.clone(), "TestKind", "bulk-first");
    first.uid = repo.new_resource_uid().await.unwrap();
    let mut second = make_test_snapshot(account_id, "TestKind", "bulk-second");
    second.uid = repo.new_resource_uid().await.unwrap();

    repo.create_resource(&first).await.unwrap();
    repo.create_resource(&second).await.unwrap();

    let first_event_id = EventID::new(1);
    let second_event_id = EventID::new(2);

    let updated_first = ResourceSnapshot {
        headers: ResourceHeaders {
            description: Some("Updated first".to_string()),
            generation: 1,
            updated_at: Utc::now(),
            ..first.headers.clone()
        },
        last_event_id: Some(first_event_id),
        ..first.clone()
    };
    let updated_second = ResourceSnapshot {
        headers: ResourceHeaders {
            description: Some("Updated second".to_string()),
            generation: 1,
            updated_at: Utc::now(),
            ..second.headers.clone()
        },
        last_event_id: Some(second_event_id),
        ..second.clone()
    };

    repo.update_resources(&[
        ResourceSnapshotUpdate {
            snapshot: updated_first.clone(),
            expected_last_event_id: None,
        },
        ResourceSnapshotUpdate {
            snapshot: updated_second.clone(),
            expected_last_event_id: None,
        },
    ])
    .await
    .unwrap();

    let found_first = repo
        .find_resource_snapshot_by_uid(&first.uid)
        .await
        .unwrap()
        .unwrap();
    let found_second = repo
        .find_resource_snapshot_by_uid(&second.uid)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        found_first.headers.description,
        updated_first.headers.description
    );
    assert_eq!(found_first.headers.generation, 1);
    assert_eq!(found_first.last_event_id, Some(first_event_id));

    assert_eq!(
        found_second.headers.description,
        updated_second.headers.description
    );
    assert_eq!(found_second.headers.generation, 1);
    assert_eq!(found_second.last_event_id, Some(second_event_id));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_resources_wrong_event_id_fails(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let mut first = make_test_snapshot(account_id.clone(), "TestKind", "bulk-concurrent-first");
    first.uid = repo.new_resource_uid().await.unwrap();
    let mut second = make_test_snapshot(account_id, "TestKind", "bulk-concurrent-second");
    second.uid = repo.new_resource_uid().await.unwrap();

    repo.create_resource(&first).await.unwrap();
    repo.create_resource(&second).await.unwrap();

    let updated_first = ResourceSnapshot {
        headers: ResourceHeaders {
            generation: 1,
            ..first.headers.clone()
        },
        last_event_id: Some(EventID::new(1)),
        ..first.clone()
    };
    let updated_second = ResourceSnapshot {
        headers: ResourceHeaders {
            generation: 1,
            ..second.headers.clone()
        },
        last_event_id: Some(EventID::new(2)),
        ..second.clone()
    };

    let result = repo
        .update_resources(&[
            ResourceSnapshotUpdate {
                snapshot: updated_first,
                expected_last_event_id: Some(EventID::new(99)),
            },
            ResourceSnapshotUpdate {
                snapshot: updated_second,
                expected_last_event_id: None,
            },
        ])
        .await;

    assert!(matches!(
        result,
        Err(UpdateResourceError::ConcurrentModification(_))
    ));

    let found_first = repo
        .find_resource_snapshot_by_uid(&first.uid)
        .await
        .unwrap()
        .unwrap();
    let found_second = repo
        .find_resource_snapshot_by_uid(&second.uid)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(found_first.last_event_id, None);
    assert_eq!(found_first.headers.generation, 0);
    assert_eq!(found_second.last_event_id, None);
    assert_eq!(found_second.headers.generation, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_resource_ids_with_pagination(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    for i in 1..=5_u32 {
        let mut snapshot =
            make_test_snapshot(account_id.clone(), "TestKind", &format!("resource-{i}"));
        snapshot.uid = repo.new_resource_uid().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }

    let first_page: Vec<_> = repo
        .list_resource_uids(
            account_id.clone(),
            "TestKind",
            PaginationOpts::from_max_results(3),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(first_page.len(), 3);

    let second_page: Vec<_> = repo
        .list_resource_uids(
            account_id.clone(),
            "TestKind",
            PaginationOpts::from_page(1, 3),
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
        snapshot.uid = repo.new_resource_uid().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }
    for i in 1..=2_u32 {
        let mut snapshot =
            make_test_snapshot(account_id.clone(), "KindB", &format!("resource-b-{i}"));
        snapshot.uid = repo.new_resource_uid().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }

    let kind_a: Vec<_> = repo
        .list_resource_snapshots_by_kind(
            account_id.clone(),
            "KindA",
            PaginationOpts::from_max_results(100),
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
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(kind_b.len(), 2);
    assert!(kind_b.iter().all(|s| s.kind == "KindB"));

    let kind_c: Vec<_> = repo
        .list_resource_snapshots_by_kind(account_id, "KindC", PaginationOpts::from_max_results(100))
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
        snapshot.uid = repo.new_resource_uid().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }
    for i in 1..=2_u32 {
        let mut snapshot =
            make_test_snapshot(account_id.clone(), "KindB", &format!("resource-b-{i}"));
        snapshot.uid = repo.new_resource_uid().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }

    // Resources for a different account — must not appear in results
    let mut other = make_test_snapshot(other_account_id, "KindA", "other-resource");
    other.uid = repo.new_resource_uid().await.unwrap();
    repo.create_resource(&other).await.unwrap();

    let all: Vec<_> = repo
        .list_all_resource_snapshots(account_id, PaginationOpts::from_max_results(100))
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
        snapshot.uid = repo.new_resource_uid().await.unwrap();
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

pub async fn test_summarize_resources(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");
    let other_account_id = odf::AccountID::new_seeded_ed25519(b"other-account");

    let mut pending = make_test_snapshot(account_id.clone(), "KindA", "pending");
    pending.uid = repo.new_resource_uid().await.unwrap();
    pending.status = None;
    repo.create_resource(&pending).await.unwrap();

    let mut ready = make_test_snapshot(account_id.clone(), "KindA", "ready");
    ready.uid = repo.new_resource_uid().await.unwrap();
    ready.status = Some(serde_json::json!({ "phase": "Ready" }));
    repo.create_resource(&ready).await.unwrap();

    let mut degraded_v2 = make_test_snapshot(account_id.clone(), "KindA", "degraded-v2");
    degraded_v2.uid = repo.new_resource_uid().await.unwrap();
    degraded_v2.api_version = "v2".to_string();
    degraded_v2.status = Some(serde_json::json!({ "phase": "Degraded" }));
    repo.create_resource(&degraded_v2).await.unwrap();

    let mut failed = make_test_snapshot(account_id.clone(), "KindB", "failed");
    failed.uid = repo.new_resource_uid().await.unwrap();
    failed.status = Some(serde_json::json!({ "phase": "Failed" }));
    repo.create_resource(&failed).await.unwrap();

    let mut unknown_phase = make_test_snapshot(account_id.clone(), "KindB", "unknown");
    unknown_phase.uid = repo.new_resource_uid().await.unwrap();
    unknown_phase.status = Some(serde_json::json!({ "phase": "UnknownFuturePhase" }));
    repo.create_resource(&unknown_phase).await.unwrap();

    let mut deleted = make_test_snapshot(account_id.clone(), "KindB", "deleted");
    deleted.uid = repo.new_resource_uid().await.unwrap();
    deleted.status = Some(serde_json::json!({ "phase": "Reconciling" }));
    repo.create_resource(&deleted).await.unwrap();
    let deleted = ResourceSnapshot {
        headers: ResourceHeaders {
            deleted_at: Some(Utc::now()),
            ..deleted.headers
        },
        ..deleted
    };
    repo.update_resource(&deleted, None).await.unwrap();

    let mut other_account = make_test_snapshot(other_account_id, "KindA", "other-account");
    other_account.uid = repo.new_resource_uid().await.unwrap();
    other_account.status = Some(serde_json::json!({ "phase": "Reconciling" }));
    repo.create_resource(&other_account).await.unwrap();

    let summary = repo.summarize_resources(account_id).await.unwrap();

    assert_eq!(
        summary,
        vec![
            ResourceSummaryRow {
                kind: "KindA".to_string(),
                api_version: "v1".to_string(),
                total_count: 2,
                phase_counts: ResourcePhaseCounts {
                    pending: 1,
                    reconciling: 0,
                    ready: 1,
                    degraded: 0,
                    failed: 0,
                },
            },
            ResourceSummaryRow {
                kind: "KindA".to_string(),
                api_version: "v2".to_string(),
                total_count: 1,
                phase_counts: ResourcePhaseCounts {
                    pending: 0,
                    reconciling: 0,
                    ready: 0,
                    degraded: 1,
                    failed: 0,
                },
            },
            ResourceSummaryRow {
                kind: "KindB".to_string(),
                api_version: "v1".to_string(),
                total_count: 2,
                phase_counts: ResourcePhaseCounts {
                    pending: 1,
                    reconciling: 0,
                    ready: 0,
                    degraded: 0,
                    failed: 1,
                },
            },
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_deleted_resource_not_returned(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_id = odf::AccountID::new_seeded_ed25519(b"test-account");

    let mut snapshot = make_test_snapshot(account_id.clone(), "TestKind", "to-delete");
    snapshot.uid = repo.new_resource_uid().await.unwrap();
    let uid = snapshot.uid;
    repo.create_resource(&snapshot).await.unwrap();

    // Mark as deleted
    let deleted = ResourceSnapshot {
        headers: ResourceHeaders {
            deleted_at: Some(Utc::now()),
            ..snapshot.headers.clone()
        },
        ..snapshot
    };
    repo.update_resource(&deleted, None).await.unwrap();

    let by_id = repo.find_resource_snapshot_by_uid(&uid).await.unwrap();
    assert!(by_id.is_none());

    let by_query = repo
        .find_resource_snapshot(&ResourceRawEventQuery {
            kind: "TestKind".to_string(),
            uid,
        })
        .await
        .unwrap();
    assert!(by_query.is_none());

    let by_name = repo
        .find_resource_uid_by_name(&account_id, "TestKind", &"to-delete".to_string())
        .await
        .unwrap();
    assert!(by_name.is_none());

    let count = repo
        .count_resources(account_id.clone(), "TestKind")
        .await
        .unwrap();
    assert_eq!(0, count);

    let ids: Vec<_> = repo
        .list_resource_uids(
            account_id.clone(),
            "TestKind",
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert!(ids.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
