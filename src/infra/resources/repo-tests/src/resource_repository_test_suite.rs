// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

use chrono::Utc;
use database_common::PaginationOpts;
use dill::Catalog;
use event_sourcing::EventID;
use futures::TryStreamExt;
use kamu_accounts::{Account, AccountRepository, AccountType};
use kamu_resources::{
    CreateResourceError,
    ResourceHeaders,
    ResourceHeadersExt,
    ResourceID,
    ResourcePhase,
    ResourcePhaseCounts,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceSnapshotUpdate,
    ResourceSummaryRow,
    TypeUri,
    UpdateResourceError,
    new_pending_resource_status,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static TEST_KIND: LazyLock<TypeUri> = LazyLock::new(|| TypeUri::new_unchecked("TestKind"));
static OTHER_KIND: LazyLock<TypeUri> = LazyLock::new(|| TypeUri::new_unchecked("OtherKind"));
static KIND_A: LazyLock<TypeUri> = LazyLock::new(|| TypeUri::new_unchecked("KindA"));
static KIND_A_V2: LazyLock<TypeUri> = LazyLock::new(|| TypeUri::new_unchecked("KindA-v2"));
static KIND_B: LazyLock<TypeUri> = LazyLock::new(|| TypeUri::new_unchecked("KindB"));
static KIND_C: LazyLock<TypeUri> = LazyLock::new(|| TypeUri::new_unchecked("KindC"));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_test_snapshot(
    account: &odf::AccountHandle,
    schema: &TypeUri,
    name: &str,
) -> ResourceSnapshot {
    let id = ResourceID::new(uuid::Uuid::new_v4());

    ResourceSnapshot {
        id,
        schema: schema.clone(),
        headers: ResourceHeaders::simple(Utc::now(), id, account.clone(), name),
        spec: serde_json::json!({"key": "value"}),
        status: None,
        last_event_id: None,
    }
}

fn status_with_phase(phase: ResourcePhase) -> kamu_resources::ResourceStatus {
    let mut status = new_pending_resource_status();
    status.phase = phase;
    status
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_no_resources_initially(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_name = odf::AccountName::new_unchecked("test-account");
    let account_id = odf::AccountID::new_seeded_ed25519(account_name.as_bytes());

    let count = repo
        .count_resources(account_id.clone(), &TEST_KIND)
        .await
        .unwrap();
    assert_eq!(0, count);

    let ids: Vec<_> = repo
        .list_resource_ids(
            account_id.clone(),
            &TEST_KIND,
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

    let account_handle = odf::AccountHandle::new_test("test-account");

    let mut snapshot = make_test_snapshot(&account_handle, &TEST_KIND, "my-resource");
    snapshot.id = repo.new_resource_id().await.unwrap();
    let id = snapshot.id;

    repo.create_resource(&snapshot).await.unwrap();

    // find by id
    let found = repo.find_resource_snapshot_by_id(&id).await.unwrap();
    assert!(found.is_some());
    let found = found.unwrap();
    assert_eq!(found.id, id);
    assert_eq!(found.schema, *TEST_KIND);
    assert_eq!(found.headers.name, "my-resource");
    assert_eq!(found.last_event_id, None);

    // find by name
    let found_id = repo
        .find_resource_id_by_name(
            &account_handle.id,
            &TEST_KIND,
            &"my-resource".parse().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(found_id, Some(id));

    // find via raw event query
    let found = repo
        .find_resource_snapshot(&ResourceRawEventQuery {
            schema: TEST_KIND.clone(),
            id,
        })
        .await
        .unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().id, id);

    // wrong kind returns nothing
    let not_found = repo
        .find_resource_snapshot(&ResourceRawEventQuery {
            schema: OTHER_KIND.clone(),
            id,
        })
        .await
        .unwrap();
    assert!(not_found.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_find_update_resource_with_populated_labels_annotations(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    let mut snapshot = make_test_snapshot(&account_handle, &TEST_KIND, "labeled-resource");
    snapshot.id = repo.new_resource_id().await.unwrap();
    let id = snapshot.id;

    snapshot
        .headers
        .labels
        .entries
        .insert("env".parse().unwrap(), serde_json::json!("prod"));
    snapshot.headers.labels.entries.insert(
        "https://opendatafabric.org/schemas/labels/v1/Team"
            .parse()
            .unwrap(),
        serde_json::json!({ "name": "data-platform", "oncall": ["alice", "bob"] }),
    );
    snapshot.headers.annotations.entries.insert(
        "https://opendatafabric.org/schemas/labels/v1/Repo"
            .parse()
            .unwrap(),
        serde_json::json!("https://github.com/open-data-fabric/spec"),
    );

    repo.create_resource(&snapshot).await.unwrap();

    let found = repo
        .find_resource_snapshot_by_id(&id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.headers.labels, snapshot.headers.labels);
    assert_eq!(found.headers.annotations, snapshot.headers.annotations);

    // update: change a value, add a key, remove a key
    let mut updated_headers = found.headers.clone();
    updated_headers
        .labels
        .entries
        .insert("env".parse().unwrap(), serde_json::json!("staging"));
    updated_headers.labels.entries.remove(
        &"https://opendatafabric.org/schemas/labels/v1/Team"
            .parse()
            .unwrap(),
    );
    updated_headers.annotations.entries.insert(
        "owner".parse().unwrap(),
        serde_json::json!("https://github.com/open-data-fabric"),
    );

    let event_id = EventID::new(1);
    let updated_snapshot = ResourceSnapshot {
        headers: updated_headers.clone(),
        last_event_id: Some(event_id),
        ..found.clone()
    };

    repo.update_resource(&updated_snapshot, found.last_event_id)
        .await
        .unwrap();

    let found_after_update = repo
        .find_resource_snapshot_by_id(&id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found_after_update.headers.labels, updated_headers.labels);
    assert_eq!(
        found_after_update.headers.annotations,
        updated_headers.annotations
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_resource_snapshots_by_ids(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");
    let other_account_handle = odf::AccountHandle::new_test("other-account");

    let mut first = make_test_snapshot(&account_handle, &TEST_KIND, "first");
    first.id = repo.new_resource_id().await.unwrap();
    let mut second = make_test_snapshot(&account_handle, &OTHER_KIND, "second");
    second.id = repo.new_resource_id().await.unwrap();
    let mut other_account = make_test_snapshot(&other_account_handle, &TEST_KIND, "other-account");
    other_account.id = repo.new_resource_id().await.unwrap();
    let missing_id = repo.new_resource_id().await.unwrap();

    repo.create_resource(&first).await.unwrap();
    repo.create_resource(&second).await.unwrap();
    repo.create_resource(&other_account).await.unwrap();

    let found = repo
        .find_resource_snapshots_by_ids(
            &account_handle.id,
            &[second.id, missing_id, first.id, other_account.id],
        )
        .await
        .unwrap();

    let found_ids = found
        .into_iter()
        .map(|snapshot| snapshot.id)
        .collect::<Vec<_>>();
    assert_eq!(found_ids, vec![second.id, first.id]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_resource_snapshots_by_schema_and_ids(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");
    let other_account_handle = odf::AccountHandle::new_test("other-account");

    let mut first = make_test_snapshot(&account_handle, &TEST_KIND, "first");
    first.id = repo.new_resource_id().await.unwrap();
    let mut second = make_test_snapshot(&account_handle, &OTHER_KIND, "second");
    second.id = repo.new_resource_id().await.unwrap();
    let mut third = make_test_snapshot(&other_account_handle, &TEST_KIND, "third");
    third.id = repo.new_resource_id().await.unwrap();
    let missing_id = repo.new_resource_id().await.unwrap();

    repo.create_resource(&first).await.unwrap();
    repo.create_resource(&second).await.unwrap();
    repo.create_resource(&third).await.unwrap();

    let found = repo
        .find_resource_snapshots_by_schema_and_ids(
            &TEST_KIND,
            &[second.id, missing_id, third.id, first.id],
        )
        .await
        .unwrap();

    let found_ids = found
        .into_iter()
        .map(|snapshot| snapshot.id)
        .collect::<Vec<_>>();
    assert_eq!(found_ids, vec![third.id, first.id]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_resource_handles(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    seed_search_resource_handles(repo.as_ref(), &account_handle).await;

    // --- name_pattern: prefix wildcard matches only TestKind items for this
    // account ---
    let rows = repo
        .search_resource_handles(
            &account_handle.id,
            std::slice::from_ref(&TEST_KIND),
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
        .search_resource_handles(
            &account_handle.id,
            std::slice::from_ref(&TEST_KIND),
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
        .search_resource_handles(
            &account_handle.id,
            std::slice::from_ref(&TEST_KIND),
            Some(&["app-alpha".parse().unwrap(), "db-alpha".parse().unwrap()]),
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
        .search_resource_handles(
            &account_handle.id,
            std::slice::from_ref(&TEST_KIND),
            Some(&["App-Alpha".parse().unwrap(), "DB-ALPHA".parse().unwrap()]),
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
        .search_resource_handles(
            &account_handle.id,
            &[TEST_KIND.clone(), OTHER_KIND.clone()],
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
        .search_resource_handles(
            &account_handle.id,
            std::slice::from_ref(&TEST_KIND),
            None,
            None,
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    // --- other account's resources are never returned ---
    let rows = repo
        .search_resource_handles(
            &account_handle.id,
            std::slice::from_ref(&TEST_KIND),
            None,
            Some("app-other-%"),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_count_search_resource_handles(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    seed_search_resource_handles(repo.as_ref(), &account_handle).await;

    let count = repo
        .count_search_resource_handles(
            &account_handle.id,
            std::slice::from_ref(&TEST_KIND),
            None,
            Some("app-%"),
        )
        .await
        .unwrap();
    assert_eq!(count, 2);

    let count = repo
        .count_search_resource_handles(
            &account_handle.id,
            std::slice::from_ref(&TEST_KIND),
            Some(&["App-Alpha".parse().unwrap(), "DB-ALPHA".parse().unwrap()]),
            None,
        )
        .await
        .unwrap();
    assert_eq!(count, 2);

    let count = repo
        .count_search_resource_handles(
            &account_handle.id,
            &[TEST_KIND.clone(), OTHER_KIND.clone()],
            None,
            Some("app-%"),
        )
        .await
        .unwrap();
    assert_eq!(count, 4);

    let count = repo
        .count_search_resource_handles(
            &account_handle.id,
            std::slice::from_ref(&TEST_KIND),
            None,
            Some("app-other-%"),
        )
        .await
        .unwrap();
    assert_eq!(count, 0);

    let count = repo
        .count_search_resource_handles(
            &account_handle.id,
            std::slice::from_ref(&TEST_KIND),
            Some(&[]),
            None,
        )
        .await
        .unwrap();
    assert_eq!(count, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resource_name_case_insensitive(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    // Stored names are always lowercase (as produced by try_new).
    let mut alpha = make_test_snapshot(&account_handle, &TEST_KIND, "my-resource");
    alpha.id = repo.new_resource_id().await.unwrap();
    let id = alpha.id;
    repo.create_resource(&alpha).await.unwrap();

    let mut beta = make_test_snapshot(&account_handle, &TEST_KIND, "other-resource");
    beta.id = repo.new_resource_id().await.unwrap();
    repo.create_resource(&beta).await.unwrap();

    // --- find_resource_id_by_name is case-insensitive ---
    let found = repo
        .find_resource_id_by_name(
            &account_handle.id,
            &TEST_KIND,
            &"My-Resource".parse().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(found, Some(id));

    let found = repo
        .find_resource_id_by_name(
            &account_handle.id,
            &TEST_KIND,
            &"MY-RESOURCE".parse().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(found, Some(id));

    // --- find_resource_handles_by_names is case-insensitive ---
    let rows = repo
        .find_resource_handles_by_names(
            &account_handle.id,
            &TEST_KIND,
            &[
                "My-Resource".parse().unwrap(),
                "OTHER-RESOURCE".parse().unwrap(),
            ],
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|r| r.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["my-resource", "other-resource"]);

    // --- search_resource_handles exact_names is case-insensitive ---
    let rows = repo
        .search_resource_handles(
            &account_handle.id,
            std::slice::from_ref(&TEST_KIND),
            Some(&["MY-RESOURCE".parse().unwrap()]),
            None,
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|r| r.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["my-resource"]);

    // --- name_pattern search is case-insensitive ---
    let rows = repo
        .search_resource_handles(
            &account_handle.id,
            std::slice::from_ref(&TEST_KIND),
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

async fn seed_search_resource_handles(
    repo: &dyn ResourceRepository,
    account_handle: &odf::AccountHandle,
) {
    let other_account_handle = odf::AccountHandle::new_test("other-account");

    for (kind, name, account) in [
        (&*TEST_KIND, "app-alpha", account_handle.clone()),
        (&*TEST_KIND, "app-beta", account_handle.clone()),
        (&*TEST_KIND, "db-alpha", account_handle.clone()),
        (&*OTHER_KIND, "app-gamma", account_handle.clone()),
        (&*OTHER_KIND, "app-delta", account_handle.clone()),
        (&*TEST_KIND, "app-other-account", other_account_handle),
    ] {
        let mut snapshot = make_test_snapshot(&account, kind, name);
        snapshot.id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_resource_duplicate_fails(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    let mut first = make_test_snapshot(&account_handle, &TEST_KIND, "duplicate-resource");
    first.id = repo.new_resource_id().await.unwrap();
    repo.create_resource(&first).await.unwrap();

    // Same name - should be considered duplicate
    let mut second = make_test_snapshot(&account_handle, &TEST_KIND, "duplicate-resource");
    second.id = repo.new_resource_id().await.unwrap();

    let result = repo.create_resource(&second).await;
    assert!(matches!(result, Err(CreateResourceError::Duplicate(_))));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_resource_duplicate_ignore_case_fails(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    let mut first = make_test_snapshot(&account_handle, &TEST_KIND, "duplicate-resource");
    first.id = repo.new_resource_id().await.unwrap();
    repo.create_resource(&first).await.unwrap();

    // Same name but different case - should still be considered duplicate
    let mut second = make_test_snapshot(&account_handle, &TEST_KIND, "Duplicate-Resource");
    second.id = repo.new_resource_id().await.unwrap();

    let result = repo.create_resource(&second).await;
    assert!(matches!(result, Err(CreateResourceError::Duplicate(_))));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_resource(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    let mut snapshot = make_test_snapshot(&account_handle, &TEST_KIND, "update-me");
    snapshot.id = repo.new_resource_id().await.unwrap();
    let id = snapshot.id;
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
        .find_resource_snapshot_by_id(&id)
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

    let account_handle = odf::AccountHandle::new_test("test-account");

    let mut snapshot = make_test_snapshot(&account_handle, &TEST_KIND, "concurrent-resource");
    snapshot.id = repo.new_resource_id().await.unwrap();
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

    let account_handle = odf::AccountHandle::new_test("test-account");

    let mut snapshot = make_test_snapshot(&account_handle, &TEST_KIND, "locked-resource");
    snapshot.id = repo.new_resource_id().await.unwrap();
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

    let account_handle = odf::AccountHandle::new_test("test-account");

    let mut first = make_test_snapshot(&account_handle, &TEST_KIND, "bulk-first");
    first.id = repo.new_resource_id().await.unwrap();
    let mut second = make_test_snapshot(&account_handle, &TEST_KIND, "bulk-second");
    second.id = repo.new_resource_id().await.unwrap();

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
        .find_resource_snapshot_by_id(&first.id)
        .await
        .unwrap()
        .unwrap();
    let found_second = repo
        .find_resource_snapshot_by_id(&second.id)
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

    let account_handle = odf::AccountHandle::new_test("test-account");

    let mut first = make_test_snapshot(&account_handle, &TEST_KIND, "bulk-concurrent-first");
    first.id = repo.new_resource_id().await.unwrap();
    let mut second = make_test_snapshot(&account_handle, &TEST_KIND, "bulk-concurrent-second");
    second.id = repo.new_resource_id().await.unwrap();

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
        .find_resource_snapshot_by_id(&first.id)
        .await
        .unwrap()
        .unwrap();
    let found_second = repo
        .find_resource_snapshot_by_id(&second.id)
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

    let account_handle = odf::AccountHandle::new_test("test-account");

    for i in 1..=5_u32 {
        let mut snapshot =
            make_test_snapshot(&account_handle, &TEST_KIND, &format!("resource-{i}"));
        snapshot.id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }

    let first_page: Vec<_> = repo
        .list_resource_ids(
            account_handle.id.clone(),
            &TEST_KIND,
            PaginationOpts::from_max_results(3),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(first_page.len(), 3);

    let second_page: Vec<_> = repo
        .list_resource_ids(
            account_handle.id.clone(),
            &TEST_KIND,
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

pub async fn test_list_resource_snapshots_by_schema(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    for i in 1..=3_u32 {
        let mut snapshot = make_test_snapshot(&account_handle, &KIND_A, &format!("resource-a-{i}"));
        snapshot.id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }
    for i in 1..=2_u32 {
        let mut snapshot = make_test_snapshot(&account_handle, &KIND_B, &format!("resource-b-{i}"));
        snapshot.id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }

    let kind_a: Vec<_> = repo
        .list_resource_snapshots_by_schema(
            account_handle.id.clone(),
            &KIND_A,
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(kind_a.len(), 3);
    assert!(kind_a.iter().all(|s| s.schema == *KIND_A));

    let kind_b: Vec<_> = repo
        .list_resource_snapshots_by_schema(
            account_handle.id.clone(),
            &KIND_B,
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(kind_b.len(), 2);
    assert!(kind_b.iter().all(|s| s.schema == *KIND_B));

    let kind_c: Vec<_> = repo
        .list_resource_snapshots_by_schema(
            account_handle.id,
            &KIND_C,
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert!(kind_c.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_all_resource_snapshots(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");
    let other_account_handle = odf::AccountHandle::new_test("other-account");

    for i in 1..=2_u32 {
        let mut snapshot = make_test_snapshot(&account_handle, &KIND_A, &format!("resource-a-{i}"));
        snapshot.id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }
    for i in 1..=2_u32 {
        let mut snapshot = make_test_snapshot(&account_handle, &KIND_B, &format!("resource-b-{i}"));
        snapshot.id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }

    // Resources for a different account — must not appear in results
    let mut other = make_test_snapshot(&other_account_handle, &KIND_A, "other-resource");
    other.id = repo.new_resource_id().await.unwrap();
    repo.create_resource(&other).await.unwrap();

    let all: Vec<_> = repo
        .list_all_resource_snapshots(account_handle.id, PaginationOpts::from_max_results(100))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(all.len(), 4);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_count_resources(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    for i in 1..=3_u32 {
        let mut snapshot =
            make_test_snapshot(&account_handle, &TEST_KIND, &format!("resource-{i}"));
        snapshot.id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }

    let count = repo
        .count_resources(account_handle.id.clone(), &TEST_KIND)
        .await
        .unwrap();
    assert_eq!(3, count);

    let count_other = repo
        .count_resources(account_handle.id, &OTHER_KIND)
        .await
        .unwrap();
    assert_eq!(0, count_other);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_summarize_resources(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");
    let other_account_handle = odf::AccountHandle::new_test("other-account");

    let mut pending = make_test_snapshot(&account_handle, &KIND_A, "pending");
    pending.id = repo.new_resource_id().await.unwrap();
    pending.status = None;
    repo.create_resource(&pending).await.unwrap();

    let mut ready = make_test_snapshot(&account_handle, &KIND_A, "ready");
    ready.id = repo.new_resource_id().await.unwrap();
    ready.status = Some(status_with_phase(ResourcePhase::Ready));
    repo.create_resource(&ready).await.unwrap();

    let mut pending_v2 = make_test_snapshot(&account_handle, &KIND_A, "pending-v2");
    pending_v2.id = repo.new_resource_id().await.unwrap();
    pending_v2.schema = KIND_A_V2.clone();
    pending_v2.status = None;
    repo.create_resource(&pending_v2).await.unwrap();

    let mut failed = make_test_snapshot(&account_handle, &KIND_B, "failed");
    failed.id = repo.new_resource_id().await.unwrap();
    failed.status = Some(status_with_phase(ResourcePhase::Failed));
    repo.create_resource(&failed).await.unwrap();

    let mut pending_b = make_test_snapshot(&account_handle, &KIND_B, "pending-b");
    pending_b.id = repo.new_resource_id().await.unwrap();
    pending_b.status = None;
    repo.create_resource(&pending_b).await.unwrap();

    let mut deleted = make_test_snapshot(&account_handle, &KIND_B, "deleted");
    deleted.id = repo.new_resource_id().await.unwrap();
    deleted.status = Some(status_with_phase(ResourcePhase::Reconciling));
    repo.create_resource(&deleted).await.unwrap();
    let deleted = ResourceSnapshot {
        headers: ResourceHeaders {
            deleted_at: Some(Utc::now()),
            ..deleted.headers
        },
        ..deleted
    };
    repo.update_resource(&deleted, None).await.unwrap();

    let mut other_account = make_test_snapshot(&other_account_handle, &KIND_A, "other-account");
    other_account.id = repo.new_resource_id().await.unwrap();
    other_account.status = Some(status_with_phase(ResourcePhase::Reconciling));
    repo.create_resource(&other_account).await.unwrap();

    let summary = repo.summarize_resources(account_handle.id).await.unwrap();

    assert_eq!(
        summary,
        vec![
            ResourceSummaryRow {
                schema: "KindA".to_string(),
                total_count: 2,
                phase_counts: ResourcePhaseCounts {
                    pending: 1,
                    reconciling: 0,
                    ready: 1,
                    failed: 0,
                },
            },
            ResourceSummaryRow {
                schema: "KindA-v2".to_string(),
                total_count: 1,
                phase_counts: ResourcePhaseCounts {
                    pending: 1,
                    reconciling: 0,
                    ready: 0,
                    failed: 0,
                },
            },
            ResourceSummaryRow {
                schema: "KindB".to_string(),
                total_count: 2,
                phase_counts: ResourcePhaseCounts {
                    pending: 1,
                    reconciling: 0,
                    ready: 0,
                    failed: 1,
                },
            },
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_deleted_resource_not_returned(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    let mut snapshot = make_test_snapshot(&account_handle, &TEST_KIND, "to-delete");
    snapshot.id = repo.new_resource_id().await.unwrap();
    let id = snapshot.id;
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

    let by_id = repo.find_resource_snapshot_by_id(&id).await.unwrap();
    assert!(by_id.is_none());

    let by_query = repo
        .find_resource_snapshot(&ResourceRawEventQuery {
            schema: TEST_KIND.clone(),
            id,
        })
        .await
        .unwrap();
    assert!(by_query.is_none());

    let by_name = repo
        .find_resource_id_by_name(
            &account_handle.id,
            &TEST_KIND,
            &"to-delete".parse().unwrap(),
        )
        .await
        .unwrap();
    assert!(by_name.is_none());

    let count = repo
        .count_resources(account_handle.id.clone(), &TEST_KIND)
        .await
        .unwrap();
    assert_eq!(0, count);

    let ids: Vec<_> = repo
        .list_resource_ids(
            account_handle.id,
            &TEST_KIND,
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert!(ids.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_test_account(handle: &odf::AccountHandle) -> Account {
    let name = handle.name.as_str();
    Account {
        id: handle.id.clone(),
        account_name: handle.name.clone(),
        email: email_utils::Email::parse(&format!("{name}@example.com")).unwrap(),
        display_name: name.to_string(),
        account_type: AccountType::User,
        avatar_url: None,
        registered_at: Utc::now(),
        provider: "unit-test-provider".to_string(),
        provider_identity_key: name.to_string(),
    }
}

/// Pins the JOIN-on-read design's key correctness property: because the
/// owning account's name is never denormalized into the `resources`
/// row/snapshot, an account rename is reflected **immediately** on the next
/// read — no stale name, no backfill required. This is the reason a JOIN
/// (or, for the inmem backend, a live account-repo lookup) was chosen over
/// copying the name at write time.
pub async fn test_account_rename_reflected_immediately_in_headers(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

    let original_handle = odf::AccountHandle::new_test("rename-me");
    let original_account = make_test_account(&original_handle);
    account_repo.save_account(&original_account).await.unwrap();

    let mut snapshot = make_test_snapshot(&original_handle, &TEST_KIND, "owned-resource");
    snapshot.id = repo.new_resource_id().await.unwrap();
    let id = snapshot.id;
    repo.create_resource(&snapshot).await.unwrap();

    let before_rename = repo
        .find_resource_snapshot_by_id(&id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before_rename.headers.account.id, original_account.id);
    assert_eq!(before_rename.headers.account.name, original_handle.name);

    let new_name = odf::AccountName::new_unchecked("renamed");
    let renamed_account = Account {
        account_name: new_name.clone(),
        ..original_account
    };
    account_repo.update_account(&renamed_account).await.unwrap();

    let after_rename = repo
        .find_resource_snapshot_by_id(&id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_rename.headers.account.id, original_handle.id);
    assert_eq!(
        after_rename.headers.account.name, new_name,
        "account rename must be reflected immediately on re-read, without any resource-side update"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `resources.account_id` has no FK-cascade tie to the `accounts` table
/// (different bounded context; cleanup of a deleted account's resources is
/// async, via outbox handlers), so a resource can transiently reference an
/// account that no longer exists. Reads must not fail in that window — they
/// fall back to a sentinel account name instead of erroring or panicking.
pub async fn test_deleted_account_falls_back_to_sentinel_name(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    // Deliberately do NOT seed an `accounts` row for this id — simulates the
    // account having been deleted (or never synced) while its resources
    // still exist.
    let account_handle = odf::AccountHandle::new_test("no-such-account");

    let mut snapshot = make_test_snapshot(&account_handle, &TEST_KIND, "orphaned-resource");
    snapshot.id = repo.new_resource_id().await.unwrap();
    let id = snapshot.id;
    repo.create_resource(&snapshot).await.unwrap();

    let found = repo
        .find_resource_snapshot_by_id(&id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(found.headers.account.id, account_handle.id);
    assert_eq!(
        found.headers.account.name,
        kamu_resources::deleted_account_name_sentinel(),
        "reads for a resource whose account is gone must fall back to the sentinel name, not fail"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
