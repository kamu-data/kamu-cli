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
    ResourceLabelPair,
    ResourceLabelProjectionRepository,
    ResourcePhase,
    ResourcePhaseCounts,
    ResourceQuery,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceScope,
    ResourceSnapshot,
    ResourceSnapshotUpdate,
    ResourceSummaryRow,
    ResourceTypeQuery,
    TypeRef,
    TypeUri,
    UpdateResourceError,
    description_annotation_type_ref,
    get_description,
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

pub(crate) fn make_test_snapshot(
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
        .list_resource_snapshots(
            &account_id,
            &ResourceScope::default(),
            PaginationOpts::from_max_results(100),
        )
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

    let found = repo.find_resource_snapshot_by_id(&id).await.unwrap();
    assert!(found.is_some());
    let found = found.unwrap();
    assert_eq!(found.id, id);
    assert_eq!(found.schema, *TEST_KIND);
    assert_eq!(found.headers.name, "my-resource");
    assert_eq!(found.last_event_id, None);

    let found_id = repo
        .find_resource_id_by_name(
            &account_handle.did,
            &TEST_KIND,
            &"my-resource".parse().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(found_id, Some(id));

    let found = repo
        .find_resource_snapshot(&ResourceRawEventQuery {
            schema: TEST_KIND.clone(),
            id,
        })
        .await
        .unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().id, id);

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
            &account_handle.did,
            &[second.id, missing_id, first.id, other_account.id],
        )
        .await
        .unwrap();

    let found_ids = found
        .into_iter()
        .map(|snapshot| snapshot.id)
        .collect::<Vec<_>>();
    assert_eq!(found_ids, vec![second.id, first.id]);

    // Request order is the contract, so reversing the request must reverse the
    // result. Asserting only the forward case would pass against a backend that
    // returns an arbitrary-but-stable scan order.
    let reversed = repo
        .find_resource_snapshots_by_ids(
            &account_handle.did,
            &[other_account.id, first.id, missing_id, second.id],
        )
        .await
        .unwrap();

    let reversed_ids = reversed
        .into_iter()
        .map(|snapshot| snapshot.id)
        .collect::<Vec<_>>();
    assert_eq!(reversed_ids, vec![first.id, second.id]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_resource_handles_by_ids(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");
    let other_account_handle = odf::AccountHandle::new_test("other-account");

    let found = repo
        .find_resource_handles_by_ids(&account_handle.did, &[])
        .await
        .unwrap();
    assert!(found.is_empty());

    let mut first = make_test_snapshot(&account_handle, &TEST_KIND, "first");
    first.id = repo.new_resource_id().await.unwrap();
    // Different schema on purpose: unlike `search_resource_handles`, this
    // method has no schema filter, so a match here must still come back.
    let mut second = make_test_snapshot(&account_handle, &OTHER_KIND, "second");
    second.id = repo.new_resource_id().await.unwrap();
    let mut other_account = make_test_snapshot(&other_account_handle, &TEST_KIND, "other-account");
    other_account.id = repo.new_resource_id().await.unwrap();
    let mut to_delete = make_test_snapshot(&account_handle, &TEST_KIND, "to-delete");
    to_delete.id = repo.new_resource_id().await.unwrap();
    let missing_id = repo.new_resource_id().await.unwrap();

    repo.create_resource(&first).await.unwrap();
    repo.create_resource(&second).await.unwrap();
    repo.create_resource(&other_account).await.unwrap();
    repo.create_resource(&to_delete).await.unwrap();

    let deleted = ResourceSnapshot {
        headers: ResourceHeaders {
            deleted_at: Some(Utc::now()),
            ..to_delete.headers.clone()
        },
        ..to_delete.clone()
    };
    repo.update_resource(&deleted, None).await.unwrap();

    let found = repo
        .find_resource_handles_by_ids(&account_handle.did, &[first.id])
        .await
        .unwrap();
    let found_ids = found.into_iter().map(|row| row.id).collect::<Vec<_>>();
    assert_eq!(found_ids, vec![*first.id.as_ref()]);

    let found = repo
        .find_resource_handles_by_ids(
            &account_handle.did,
            &[
                first.id,
                second.id,
                missing_id,
                other_account.id,
                to_delete.id,
            ],
        )
        .await
        .unwrap();
    // Request order is the contract: the survivors keep the relative order they
    // were asked in, with misses simply absent.
    let found_ids = found.into_iter().map(|row| row.id).collect::<Vec<_>>();
    assert_eq!(found_ids, vec![*first.id.as_ref(), *second.id.as_ref()]);

    // Reversing the request must reverse the result, which an arbitrary-but-
    // stable scan order would not do.
    let reversed = repo
        .find_resource_handles_by_ids(
            &account_handle.did,
            &[
                to_delete.id,
                other_account.id,
                missing_id,
                second.id,
                first.id,
            ],
        )
        .await
        .unwrap();
    let reversed_ids = reversed.into_iter().map(|row| row.id).collect::<Vec<_>>();
    assert_eq!(reversed_ids, vec![*second.id.as_ref(), *first.id.as_ref()]);
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

    // Request order is the contract. `third` belongs to another account: this
    // lookup is by schema, not by account, so it is expected in the result.
    let found_ids = found
        .into_iter()
        .map(|snapshot| snapshot.id)
        .collect::<Vec<_>>();
    assert_eq!(found_ids, vec![third.id, first.id]);

    // Reversing the request must reverse the result.
    let reversed = repo
        .find_resource_snapshots_by_schema_and_ids(
            &TEST_KIND,
            &[first.id, third.id, missing_id, second.id],
        )
        .await
        .unwrap();
    let reversed_ids = reversed
        .into_iter()
        .map(|snapshot| snapshot.id)
        .collect::<Vec<_>>();
    assert_eq!(reversed_ids, vec![first.id, third.id]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_resource_handles(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    seed_search_resource_handles(repo.as_ref(), &account_handle).await;

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::NamePattern("app-%".to_string())),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["app-beta", "app-alpha"]);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::NamePattern("APP-%".to_string())),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["app-beta", "app-alpha"]);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::ExactNames(vec![
                    "app-alpha".parse().unwrap(),
                    "db-alpha".parse().unwrap(),
                ])),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["app-alpha", "db-alpha"]);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::ExactNames(vec![
                    "App-Alpha".parse().unwrap(),
                    "DB-ALPHA".parse().unwrap(),
                ])),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["app-alpha", "db-alpha"]);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::types(vec![
                ResourceTypeQuery {
                    schema: TEST_KIND.clone(),
                    query: Some(ResourceQuery::NamePattern("app-%".to_string())),
                    account_id: None,
                    label_pairs: vec![],
                },
                ResourceTypeQuery {
                    schema: OTHER_KIND.clone(),
                    query: Some(ResourceQuery::NamePattern("app-%".to_string())),
                    account_id: None,
                    label_pairs: vec![],
                },
            ]),
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

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::NamePattern("%".to_string())),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::NamePattern("app-other-%".to_string())),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    assert!(rows.is_empty());

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::NamePattern(String::new())),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_resource_handles_exact_ids(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");
    let ids = seed_search_resource_handles(repo.as_ref(), &account_handle).await;

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::ExactIds(vec![ids.app_alpha])),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["app-alpha"]);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::ExactIds(vec![ids.app_alpha, ids.db_alpha])),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["app-alpha", "db-alpha"]);

    let missing_id = repo.new_resource_id().await.unwrap();
    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::ExactIds(vec![ids.app_alpha, missing_id])),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["app-alpha"]);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                OTHER_KIND.clone(),
                Some(ResourceQuery::ExactIds(vec![ids.app_alpha])),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    assert!(rows.is_empty());

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::ExactIds(vec![ids.other_account])),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    assert!(rows.is_empty());

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::types(vec![
                ResourceTypeQuery {
                    schema: TEST_KIND.clone(),
                    query: Some(ResourceQuery::ExactIds(vec![ids.app_alpha, ids.app_gamma])),
                    account_id: None,
                    label_pairs: vec![],
                },
                ResourceTypeQuery {
                    schema: OTHER_KIND.clone(),
                    query: Some(ResourceQuery::ExactIds(vec![ids.app_alpha, ids.app_gamma])),
                    account_id: None,
                    label_pairs: vec![],
                },
            ]),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["app-alpha", "app-gamma"]);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(TEST_KIND.clone(), Some(ResourceQuery::ExactIds(vec![]))),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A scope where only *some* rows are vacuous.
///
/// `ResourceScope::is_vacuous` folds with `.all()`, so this scope is not
/// vacuous and no short-circuit fires — the empty list reaches the query
/// builder. The single-row cases above never exercise that path, because one
/// empty row makes the whole scope vacuous and returns before any SQL is built.
///
/// Both SQL backends happen to render the empty list correctly today (`SQLite`
/// accepts `IN ()` as an extension, and Postgres' `STRING_TO_ARRAY('', ',')`
/// yields an empty array, so both match nothing). That is load-bearing but
/// unobvious, and neither is required by the SQL standard — this test pins it
/// so a future rewrite of the predicate builder cannot regress it silently.
///
/// A vacuous row matches nothing, so the result is exactly what the non-vacuous
/// row alone would return.
pub async fn test_search_resource_handles_partially_vacuous_scope(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");
    seed_search_resource_handles(repo.as_ref(), &account_handle).await;

    for empty_query in [
        ResourceQuery::ExactIds(vec![]),
        ResourceQuery::ExactNames(vec![]),
    ] {
        let scope = ResourceScope::types(vec![
            ResourceTypeQuery {
                schema: TEST_KIND.clone(),
                query: Some(empty_query.clone()),
                account_id: None,
                label_pairs: vec![],
            },
            ResourceTypeQuery {
                schema: OTHER_KIND.clone(),
                query: Some(ResourceQuery::NamePattern("app-%".to_string())),
                account_id: None,
                label_pairs: vec![],
            },
        ]);

        let rows = repo
            .search_resource_handles(
                &account_handle.did,
                &scope,
                PaginationOpts::from_max_results(10),
            )
            .await
            .unwrap();
        let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
        names.sort();
        assert_eq!(
            names,
            vec!["app-delta", "app-gamma"],
            "the vacuous row must contribute nothing, not widen or break the query"
        );

        // The count shares the scope predicate, so it must agree.
        let count = repo
            .count_search_resource_handles(&account_handle.did, &scope)
            .await
            .unwrap();
        assert_eq!(count, 2);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_resource_handles_any_type(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");
    let ids = seed_search_resource_handles(repo.as_ref(), &account_handle).await;

    // `AnyType` finds a match regardless of which seeded schema it carries,
    // with no schema filter narrowing the search.
    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::AnyType(
                Some(ResourceQuery::ExactIds(vec![ids.app_alpha, ids.app_gamma])),
                vec![],
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        vec!["app-alpha", "app-gamma"],
        "AnyType must match ids across differing schemas"
    );

    // Account scoping still applies under `AnyType` — it only removes the
    // schema filter, not the account filter.
    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::AnyType(
                Some(ResourceQuery::ExactIds(vec![ids.other_account])),
                vec![],
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    assert!(rows.is_empty(), "AnyType must not cross account boundaries");

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::AnyType(
                Some(ResourceQuery::NamePattern("app-%".to_string())),
                vec![],
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        vec!["app-alpha", "app-beta", "app-delta", "app-gamma"],
        "AnyType name-pattern search must span every schema"
    );

    let count = repo
        .count_search_resource_handles(
            &account_handle.did,
            &ResourceScope::AnyType(
                Some(ResourceQuery::ExactIds(vec![ids.app_alpha, ids.app_gamma])),
                vec![],
            ),
        )
        .await
        .unwrap();
    assert_eq!(count, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_count_search_resource_handles_exact_ids(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");
    let ids = seed_search_resource_handles(repo.as_ref(), &account_handle).await;

    let count = repo
        .count_search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::ExactIds(vec![ids.app_alpha, ids.db_alpha])),
            ),
        )
        .await
        .unwrap();
    assert_eq!(count, 2);

    let missing_id = repo.new_resource_id().await.unwrap();
    let count = repo
        .count_search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::ExactIds(vec![ids.app_alpha, missing_id])),
            ),
        )
        .await
        .unwrap();
    assert_eq!(count, 1);

    let count = repo
        .count_search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(TEST_KIND.clone(), Some(ResourceQuery::ExactIds(vec![]))),
        )
        .await
        .unwrap();
    assert_eq!(count, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pins escaping behavior for literal `_` inside a `NamePattern`.
pub async fn test_search_resource_handles_pattern_special_characters(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    // "a-b" and "a_b" differ only in whether the middle character is a
    // literal underscore or a hyphen — an unescaped `_` in a LIKE pattern
    // would match both.
    for name in ["a-b", "a_b", "a-b-plain"] {
        let mut snapshot = make_test_snapshot(&account_handle, &TEST_KIND, name);
        snapshot.id = repo.new_resource_id().await.unwrap();
        repo.create_resource(&snapshot).await.unwrap();
    }

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::NamePattern("a_b".to_string())),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["a_b"]);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::NamePattern("a-b-%".to_string())),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["a-b-plain"]);

    // A `%` is deliberately NOT tested as a literal here: `NamePattern` is a
    // `LIKE` pattern by ODF definition, and the repository escapes it with
    // `sql_like_escape_pattern`, which passes `%` through as a wildcard. A
    // caller cannot pre-escape a literal `%` into this field — the two escapes
    // would compound into `\\%` ("a backslash, then a wildcard").
    //
    // That is sound because a `ResourceName` is a hostname, so `%` is
    // unrepresentable in a stored name in the first place. Both halves are
    // already pinned where they belong: `test_sql_like_escape_literal` in
    // `database-common`, and
    // `resource_names_cannot_contain_like_metacharacters` in the CLI
    // resolution-service tests.
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_count_search_resource_handles(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    seed_search_resource_handles(repo.as_ref(), &account_handle).await;

    let count = repo
        .count_search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::NamePattern("app-%".to_string())),
            ),
        )
        .await
        .unwrap();
    assert_eq!(count, 2);

    let count = repo
        .count_search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::ExactNames(vec![
                    "App-Alpha".parse().unwrap(),
                    "DB-ALPHA".parse().unwrap(),
                ])),
            ),
        )
        .await
        .unwrap();
    assert_eq!(count, 2);

    let count = repo
        .count_search_resource_handles(
            &account_handle.did,
            &ResourceScope::types(vec![
                ResourceTypeQuery {
                    schema: TEST_KIND.clone(),
                    query: Some(ResourceQuery::NamePattern("app-%".to_string())),
                    account_id: None,
                    label_pairs: vec![],
                },
                ResourceTypeQuery {
                    schema: OTHER_KIND.clone(),
                    query: Some(ResourceQuery::NamePattern("app-%".to_string())),
                    account_id: None,
                    label_pairs: vec![],
                },
            ]),
        )
        .await
        .unwrap();
    assert_eq!(count, 4);

    let count = repo
        .count_search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::NamePattern("app-other-%".to_string())),
            ),
        )
        .await
        .unwrap();
    assert_eq!(count, 0);

    let count = repo
        .count_search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(TEST_KIND.clone(), Some(ResourceQuery::ExactNames(vec![]))),
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

    let found = repo
        .find_resource_id_by_name(
            &account_handle.did,
            &TEST_KIND,
            &"My-Resource".parse().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(found, Some(id));

    let found = repo
        .find_resource_id_by_name(
            &account_handle.did,
            &TEST_KIND,
            &"MY-RESOURCE".parse().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(found, Some(id));

    let rows = repo
        .resolve_resource_ids_by_names(
            &account_handle.did,
            &TEST_KIND,
            &[
                "My-Resource".parse().unwrap(),
                "OTHER-RESOURCE".parse().unwrap(),
            ],
        )
        .await
        .unwrap();
    // Request order is the contract, and it holds even though the requested
    // spelling differs in case from the stored one.
    let names = rows
        .into_iter()
        .map(|(name, _)| name.to_string())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["my-resource", "other-resource"]);

    let rows = repo
        .resolve_resource_ids_by_names(
            &account_handle.did,
            &TEST_KIND,
            &[
                "OTHER-RESOURCE".parse().unwrap(),
                "My-Resource".parse().unwrap(),
            ],
        )
        .await
        .unwrap();
    let names = rows
        .into_iter()
        .map(|(name, _)| name.to_string())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["other-resource", "my-resource"]);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::ExactNames(vec![
                    "MY-RESOURCE".parse().unwrap(),
                ])),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|r| r.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["my-resource"]);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &ResourceScope::one_type(
                TEST_KIND.clone(),
                Some(ResourceQuery::NamePattern("MY-%".to_string())),
            ),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|r| r.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["my-resource"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Seeds resources and their label projection rows.
async fn seed_label_filtered_resources(
    repo: &dyn ResourceRepository,
    projection_repo: &dyn ResourceLabelProjectionRepository,
    account_handle: &odf::AccountHandle,
) {
    for (name, labels) in [
        ("prod-data", vec![("environment", "prod"), ("team", "data")]),
        (
            "prod-infra",
            vec![("environment", "prod"), ("team", "infra")],
        ),
        (
            "staging-data",
            vec![("environment", "staging"), ("team", "data")],
        ),
    ] {
        let mut snapshot = make_test_snapshot(account_handle, &TEST_KIND, name);
        snapshot.id = repo.new_resource_id().await.unwrap();
        snapshot.headers.labels.entries = labels
            .iter()
            .map(|(k, v)| ((*k).parse().unwrap(), serde_json::json!(v)))
            .collect();

        repo.create_resource(&snapshot).await.unwrap();

        let entries = labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect::<Vec<_>>();
        projection_repo
            .replace_entries(&snapshot.id, &entries)
            .await
            .unwrap();
    }
}

/// Seeds one labelled resource of an arbitrary type.
async fn seed_labelled_resource(
    repo: &dyn ResourceRepository,
    projection_repo: &dyn ResourceLabelProjectionRepository,
    account_handle: &odf::AccountHandle,
    schema: &TypeUri,
    name: &str,
    labels: &[(&str, &str)],
) {
    let mut snapshot = make_test_snapshot(account_handle, schema, name);
    snapshot.id = repo.new_resource_id().await.unwrap();
    snapshot.headers.labels.entries = labels
        .iter()
        .map(|(k, v)| ((*k).parse().unwrap(), serde_json::json!(v)))
        .collect();

    repo.create_resource(&snapshot).await.unwrap();

    let entries = labels
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect::<Vec<_>>();
    projection_repo
        .replace_entries(&snapshot.id, &entries)
        .await
        .unwrap();
}

/// Per-row label pairs: two scope rows filtering by **different** labels.
///
/// The capability the per-row `label_pairs` exist for. A backend that hoisted
/// the pairs out of the row — evaluating one filter for the whole scope, as the
/// call-wide predicate used to — returns the wrong set here while still passing
/// every single-row label test.
///
/// Like `test_search_resource_handles_per_row_account`, this is the only safety
/// net for `SQLite`, whose scope predicate is built with a runtime
/// `QueryBuilder` and so is not compile-time checked.
pub async fn test_search_resource_handles_per_row_labels(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let projection_repo = catalog
        .get_one::<dyn ResourceLabelProjectionRepository>()
        .unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    for (schema, name, labels) in [
        (&*KIND_A, "a-prod", vec![("environment", "prod")]),
        (&*KIND_A, "a-staging", vec![("environment", "staging")]),
        (&*KIND_B, "b-prod", vec![("environment", "prod")]),
        (&*KIND_B, "b-staging", vec![("environment", "staging")]),
    ] {
        seed_labelled_resource(
            repo.as_ref(),
            projection_repo.as_ref(),
            &account_handle,
            schema,
            name,
            &labels,
        )
        .await;
    }

    // KindA filtered to prod, KindB filtered to staging — in one call.
    let scope = ResourceScope::Types(vec![
        ResourceTypeQuery {
            schema: KIND_A.clone(),
            query: None,
            account_id: None,
            label_pairs: label_pairs_of(&[("environment", "prod")]),
        },
        ResourceTypeQuery {
            schema: KIND_B.clone(),
            query: None,
            account_id: None,
            label_pairs: label_pairs_of(&[("environment", "staging")]),
        },
    ]);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &scope,
            PaginationOpts::from_max_results(100),
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        vec!["a-prod", "b-staging"],
        "each row must apply only its own label pairs"
    );

    // The count must agree with the rows, or pagination totals drift.
    let count = repo
        .count_search_resource_handles(&account_handle.did, &scope)
        .await
        .unwrap();
    assert_eq!(count, 2);

    // A filtered row beside an unfiltered one: the filter must not leak.
    let mixed_scope = ResourceScope::Types(vec![
        ResourceTypeQuery {
            schema: KIND_A.clone(),
            query: None,
            account_id: None,
            label_pairs: label_pairs_of(&[("environment", "prod")]),
        },
        ResourceTypeQuery {
            schema: KIND_B.clone(),
            query: None,
            account_id: None,
            label_pairs: Vec::new(),
        },
    ]);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &mixed_scope,
            PaginationOpts::from_max_results(100),
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        vec!["a-prod", "b-prod", "b-staging"],
        "an unfiltered row must not inherit its neighbour's label pairs"
    );

    // `list_resource_snapshots` shares the scope shape, so it must agree.
    let snapshots = repo
        .list_resource_snapshots(
            &account_handle.did,
            &scope,
            PaginationOpts::from_max_results(100),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut names = snapshots
        .into_iter()
        .map(|s| s.headers.name.to_string())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["a-prod", "b-staging"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `AnyType` carries label pairs for the whole scope, spanning every type.
pub async fn test_search_resource_handles_any_type_labels(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let projection_repo = catalog
        .get_one::<dyn ResourceLabelProjectionRepository>()
        .unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    for (schema, name, labels) in [
        (&*KIND_A, "any-a-prod", vec![("environment", "prod")]),
        (&*KIND_A, "any-a-staging", vec![("environment", "staging")]),
        (&*KIND_B, "any-b-prod", vec![("environment", "prod")]),
    ] {
        seed_labelled_resource(
            repo.as_ref(),
            projection_repo.as_ref(),
            &account_handle,
            schema,
            name,
            &labels,
        )
        .await;
    }

    let scope = ResourceScope::AnyType(None, label_pairs_of(&[("environment", "prod")]));

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &scope,
            PaginationOpts::from_max_results(100),
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        vec!["any-a-prod", "any-b-prod"],
        "an any-type scope must apply its labels across every type"
    );

    let count = repo
        .count_search_resource_handles(&account_handle.did, &scope)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The resolved `(key, value)` pairs a scope row carries.
fn label_pairs_of(pairs: &[(&str, &str)]) -> Vec<ResourceLabelPair> {
    pairs
        .iter()
        .map(|(key, value)| (TypeRef::Name((*key).parse().unwrap()), (*value).to_string()))
        .collect()
}

/// One type, unnarrowed by name, filtered by `label_pairs`.
fn one_type_scope_with_labels(label_pairs: Vec<ResourceLabelPair>) -> ResourceScope {
    ResourceScope::Types(vec![ResourceTypeQuery {
        schema: TEST_KIND.clone(),
        query: None,
        account_id: None,
        label_pairs,
    }])
}

async fn filtered_list_names(
    repo: &dyn ResourceRepository,
    account_handle: &odf::AccountHandle,
    label_pairs: Vec<ResourceLabelPair>,
) -> Vec<String> {
    let snapshots = repo
        .list_resource_snapshots(
            &account_handle.did,
            &one_type_scope_with_labels(label_pairs),
            PaginationOpts::from_max_results(10),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let mut names = snapshots
        .into_iter()
        .map(|s| s.headers.name.to_string())
        .collect::<Vec<_>>();
    names.sort();
    names
}

pub async fn test_list_resource_snapshots_label_filtering(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let projection_repo = catalog
        .get_one::<dyn ResourceLabelProjectionRepository>()
        .unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");
    seed_label_filtered_resources(repo.as_ref(), projection_repo.as_ref(), &account_handle).await;

    let names = filtered_list_names(repo.as_ref(), &account_handle, Vec::new()).await;
    assert_eq!(names, vec!["prod-data", "prod-infra", "staging-data"]);

    let names = filtered_list_names(
        repo.as_ref(),
        &account_handle,
        label_pairs_of(&[("environment", "prod")]),
    )
    .await;
    assert_eq!(names, vec!["prod-data", "prod-infra"]);

    let names = filtered_list_names(
        repo.as_ref(),
        &account_handle,
        label_pairs_of(&[("environment", "prod"), ("team", "data")]),
    )
    .await;
    assert_eq!(names, vec!["prod-data"]);

    let names = filtered_list_names(
        repo.as_ref(),
        &account_handle,
        label_pairs_of(&[("environment", "nope")]),
    )
    .await;
    assert!(names.is_empty(), "expected no matches, got {names:?}");

    let names = filtered_list_names(
        repo.as_ref(),
        &account_handle,
        label_pairs_of(&[("no-such-label", "x")]),
    )
    .await;
    assert!(names.is_empty(), "expected no matches, got {names:?}");

    let names = filtered_list_names(
        repo.as_ref(),
        &account_handle,
        label_pairs_of(&[("environment", "PROD")]),
    )
    .await;
    assert!(names.is_empty(), "expected no matches, got {names:?}");
}

pub async fn test_search_resource_handles_label_filtering(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let projection_repo = catalog
        .get_one::<dyn ResourceLabelProjectionRepository>()
        .unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");
    seed_label_filtered_resources(repo.as_ref(), projection_repo.as_ref(), &account_handle).await;

    let scope_with_pattern = |pattern: &str| {
        ResourceScope::Types(vec![ResourceTypeQuery {
            schema: TEST_KIND.clone(),
            query: Some(ResourceQuery::NamePattern(pattern.to_string())),
            account_id: None,
            label_pairs: label_pairs_of(&[("environment", "prod")]),
        }])
    };

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &scope_with_pattern("%"),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["prod-data", "prod-infra"]);

    let count = repo
        .count_search_resource_handles(&account_handle.did, &scope_with_pattern("%"))
        .await
        .unwrap();
    assert_eq!(count, 2);

    let rows = repo
        .search_resource_handles(
            &account_handle.did,
            &scope_with_pattern("%-infra"),
            PaginationOpts::from_max_results(10),
        )
        .await
        .unwrap();
    let names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
    assert_eq!(names, vec!["prod-infra"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SeededSearchHandleIds {
    app_alpha: ResourceID,
    db_alpha: ResourceID,
    app_gamma: ResourceID,
    other_account: ResourceID,
}

/// Per-row account: a scope row may name its own account, and rows that do not
/// fall back to the call-level scalar.
///
/// This is the *only* safety net for the `SQLite` backend, whose scope
/// predicate is built with a runtime `QueryBuilder` and so is not compile-time
/// checked.
pub async fn test_search_resource_handles_per_row_account(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");
    let other_account_handle = odf::AccountHandle::new_test("other-account");
    seed_search_resource_handles(repo.as_ref(), &account_handle).await;

    let search = async |scope: ResourceScope| {
        let rows = repo
            .search_resource_handles(
                &account_handle.did,
                &scope,
                PaginationOpts::from_max_results(100),
            )
            .await
            .unwrap();
        let mut names = rows.into_iter().map(|row| row.name).collect::<Vec<_>>();
        names.sort();
        names
    };

    // A row naming another account reads that account, not the caller's — the
    // whole point of the stage.
    assert_eq!(
        search(ResourceScope::Types(vec![ResourceTypeQuery {
            schema: TEST_KIND.clone(),
            query: None,
            account_id: Some(other_account_handle.did.clone()),
            label_pairs: vec![],
        }]))
        .await,
        vec!["app-other-account"]
    );

    // A row with no account falls back to the call-level one, unchanged from
    // before this stage.
    assert_eq!(
        search(ResourceScope::Types(vec![ResourceTypeQuery {
            schema: TEST_KIND.clone(),
            query: None,
            account_id: None,
            label_pairs: vec![],
        }]))
        .await,
        vec!["app-alpha", "app-beta", "db-alpha"]
    );

    // Both in one call: the two rows are a logical OR spanning two accounts,
    // which is what the ODF-shaped API exists to express.
    assert_eq!(
        search(ResourceScope::Types(vec![
            ResourceTypeQuery {
                schema: TEST_KIND.clone(),
                query: Some(ResourceQuery::NamePattern("app-a%".to_string())),
                account_id: None,
                label_pairs: vec![],
            },
            ResourceTypeQuery {
                schema: TEST_KIND.clone(),
                query: None,
                account_id: Some(other_account_handle.did.clone()),
                label_pairs: vec![],
            },
        ]))
        .await,
        vec!["app-alpha", "app-other-account"]
    );

    // The per-row account narrows as well as redirects: the caller's own
    // resources are excluded from a row naming someone else.
    assert_eq!(
        search(ResourceScope::Types(vec![ResourceTypeQuery {
            schema: TEST_KIND.clone(),
            query: Some(ResourceQuery::NamePattern("app-a%".to_string())),
            account_id: Some(other_account_handle.did.clone()),
            label_pairs: vec![],
        }]))
        .await,
        Vec::<String>::new(),
        "`app-alpha` belongs to the caller, so a row naming another account must not see it"
    );

    // `count_search_resource_handles` shares the scope predicate with
    // `search_resource_handles`, so it must agree row-for-row.
    let count = repo
        .count_search_resource_handles(
            &account_handle.did,
            &ResourceScope::Types(vec![ResourceTypeQuery {
                schema: TEST_KIND.clone(),
                query: None,
                account_id: Some(other_account_handle.did.clone()),
                label_pairs: vec![],
            }]),
        )
        .await
        .unwrap();
    assert_eq!(count, 1, "count must honour the per-row account too");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn seed_search_resource_handles(
    repo: &dyn ResourceRepository,
    account_handle: &odf::AccountHandle,
) -> SeededSearchHandleIds {
    let other_account_handle = odf::AccountHandle::new_test("other-account");

    let mut ids = std::collections::HashMap::new();
    // Results are ordered `updated_at DESC, resource_id DESC`. Seeding with a
    // bare `Utc::now()` per row lets two rows share a timestamp, which drops
    // the tiebreak onto a random v4 UUID and makes order assertions flip. Stagger
    // the timestamps so `updated_at` alone decides, newest last in this list.
    let base = Utc::now() - chrono::Duration::seconds(60);
    for (index, (kind, name, account)) in [
        (&*TEST_KIND, "app-alpha", account_handle.clone()),
        (&*TEST_KIND, "app-beta", account_handle.clone()),
        (&*TEST_KIND, "db-alpha", account_handle.clone()),
        (&*OTHER_KIND, "app-gamma", account_handle.clone()),
        (&*OTHER_KIND, "app-delta", account_handle.clone()),
        (&*TEST_KIND, "app-other-account", other_account_handle),
    ]
    .into_iter()
    .enumerate()
    {
        let mut snapshot = make_test_snapshot(&account, kind, name);
        snapshot.id = repo.new_resource_id().await.unwrap();
        snapshot.headers = ResourceHeaders::simple(
            base + chrono::Duration::seconds(i64::try_from(index).unwrap()),
            snapshot.id,
            account.clone(),
            name,
        );
        ids.insert(name, snapshot.id);
        repo.create_resource(&snapshot).await.unwrap();
    }

    SeededSearchHandleIds {
        app_alpha: ids["app-alpha"],
        db_alpha: ids["db-alpha"],
        app_gamma: ids["app-gamma"],
        other_account: ids["app-other-account"],
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
    let mut annotations = snapshot.headers.annotations.entries.clone();
    annotations.insert(
        description_annotation_type_ref(),
        serde_json::json!("Updated description"),
    );
    let updated = ResourceSnapshot {
        headers: ResourceHeaders {
            annotations: kamu_resources::ResourceAnnotations {
                entries: annotations,
            },
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
        get_description(&found.headers.annotations.entries),
        Some("Updated description")
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

    let mut first_annotations = first.headers.annotations.entries.clone();
    first_annotations.insert(
        description_annotation_type_ref(),
        serde_json::json!("Updated first"),
    );
    let mut second_annotations = second.headers.annotations.entries.clone();
    second_annotations.insert(
        description_annotation_type_ref(),
        serde_json::json!("Updated second"),
    );

    let updated_first = ResourceSnapshot {
        headers: ResourceHeaders {
            annotations: kamu_resources::ResourceAnnotations {
                entries: first_annotations,
            },
            generation: 1,
            updated_at: Utc::now(),
            ..first.headers.clone()
        },
        last_event_id: Some(first_event_id),
        ..first.clone()
    };
    let updated_second = ResourceSnapshot {
        headers: ResourceHeaders {
            annotations: kamu_resources::ResourceAnnotations {
                entries: second_annotations,
            },
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
        get_description(&found_first.headers.annotations.entries),
        get_description(&updated_first.headers.annotations.entries)
    );
    assert_eq!(found_first.headers.generation, 1);
    assert_eq!(found_first.last_event_id, Some(first_event_id));

    assert_eq!(
        get_description(&found_second.headers.annotations.entries),
        get_description(&updated_second.headers.annotations.entries)
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
            account_handle.did.clone(),
            &TEST_KIND,
            PaginationOpts::from_max_results(3),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(first_page.len(), 3);

    let second_page: Vec<_> = repo
        .list_resource_ids(
            account_handle.did.clone(),
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

pub async fn test_list_resource_snapshots_by_scope(catalog: &Catalog) {
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
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::one_type(KIND_A.clone(), None),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(kind_a.len(), 3);
    assert!(kind_a.iter().all(|s| s.schema == *KIND_A));

    let kind_b: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::one_type(KIND_B.clone(), None),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(kind_b.len(), 2);
    assert!(kind_b.iter().all(|s| s.schema == *KIND_B));

    let kind_c: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::one_type(KIND_C.clone(), None),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert!(kind_c.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_resource_snapshots_with_queries(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account_handle = odf::AccountHandle::new_test("test-account");

    let mut app_a = make_test_snapshot(&account_handle, &KIND_A, "app-alpha");
    app_a.id = repo.new_resource_id().await.unwrap();
    repo.create_resource(&app_a).await.unwrap();

    let mut db_a = make_test_snapshot(&account_handle, &KIND_A, "db-alpha");
    db_a.id = repo.new_resource_id().await.unwrap();
    repo.create_resource(&db_a).await.unwrap();

    let mut app_b = make_test_snapshot(&account_handle, &KIND_B, "app-beta");
    app_b.id = repo.new_resource_id().await.unwrap();
    repo.create_resource(&app_b).await.unwrap();

    let names = |snapshots: Vec<ResourceSnapshot>| {
        let mut names = snapshots
            .into_iter()
            .map(|snapshot| snapshot.headers.name.to_string())
            .collect::<Vec<_>>();
        names.sort();
        names
    };

    // A name pattern narrows within one type.
    let matched: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::one_type(
                KIND_A.clone(),
                Some(ResourceQuery::NamePattern("app-%".to_string())),
            ),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(matched), vec!["app-alpha"]);

    // An exact ID is matched as an ID, never as a `LIKE` pattern.
    let by_id: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::one_type(KIND_A.clone(), Some(ResourceQuery::ExactIds(vec![db_a.id]))),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(by_id), vec!["db-alpha"]);

    // An ID belonging to another type yields nothing rather than erroring.
    let wrong_type: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::one_type(KIND_B.clone(), Some(ResourceQuery::ExactIds(vec![db_a.id]))),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert!(wrong_type.is_empty());

    // Each type carries its own query, so one call can span several.
    let multi_type: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::types(vec![
                ResourceTypeQuery {
                    schema: KIND_A.clone(),
                    query: Some(ResourceQuery::NamePattern("app-%".to_string())),
                    account_id: None,
                    label_pairs: vec![],
                },
                ResourceTypeQuery {
                    schema: KIND_B.clone(),
                    query: Some(ResourceQuery::NamePattern("app-%".to_string())),
                    account_id: None,
                    label_pairs: vec![],
                },
            ]),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(multi_type), vec!["app-alpha", "app-beta"]);

    // A per-type query applies only to its own type.
    let asymmetric: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::types(vec![
                ResourceTypeQuery {
                    schema: KIND_A.clone(),
                    query: Some(ResourceQuery::NamePattern("db-%".to_string())),
                    account_id: None,
                    label_pairs: vec![],
                },
                ResourceTypeQuery {
                    schema: KIND_B.clone(),
                    query: None,
                    account_id: None,
                    label_pairs: vec![],
                },
            ]),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(asymmetric), vec!["app-beta", "db-alpha"]);

    // A query spanning every type.
    let any_type: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::any_type_with_query(ResourceQuery::NamePattern("app-%".to_string())),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(any_type), vec!["app-alpha", "app-beta"]);

    // Exact names across every type — the `AnyType` + `ExactNames` pairing.
    let any_type_names: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::any_type_with_query(ResourceQuery::ExactNames(vec![
                "app-alpha".parse().unwrap(),
                "app-beta".parse().unwrap(),
            ])),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(any_type_names), vec!["app-alpha", "app-beta"]);

    // Per-type exact names in a multi-type scope.
    let multi_type_names: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::types(vec![
                ResourceTypeQuery {
                    schema: KIND_A.clone(),
                    query: Some(ResourceQuery::ExactNames(vec!["db-alpha".parse().unwrap()])),
                    account_id: None,
                    label_pairs: vec![],
                },
                ResourceTypeQuery {
                    schema: KIND_B.clone(),
                    query: Some(ResourceQuery::ExactNames(vec!["app-beta".parse().unwrap()])),
                    account_id: None,
                    label_pairs: vec![],
                },
            ]),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(multi_type_names.len(), 2);

    // Mixed query modes across rows — the pairing must bind each type to *its
    // own* query, so a cross-wired implementation would return the wrong rows
    // here even though every individual mode works in isolation.
    let mixed: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::types(vec![
                ResourceTypeQuery {
                    schema: KIND_A.clone(),
                    query: Some(ResourceQuery::ExactIds(vec![db_a.id])),
                    account_id: None,
                    label_pairs: vec![],
                },
                ResourceTypeQuery {
                    schema: KIND_B.clone(),
                    query: Some(ResourceQuery::NamePattern("app-%".to_string())),
                    account_id: None,
                    label_pairs: vec![],
                },
            ]),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names(mixed), vec!["app-beta", "db-alpha"]);

    // Swapping the two queries must change the result, proving the binding is
    // positional rather than "any query matches any type".
    let mixed_swapped: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::types(vec![
                ResourceTypeQuery {
                    schema: KIND_A.clone(),
                    query: Some(ResourceQuery::NamePattern("app-%".to_string())),
                    account_id: None,
                    label_pairs: vec![],
                },
                ResourceTypeQuery {
                    schema: KIND_B.clone(),
                    query: Some(ResourceQuery::ExactIds(vec![db_a.id])),
                    account_id: None,
                    label_pairs: vec![],
                },
            ]),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        names(mixed_swapped),
        vec!["app-alpha"],
        "`db_a.id` belongs to KIND_A, so scoping it to KIND_B must match nothing"
    );

    // An empty type list can never match, and must not degrade into "all".
    let empty_scope: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::Types(Vec::new()),
            PaginationOpts::from_max_results(100),
        )
        .try_collect()
        .await
        .unwrap();
    assert!(empty_scope.is_empty());

    // Pagination must apply after filtering, not before.
    let first_page: Vec<_> = repo
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::any_type_with_query(ResourceQuery::NamePattern("app-%".to_string())),
            PaginationOpts {
                limit: 1,
                offset: 0,
            },
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(first_page.len(), 1);
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
        .list_resource_snapshots(
            &account_handle.did,
            &ResourceScope::default(),
            PaginationOpts::from_max_results(100),
        )
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
        .count_resources(account_handle.did.clone(), &TEST_KIND)
        .await
        .unwrap();
    assert_eq!(3, count);

    let count_other = repo
        .count_resources(account_handle.did, &OTHER_KIND)
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

    let summary = repo.summarize_resources(account_handle.did).await.unwrap();

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
            &account_handle.did,
            &TEST_KIND,
            &"to-delete".parse().unwrap(),
        )
        .await
        .unwrap();
    assert!(by_name.is_none());

    let count = repo
        .count_resources(account_handle.did.clone(), &TEST_KIND)
        .await
        .unwrap();
    assert_eq!(0, count);

    let ids: Vec<_> = repo
        .list_resource_ids(
            account_handle.did,
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
        id: handle.did.clone(),
        resource_id: handle.id,
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
    assert_eq!(before_rename.headers.account.did, original_account.id);
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
    assert_eq!(after_rename.headers.account.did, original_handle.did);
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

    assert_eq!(found.headers.account.did, account_handle.did);
    assert_eq!(
        found.headers.account.name,
        kamu_resources::deleted_account_name_sentinel(),
        "reads for a resource whose account is gone must fall back to the sentinel name, not fail"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
