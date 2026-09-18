// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use chrono::Utc;
use kamu_resources::{
    FindOwnedResourceError,
    FindOwnedSnapshotsOutcome,
    ResourceHeaders,
    ResourceHeadersExt,
    ResourceID,
    ResourceSchemaProvider,
    ResourceSnapshot,
    TypeUri,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

use crate::tests::utils::TestResource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static OTHER_SCHEMA: std::sync::LazyLock<kamu_resources::TypeUri> =
    std::sync::LazyLock::new(|| {
        kamu_resources::TypeUri::new_unchecked(
            "https://test.kamu.dev/schemas/test/v1/OtherResource",
        )
    });

static LEGACY_SCHEMA: std::sync::LazyLock<kamu_resources::TypeUri> =
    std::sync::LazyLock::new(|| {
        kamu_resources::TypeUri::new_unchecked("https://test.kamu.dev/schemas/test/v0/TestResource")
    });

static NEWER_SCHEMA: std::sync::LazyLock<kamu_resources::TypeUri> =
    std::sync::LazyLock::new(|| {
        kamu_resources::TypeUri::new_unchecked("https://test.kamu.dev/schemas/test/v2/TestResource")
    });

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// find_owned_snapshot tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_success() {
    let harness = GenericResourceQueryServiceHarness::new();
    let id = harness.allocate_id().await;
    let account_handle = odf::AccountHandle::new_test("test-owner");

    harness
        .insert_snapshot(id, &account_handle, TestResource::schema(), "res-a")
        .await;

    let result = harness
        .find_owned_snapshot(&account_handle.did, TestResource::schema(), id)
        .await;

    let snapshot = result.unwrap().unwrap();
    assert_eq!(snapshot.id, id);
    assert_eq!(snapshot.schema, *TestResource::schema());
    assert_eq!(snapshot.headers.account.did, account_handle.did);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_not_found() {
    let harness = GenericResourceQueryServiceHarness::new();
    let id = harness.allocate_id().await;
    let account_handle = odf::AccountHandle::new_test("test-owner");

    let result = harness
        .find_owned_snapshot(&account_handle.did, TestResource::schema(), id)
        .await;

    assert!(result.unwrap().is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_access_denied() {
    let harness = GenericResourceQueryServiceHarness::new();
    let id = harness.allocate_id().await;

    let account_handle_a = odf::AccountHandle::new_test("test-owner-a");
    let account_handle_b = odf::AccountHandle::new_test("test-owner-b");

    harness
        .insert_snapshot(id, &account_handle_a, TestResource::schema(), "res-a")
        .await;

    let result = harness
        .find_owned_snapshot(&account_handle_b.did, TestResource::schema(), id)
        .await;

    assert_matches!(
        result,
        Err(FindOwnedResourceError::Access(_)),
        "expected Access error, got {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// When find_owned_snapshot is called with a different schema,
// find_resource_snapshot filters by schema in the query and returns None — no
// snapshot is found.
#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_schema_mismatch_by_query() {
    let harness = GenericResourceQueryServiceHarness::new();
    let id = harness.allocate_id().await;

    let account_handle = odf::AccountHandle::new_test("test-owner");

    harness
        .insert_snapshot(id, &account_handle, TestResource::schema(), "res-a")
        .await;

    let result = harness
        .find_owned_snapshot(&account_handle.did, &OTHER_SCHEMA, id)
        .await;

    // The repository filters by schema in find_resource_snapshot, so a wrong
    // schema returns None rather than a type-mismatch error.
    assert!(result.unwrap().is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Paired with test_find_owned_snapshots_schema_mismatch (bulk) to guard
// against discrepancy: scalar queries by schema up front, while bulk
// categorizes schema mismatches after loading owned resources by ID.
#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_schema_mismatch_by_type() {
    let harness = GenericResourceQueryServiceHarness::new();
    let id = harness.allocate_id().await;

    let account_handle = odf::AccountHandle::new_test("test-owner");

    harness
        .insert_snapshot(id, &account_handle, TestResource::schema(), "res-a")
        .await;

    let result = harness
        .find_owned_snapshot(&account_handle.did, &NEWER_SCHEMA, id)
        .await;

    assert!(result.unwrap().is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// find_owned_snapshots tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_all_found() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_handle = odf::AccountHandle::new_test("test-owner");

    let uid_1 = harness.allocate_id().await;
    let uid_2 = harness.allocate_id().await;
    let uid_3 = harness.allocate_id().await;

    for (id, name) in [(uid_1, "res-1"), (uid_2, "res-2"), (uid_3, "res-3")] {
        harness
            .insert_snapshot(id, &account_handle, TestResource::schema(), name)
            .await;
    }

    let outcome = harness
        .find_owned_snapshots(
            &account_handle.did,
            TestResource::schema(),
            &[uid_1, uid_2, uid_3],
        )
        .await;

    assert_eq!(outcome.found.len(), 3);
    assert!(outcome.not_found.is_empty());
    assert!(outcome.access_denied.is_empty());
    assert!(outcome.schema_mismatch.is_empty());
    assert!(outcome.schema_mismatch.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_not_found() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_handle = odf::AccountHandle::new_test("test-owner");

    let uid_1 = harness.allocate_id().await;
    let uid_2 = harness.allocate_id().await;

    let outcome = harness
        .find_owned_snapshots(&account_handle.did, TestResource::schema(), &[uid_1, uid_2])
        .await;

    assert!(outcome.found.is_empty());
    assert_eq!(outcome.not_found.len(), 2);
    assert!(outcome.access_denied.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_access_denied() {
    let harness = GenericResourceQueryServiceHarness::new();
    let id = harness.allocate_id().await;

    let account_handle_a = odf::AccountHandle::new_test("test-owner-a");
    let account_handle_b = odf::AccountHandle::new_test("test-owner-b");

    harness
        .insert_snapshot(id, &account_handle_a, TestResource::schema(), "res-a")
        .await;

    let outcome = harness
        .find_owned_snapshots(&account_handle_b.did, TestResource::schema(), &[id])
        .await;

    assert!(outcome.found.is_empty());
    assert_eq!(outcome.access_denied, vec![id]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_schema_mismatch_by_type() {
    let harness = GenericResourceQueryServiceHarness::new();
    let id = harness.allocate_id().await;
    let account_handle = odf::AccountHandle::new_test("test-owner");

    harness
        .insert_snapshot(id, &account_handle, &OTHER_SCHEMA, "res-a")
        .await;

    let outcome = harness
        .find_owned_snapshots(&account_handle.did, TestResource::schema(), &[id])
        .await;

    assert!(outcome.found.is_empty());
    assert_eq!(outcome.schema_mismatch.len(), 1);
    assert_eq!(outcome.schema_mismatch[0].0, id);
    assert_eq!(outcome.schema_mismatch[0].1, *OTHER_SCHEMA);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_schema_mismatch_by_version() {
    let harness = GenericResourceQueryServiceHarness::new();
    let id = harness.allocate_id().await;
    let account_handle = odf::AccountHandle::new_test("test-owner");

    harness
        .insert_snapshot(id, &account_handle, &LEGACY_SCHEMA, "res-a")
        .await;

    let outcome = harness
        .find_owned_snapshots(&account_handle.did, TestResource::schema(), &[id])
        .await;

    assert!(outcome.found.is_empty());
    assert_eq!(outcome.schema_mismatch.len(), 1);
    assert_eq!(outcome.schema_mismatch[0].0, id);
    assert_eq!(outcome.schema_mismatch[0].1, *LEGACY_SCHEMA);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_mixed_outcomes() {
    let harness = GenericResourceQueryServiceHarness::new();

    let account_handle_a = odf::AccountHandle::new_test("test-owner-a");
    let account_handle_b = odf::AccountHandle::new_test("test-owner-b");

    // uid_found: correct owner and schema
    let uid_found = harness.allocate_id().await;
    harness
        .insert_snapshot(
            uid_found,
            &account_handle_a,
            TestResource::schema(),
            "res-found",
        )
        .await;

    // uid_schema_mismatch: owned by account_a but with a different schema
    let uid_schema_mismatch = harness.allocate_id().await;
    harness
        .insert_snapshot(
            uid_schema_mismatch,
            &account_handle_a,
            &OTHER_SCHEMA,
            "res-other-schema",
        )
        .await;

    // uid_access_denied: owned by account_b
    let uid_access_denied = harness.allocate_id().await;
    harness
        .insert_snapshot(
            uid_access_denied,
            &account_handle_b,
            TestResource::schema(),
            "res-denied",
        )
        .await;

    // uid_not_found: never inserted
    let uid_not_found = harness.allocate_id().await;

    let outcome = harness
        .find_owned_snapshots(
            &account_handle_a.did,
            TestResource::schema(),
            &[
                uid_found,
                uid_schema_mismatch,
                uid_access_denied,
                uid_not_found,
            ],
        )
        .await;

    assert_eq!(outcome.found.len(), 1);
    assert_eq!(outcome.found[0].id, uid_found);

    assert_eq!(outcome.schema_mismatch.len(), 1);
    assert_eq!(outcome.schema_mismatch[0].0, uid_schema_mismatch);

    assert_eq!(outcome.access_denied, vec![uid_access_denied]);

    assert_eq!(outcome.not_found, vec![uid_not_found]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseResourceServiceHarness, base)]
struct GenericResourceQueryServiceHarness {
    base: BaseResourceServiceHarness,
}

impl GenericResourceQueryServiceHarness {
    fn new() -> Self {
        let base = BaseResourceServiceHarness::new();
        Self { base }
    }

    async fn insert_snapshot(
        &self,
        id: ResourceID,
        account_handle: &odf::AccountHandle,
        schema: &TypeUri,
        name: &str,
    ) {
        let snapshot = ResourceSnapshot {
            id,
            schema: schema.clone(),
            headers: ResourceHeaders::simple(Utc::now(), id, account_handle.clone(), name),
            spec: serde_json::json!({"value": name}),
            status: None,
            last_event_id: None,
        };

        self.resource_repo()
            .create_resource(&snapshot)
            .await
            .unwrap();
    }

    async fn find_owned_snapshot(
        &self,
        account_id: &odf::AccountID,
        schema: &'static TypeUri,
        id: ResourceID,
    ) -> Result<Option<ResourceSnapshot>, FindOwnedResourceError> {
        self.generic_query_svc()
            .find_owned_snapshot(account_id, schema, id)
            .await
    }

    async fn find_owned_snapshots(
        &self,
        account_id: &odf::AccountID,
        schema: &'static TypeUri,
        ids: &[ResourceID],
    ) -> FindOwnedSnapshotsOutcome {
        self.generic_query_svc()
            .find_owned_snapshots(account_id, schema, ids)
            .await
            .unwrap()
    }

    async fn allocate_id(&self) -> ResourceID {
        self.generic_query_svc().allocate_id().await.unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
