// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_resources::{
    FindOwnedResourceError,
    FindOwnedSnapshotsOutcome,
    ResourceHeaders,
    ResourceID,
    ResourceSnapshot,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

use crate::tests::utils::{TestResource, make_account_id};

const OTHER_SCHEMA: &str = "https://test.kamu.dev/schemas/test/v1/OtherResource";
const LEGACY_SCHEMA: &str = "https://test.kamu.dev/schemas/test/v0/TestResource";
const NEWER_SCHEMA: &str = "https://test.kamu.dev/schemas/test/v2/TestResource";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// find_owned_snapshot tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_success() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let id = harness.allocate_id().await;

    harness
        .insert_snapshot(id, account_a.clone(), TestResource::SCHEMA, "res-a")
        .await;

    let result = harness
        .find_owned_snapshot(&account_a, TestResource::SCHEMA, id)
        .await;

    let snapshot = result.unwrap().unwrap();
    assert_eq!(snapshot.id, id);
    assert_eq!(snapshot.schema, TestResource::SCHEMA);
    assert_eq!(snapshot.headers.account, account_a);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_not_found() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let id = harness.allocate_id().await;

    let result = harness
        .find_owned_snapshot(&account_a, TestResource::SCHEMA, id)
        .await;

    assert!(result.unwrap().is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_access_denied() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let account_b = make_account_id();
    let id = harness.allocate_id().await;

    harness
        .insert_snapshot(id, account_a.clone(), TestResource::SCHEMA, "res-a")
        .await;

    let result = harness
        .find_owned_snapshot(&account_b, TestResource::SCHEMA, id)
        .await;

    assert!(
        matches!(result, Err(FindOwnedResourceError::Access(_))),
        "expected Access error, got {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// When find_owned_snapshot is called with a different kind,
// find_resource_snapshot filters by kind in the query and returns None — no
// snapshot is found.
#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_schema_mismatch_by_query() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let id = harness.allocate_id().await;

    harness
        .insert_snapshot(id, account_a.clone(), TestResource::SCHEMA, "res-a")
        .await;

    let result = harness
        .find_owned_snapshot(&account_a, OTHER_SCHEMA, id)
        .await;

    // The repository filters by kind in find_resource_snapshot, so a wrong kind
    // returns None rather than a type-mismatch error.
    assert!(result.unwrap().is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Paired with test_find_owned_snapshots_schema_mismatch (bulk) to guard
// against discrepancy: scalar queries by schema up front, while bulk
// categorizes schema mismatches after loading owned resources by ID.
#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_schema_mismatch_by_type() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let id = harness.allocate_id().await;

    harness
        .insert_snapshot(id, account_a.clone(), TestResource::SCHEMA, "res-a")
        .await;

    let result = harness
        .find_owned_snapshot(&account_a, NEWER_SCHEMA, id)
        .await;

    assert!(result.unwrap().is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// find_owned_snapshots tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_all_found() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();

    let uid_1 = harness.allocate_id().await;
    let uid_2 = harness.allocate_id().await;
    let uid_3 = harness.allocate_id().await;

    for (id, name) in [(uid_1, "res-1"), (uid_2, "res-2"), (uid_3, "res-3")] {
        harness
            .insert_snapshot(id, account_a.clone(), TestResource::SCHEMA, name)
            .await;
    }

    let outcome = harness
        .find_owned_snapshots(&account_a, TestResource::SCHEMA, &[uid_1, uid_2, uid_3])
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
    let account_a = make_account_id();

    let uid_1 = harness.allocate_id().await;
    let uid_2 = harness.allocate_id().await;

    let outcome = harness
        .find_owned_snapshots(&account_a, TestResource::SCHEMA, &[uid_1, uid_2])
        .await;

    assert!(outcome.found.is_empty());
    assert_eq!(outcome.not_found.len(), 2);
    assert!(outcome.access_denied.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_access_denied() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let account_b = make_account_id();
    let id = harness.allocate_id().await;

    harness
        .insert_snapshot(id, account_a.clone(), TestResource::SCHEMA, "res-a")
        .await;

    let outcome = harness
        .find_owned_snapshots(&account_b, TestResource::SCHEMA, &[id])
        .await;

    assert!(outcome.found.is_empty());
    assert_eq!(outcome.access_denied, vec![id]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_schema_mismatch_by_type() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let id = harness.allocate_id().await;

    harness
        .insert_snapshot(id, account_a.clone(), OTHER_SCHEMA, "res-a")
        .await;

    let outcome = harness
        .find_owned_snapshots(&account_a, TestResource::SCHEMA, &[id])
        .await;

    assert!(outcome.found.is_empty());
    assert_eq!(outcome.schema_mismatch.len(), 1);
    assert_eq!(outcome.schema_mismatch[0].0, id);
    assert_eq!(outcome.schema_mismatch[0].1, OTHER_SCHEMA);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_schema_mismatch_by_version() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let id = harness.allocate_id().await;

    harness
        .insert_snapshot(id, account_a.clone(), LEGACY_SCHEMA, "res-a")
        .await;

    let outcome = harness
        .find_owned_snapshots(&account_a, TestResource::SCHEMA, &[id])
        .await;

    assert!(outcome.found.is_empty());
    assert_eq!(outcome.schema_mismatch.len(), 1);
    assert_eq!(outcome.schema_mismatch[0].0, id);
    assert_eq!(outcome.schema_mismatch[0].1, LEGACY_SCHEMA);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_mixed_outcomes() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let account_b = make_account_id();

    // uid_found: correct owner and schema
    let uid_found = harness.allocate_id().await;
    harness
        .insert_snapshot(
            uid_found,
            account_a.clone(),
            TestResource::SCHEMA,
            "res-found",
        )
        .await;

    // uid_schema_mismatch: owned by account_a but with a different schema
    let uid_schema_mismatch = harness.allocate_id().await;
    harness
        .insert_snapshot(
            uid_schema_mismatch,
            account_a.clone(),
            OTHER_SCHEMA,
            "res-other-kind",
        )
        .await;

    // uid_access_denied: owned by account_b
    let uid_access_denied = harness.allocate_id().await;
    harness
        .insert_snapshot(
            uid_access_denied,
            account_b.clone(),
            TestResource::SCHEMA,
            "res-denied",
        )
        .await;

    // uid_not_found: never inserted
    let uid_not_found = harness.allocate_id().await;

    let outcome = harness
        .find_owned_snapshots(
            &account_a,
            TestResource::SCHEMA,
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
        owner_account_id: odf::AccountID,
        schema: &str,
        name: &str,
    ) {
        let snapshot = ResourceSnapshot {
            id,
            schema: schema.to_string(),
            headers: ResourceHeaders::simple(Utc::now(), owner_account_id, name),
            spec: serde_json::json!({"value": name}),
            status: None,
            last_reconciled_at: None,
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
        schema: &'static str,
        id: ResourceID,
    ) -> Result<Option<ResourceSnapshot>, FindOwnedResourceError> {
        self.generic_query_svc()
            .find_owned_snapshot(account_id, schema, id)
            .await
    }

    async fn find_owned_snapshots(
        &self,
        account_id: &odf::AccountID,
        schema: &'static str,
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
