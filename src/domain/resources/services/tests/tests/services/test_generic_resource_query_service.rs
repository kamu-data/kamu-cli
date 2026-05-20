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
    ResourceMetadata,
    ResourceSnapshot,
    ResourceUID,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

use crate::tests::utils::make_account_id;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// find_owned_snapshot tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_success() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let uid = harness.allocate_uid().await;

    harness
        .insert_snapshot(
            uid,
            account_a.clone(),
            "TestResource",
            "test.kamu.dev/v1",
            "res-a",
        )
        .await;

    let result = harness
        .find_owned_snapshot(&account_a, "TestResource", "test.kamu.dev/v1", uid)
        .await;

    let snapshot = result.unwrap().unwrap();
    assert_eq!(snapshot.uid, uid);
    assert_eq!(snapshot.kind, "TestResource");
    assert_eq!(snapshot.api_version, "test.kamu.dev/v1");
    assert_eq!(snapshot.metadata.account, account_a);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_not_found() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let uid = harness.allocate_uid().await;

    let result = harness
        .find_owned_snapshot(&account_a, "TestResource", "test.kamu.dev/v1", uid)
        .await;

    assert!(result.unwrap().is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_access_denied() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let account_b = make_account_id();
    let uid = harness.allocate_uid().await;

    harness
        .insert_snapshot(
            uid,
            account_a.clone(),
            "TestResource",
            "test.kamu.dev/v1",
            "res-a",
        )
        .await;

    let result = harness
        .find_owned_snapshot(&account_b, "TestResource", "test.kamu.dev/v1", uid)
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
async fn test_find_owned_snapshot_kind_mismatch() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let uid = harness.allocate_uid().await;

    harness
        .insert_snapshot(
            uid,
            account_a.clone(),
            "TestResource",
            "test.kamu.dev/v1",
            "res-a",
        )
        .await;

    let result = harness
        .find_owned_snapshot(&account_a, "OtherKind", "test.kamu.dev/v1", uid)
        .await;

    // The repository filters by kind in find_resource_snapshot, so a wrong kind
    // returns None rather than a type-mismatch error.
    assert!(result.unwrap().is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Paired with test_find_owned_snapshots_api_version_mismatch (bulk) to guard
// against discrepancy: scalar checks api_version after confirming ownership;
// bulk categorizes via kind + api_version buckets independently.
#[test_log::test(tokio::test)]
async fn test_find_owned_snapshot_api_version_mismatch() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let uid = harness.allocate_uid().await;

    harness
        .insert_snapshot(
            uid,
            account_a.clone(),
            "TestResource",
            "test.kamu.dev/v1",
            "res-a",
        )
        .await;

    let result = harness
        .find_owned_snapshot(&account_a, "TestResource", "wrong.dev/v99", uid)
        .await;

    assert!(
        matches!(result, Err(FindOwnedResourceError::TypeMismatch(_))),
        "expected TypeMismatch error, got {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// find_owned_snapshots tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_all_found() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();

    let uid_1 = harness.allocate_uid().await;
    let uid_2 = harness.allocate_uid().await;
    let uid_3 = harness.allocate_uid().await;

    for (uid, name) in [(uid_1, "res-1"), (uid_2, "res-2"), (uid_3, "res-3")] {
        harness
            .insert_snapshot(
                uid,
                account_a.clone(),
                "TestResource",
                "test.kamu.dev/v1",
                name,
            )
            .await;
    }

    let outcome = harness
        .find_owned_snapshots(
            &account_a,
            "TestResource",
            "test.kamu.dev/v1",
            &[uid_1, uid_2, uid_3],
        )
        .await;

    assert_eq!(outcome.found.len(), 3);
    assert!(outcome.not_found.is_empty());
    assert!(outcome.access_denied.is_empty());
    assert!(outcome.kind_mismatch.is_empty());
    assert!(outcome.api_version_mismatch.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_not_found() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();

    let uid_1 = harness.allocate_uid().await;
    let uid_2 = harness.allocate_uid().await;

    let outcome = harness
        .find_owned_snapshots(
            &account_a,
            "TestResource",
            "test.kamu.dev/v1",
            &[uid_1, uid_2],
        )
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
    let uid = harness.allocate_uid().await;

    harness
        .insert_snapshot(
            uid,
            account_a.clone(),
            "TestResource",
            "test.kamu.dev/v1",
            "res-a",
        )
        .await;

    let outcome = harness
        .find_owned_snapshots(&account_b, "TestResource", "test.kamu.dev/v1", &[uid])
        .await;

    assert!(outcome.found.is_empty());
    assert_eq!(outcome.access_denied, vec![uid]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_kind_mismatch() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let uid = harness.allocate_uid().await;

    harness
        .insert_snapshot(uid, account_a.clone(), "Foo", "test.kamu.dev/v1", "res-a")
        .await;

    let outcome = harness
        .find_owned_snapshots(&account_a, "Bar", "test.kamu.dev/v1", &[uid])
        .await;

    assert!(outcome.found.is_empty());
    assert_eq!(outcome.kind_mismatch.len(), 1);
    assert_eq!(outcome.kind_mismatch[0].0, uid);
    assert_eq!(outcome.kind_mismatch[0].1, "Foo");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_api_version_mismatch() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let uid = harness.allocate_uid().await;

    harness
        .insert_snapshot(
            uid,
            account_a.clone(),
            "TestResource",
            "test.kamu.dev/v1",
            "res-a",
        )
        .await;

    let outcome = harness
        .find_owned_snapshots(&account_a, "TestResource", "test.kamu.dev/v2", &[uid])
        .await;

    assert!(outcome.found.is_empty());
    assert_eq!(outcome.api_version_mismatch.len(), 1);
    assert_eq!(outcome.api_version_mismatch[0].0, uid);
    assert_eq!(outcome.api_version_mismatch[0].1, "test.kamu.dev/v1");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_owned_snapshots_mixed_outcomes() {
    let harness = GenericResourceQueryServiceHarness::new();
    let account_a = make_account_id();
    let account_b = make_account_id();

    // uid_found: correct owner, kind, api_version
    let uid_found = harness.allocate_uid().await;
    harness
        .insert_snapshot(
            uid_found,
            account_a.clone(),
            "TestResource",
            "test.kamu.dev/v1",
            "res-found",
        )
        .await;

    // uid_kind_mismatch: owned by account_a but with a different kind
    let uid_kind_mismatch = harness.allocate_uid().await;
    harness
        .insert_snapshot(
            uid_kind_mismatch,
            account_a.clone(),
            "OtherKind",
            "test.kamu.dev/v1",
            "res-other-kind",
        )
        .await;

    // uid_api_version_mismatch: owned by account_a, correct kind, wrong api_version
    let uid_api_mismatch = harness.allocate_uid().await;
    harness
        .insert_snapshot(
            uid_api_mismatch,
            account_a.clone(),
            "TestResource",
            "test.kamu.dev/v99",
            "res-api-mismatch",
        )
        .await;

    // uid_access_denied: owned by account_b
    let uid_access_denied = harness.allocate_uid().await;
    harness
        .insert_snapshot(
            uid_access_denied,
            account_b.clone(),
            "TestResource",
            "test.kamu.dev/v1",
            "res-denied",
        )
        .await;

    // uid_not_found: never inserted
    let uid_not_found = harness.allocate_uid().await;

    let outcome = harness
        .find_owned_snapshots(
            &account_a,
            "TestResource",
            "test.kamu.dev/v1",
            &[
                uid_found,
                uid_kind_mismatch,
                uid_api_mismatch,
                uid_access_denied,
                uid_not_found,
            ],
        )
        .await;

    assert_eq!(outcome.found.len(), 1);
    assert_eq!(outcome.found[0].uid, uid_found);

    assert_eq!(outcome.kind_mismatch.len(), 1);
    assert_eq!(outcome.kind_mismatch[0].0, uid_kind_mismatch);

    assert_eq!(outcome.api_version_mismatch.len(), 1);
    assert_eq!(outcome.api_version_mismatch[0].0, uid_api_mismatch);

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
        uid: ResourceUID,
        owner_account_id: odf::AccountID,
        kind: &str,
        api_version: &str,
        name: &str,
    ) {
        let snapshot = ResourceSnapshot {
            uid,
            kind: kind.to_string(),
            api_version: api_version.to_string(),
            metadata: ResourceMetadata::new_minimal(Utc::now(), owner_account_id, name),
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
        kind: &'static str,
        api_version: &'static str,
        uid: ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, FindOwnedResourceError> {
        self.generic_query_svc()
            .find_owned_snapshot(account_id, kind, api_version, uid)
            .await
    }

    async fn find_owned_snapshots(
        &self,
        account_id: &odf::AccountID,
        kind: &'static str,
        api_version: &'static str,
        uids: &[ResourceUID],
    ) -> FindOwnedSnapshotsOutcome {
        self.generic_query_svc()
            .find_owned_snapshots(account_id, kind, api_version, uids)
            .await
            .unwrap()
    }

    async fn allocate_uid(&self) -> ResourceUID {
        self.generic_query_svc().allocate_uid().await.unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
