// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{SubsecRound as _, Utc};
use kamu_resources::{
    ApplyManifestChangeKind,
    ResourceView,
    ResourceViewAccount,
    ResourceViewHeaders,
};
use kamu_resources_services::make_apply_manifest_changes;

use crate::tests::utils::{make_account_id, make_id};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_view(name: &str, value: &str) -> ResourceView {
    let id = make_id();
    ResourceView {
        kind: "TestResource".to_string(),
        api_version: "test.kamu.dev/v1".to_string(),
        account: ResourceViewAccount {
            id: make_account_id(),
            name: None,
        },
        headers: ResourceViewHeaders::simple(Utc::now(), id, name.to_string()),
        last_reconciled_at: None,
        spec: serde_json::json!({ "value": value }),
        status: None,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_make_changes_create_has_empty_before() {
    let after = make_view("res-a", "hello");
    let changes = make_apply_manifest_changes(None, &after).unwrap();

    assert!(
        !changes.is_empty(),
        "create diff must report at least the generation change"
    );
    assert!(changes.iter().all(|c| c.before.is_none()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_make_changes_detects_name_change() {
    let mut before = make_view("a", "same");
    let mut after = make_view("b", "same");

    // Force shared id/timestamps so they don't generate noise
    after.headers.id = before.headers.id;
    after.headers.created_at = before.headers.created_at;
    after.headers.updated_at = before.headers.updated_at;
    before.headers.generation = 1;
    after.headers.generation = 1;

    let changes = make_apply_manifest_changes(Some(&before), &after).unwrap();

    let name_changes: Vec<_> = changes
        .iter()
        .filter(|c| c.path == "headers.name")
        .collect();

    assert_eq!(name_changes.len(), 1, "expected exactly one name change");
    assert_eq!(name_changes[0].before, Some(serde_json::json!("a")));
    assert_eq!(name_changes[0].after, Some(serde_json::json!("b")));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_make_changes_detects_description_added() {
    let mut before = make_view("res", "val");
    let mut after = make_view("res", "val");

    after.headers.id = before.headers.id;
    after.headers.created_at = before.headers.created_at;
    after.headers.updated_at = before.headers.updated_at;
    before.headers.generation = 1;
    after.headers.generation = 1;
    before.headers.description = None;
    after.headers.description = Some("new desc".to_string());

    let changes = make_apply_manifest_changes(Some(&before), &after).unwrap();

    let desc_changes: Vec<_> = changes
        .iter()
        .filter(|c| c.path == "headers.description")
        .collect();

    assert_eq!(desc_changes.len(), 1);
    assert_eq!(desc_changes[0].before, None);
    assert_eq!(desc_changes[0].after, Some(serde_json::json!("new desc")));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_make_changes_detects_label_added() {
    let mut before = make_view("res", "val");
    let mut after = make_view("res", "val");

    after.headers.id = before.headers.id;
    after.headers.created_at = before.headers.created_at;
    after.headers.updated_at = before.headers.updated_at;
    before.headers.generation = 1;
    after.headers.generation = 1;
    after
        .headers
        .labels
        .insert("env".to_string(), "prod".to_string());

    let changes = make_apply_manifest_changes(Some(&before), &after).unwrap();

    let label_changes: Vec<_> = changes
        .iter()
        .filter(|c| c.path == "headers.labels")
        .collect();
    assert_eq!(label_changes.len(), 1, "expected a labels change entry");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_make_changes_detects_spec_field_change() {
    let mut before = make_view("res", "a");
    let after = make_view("res", "b");

    // Normalise shared fields
    let mut after = after;
    after.headers.id = before.headers.id;
    after.headers.created_at = before.headers.created_at;
    after.headers.updated_at = before.headers.updated_at;
    before.headers.generation = 1;
    after.headers.generation = 1;

    let changes = make_apply_manifest_changes(Some(&before), &after).unwrap();

    let spec_changes: Vec<_> = changes
        .iter()
        .filter(|c| c.kind == ApplyManifestChangeKind::Spec)
        .collect();

    assert_eq!(spec_changes.len(), 1, "expected one spec change");
    assert_eq!(spec_changes[0].path, "spec.value");
    assert_eq!(spec_changes[0].before, Some(serde_json::json!("a")));
    assert_eq!(spec_changes[0].after, Some(serde_json::json!("b")));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_make_changes_identical_before_after_returns_no_field_changes() {
    let ts = chrono::Utc::now().trunc_subsecs(6);
    let id = make_id();

    let make = || ResourceView {
        kind: "TestResource".to_string(),
        api_version: "test.kamu.dev/v1".to_string(),
        account: ResourceViewAccount {
            id: make_account_id(),
            name: None,
        },
        headers: ResourceViewHeaders {
            id,
            name: "res".to_string(),
            description: None,
            labels: Default::default(),
            annotations: Default::default(),
            generation: 2,
            created_at: ts,
            updated_at: ts,
            deleted_at: None,
        },
        last_reconciled_at: None,
        spec: serde_json::json!({ "value": "same" }),
        status: None,
    };

    let before = make();
    let after = make();
    let changes = make_apply_manifest_changes(Some(&before), &after).unwrap();

    // Generation is always included; all other fields should produce no changes.
    let non_gen: Vec<_> = changes
        .iter()
        .filter(|c| c.kind != ApplyManifestChangeKind::Generation)
        .collect();
    assert!(
        non_gen.is_empty(),
        "identical views should produce no headers/spec changes; got: {non_gen:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_timestamp_precision_normalized_avoids_spurious_diffs() {
    let ts_base = chrono::Utc::now().trunc_subsecs(6);
    // Add sub-microsecond noise (1 nanosecond) — should be normalized away
    let ts_noisy = ts_base + chrono::Duration::nanoseconds(1);

    let id = make_id();
    let account_id = make_account_id();

    let make_view_ts = |updated_at| ResourceView {
        kind: "TestResource".to_string(),
        api_version: "test.kamu.dev/v1".to_string(),
        account: ResourceViewAccount {
            id: account_id.clone(),
            name: None,
        },
        headers: ResourceViewHeaders {
            id,
            name: "res".to_string(),
            description: None,
            labels: Default::default(),
            annotations: Default::default(),
            generation: 1,
            created_at: ts_base,
            updated_at,
            deleted_at: None,
        },
        last_reconciled_at: None,
        spec: serde_json::json!({ "value": "same" }),
        status: None,
    };

    let before = make_view_ts(ts_base);
    let after = make_view_ts(ts_noisy);

    let changes = make_apply_manifest_changes(Some(&before), &after).unwrap();

    let ts_changes: Vec<_> = changes
        .iter()
        .filter(|c| c.path == "headers.updatedAt")
        .collect();
    assert!(
        ts_changes.is_empty(),
        "sub-microsecond timestamp differences must be normalized away"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
