// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ApplyManifestPlanningDecision, ApplyResourceOutcome};
use kamu_resources_facade::{
    ApplyManifestRequest,
    GetResourceError,
    ResourceManifestFormat,
    ResourceRef,
    ResourceSelector,
    SpecViewMode,
};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_API_VERSION,
    VARIABLE_SET_KIND,
    assert_applied_outcome,
    assert_planning_outcome,
    assert_resource_view_fields,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_selector(kind: &str, api_version: &str, name: &str) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: kind.to_string(),
        api_version: Some(api_version.to_string()),
        resource_ref: ResourceRef::ByName(name.to_string()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-010
contract_test!(plan_create_json, super::test_plan_create_json);

pub async fn test_plan_create_json(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let manifest = variable_set_manifest_json("my-vars", None, &[("FOO", "bar")]);

    let decision = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await
        .unwrap();

    assert_planning_outcome(&decision, ApplyResourceOutcome::Created);

    let ApplyManifestPlanningDecision::Planned(plan) = &decision else {
        unreachable!()
    };
    assert!(plan.executable, "plan must be executable");
    assert_resource_view_fields(
        &plan.resource,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "my-vars",
    );

    // Verify no side effect - resource must not exist yet
    let get_result = facade
        .get(
            make_selector(VARIABLE_SET_KIND, VARIABLE_SET_API_VERSION, "my-vars"),
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(get_result, Err(GetResourceError::LookupProblem(_))),
        "resource must not exist after planning"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-020
contract_test!(apply_create_json, super::test_apply_create_json);

pub async fn test_apply_create_json(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let manifest = variable_set_manifest_json("alpha", None, &[("KEY1", "val1")]);

    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await
        .unwrap();

    let view = assert_applied_outcome(&decision, ApplyResourceOutcome::Created);
    assert_resource_view_fields(view, VARIABLE_SET_KIND, VARIABLE_SET_API_VERSION, "alpha");
    assert_eq!(view.metadata.generation, 1, "initial generation must be 1");

    let uid = view.metadata.uid;

    // Verify resource is readable via get
    let fetched = facade
        .get(
            make_selector(VARIABLE_SET_KIND, VARIABLE_SET_API_VERSION, "alpha"),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    assert_eq!(fetched.metadata.uid, uid, "uid must match after apply");
    assert_resource_view_fields(
        &fetched,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "alpha",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-022
contract_test!(apply_update, super::test_apply_update);

pub async fn test_apply_update(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    // Create
    let create_manifest = variable_set_manifest_json("upd-vars", None, &[("A", "1")]);
    let create_decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: create_manifest,
        })
        .await
        .unwrap();
    let created = assert_applied_outcome(&create_decision, ApplyResourceOutcome::Created);
    let original_uid = created.metadata.uid;

    // Update spec
    let update_manifest = variable_set_manifest_json("upd-vars", None, &[("A", "1"), ("B", "2")]);
    let update_decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: update_manifest,
        })
        .await
        .unwrap();
    let updated = assert_applied_outcome(&update_decision, ApplyResourceOutcome::Updated);

    // uid preserved
    assert_eq!(
        updated.metadata.uid, original_uid,
        "uid must be preserved on update"
    );
    assert_eq!(
        updated.metadata.name, "upd-vars",
        "name must be preserved on update"
    );
    assert!(
        updated.metadata.updated_at >= created.metadata.updated_at,
        "updated_at must not be earlier after update"
    );

    // Verify via get
    let fetched = facade
        .get(
            make_selector(VARIABLE_SET_KIND, VARIABLE_SET_API_VERSION, "upd-vars"),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    assert_eq!(fetched.metadata.uid, original_uid);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-023
contract_test!(apply_idempotent, super::test_apply_idempotent);

pub async fn test_apply_idempotent(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let manifest = variable_set_manifest_json("idem-vars", None, &[("X", "42")]);

    // First apply
    let first_decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: manifest.clone(),
        })
        .await
        .unwrap();
    let first = assert_applied_outcome(&first_decision, ApplyResourceOutcome::Created);
    let uid = first.metadata.uid;
    let generation = first.metadata.generation;

    // Second apply with identical manifest
    let second_decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await
        .unwrap();
    let second = assert_applied_outcome(&second_decision, ApplyResourceOutcome::Untouched);

    assert_eq!(
        second.metadata.uid, uid,
        "uid must be preserved on no-op apply"
    );
    assert_eq!(
        second.metadata.generation, generation,
        "generation must not change on no-op apply"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
