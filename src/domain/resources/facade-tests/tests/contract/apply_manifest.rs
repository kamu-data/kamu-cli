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
    ApplyManifestError,
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
    variable_set_manifest_yaml,
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

// RF-011
contract_test!(apply_create_yaml, super::test_apply_create_yaml);

pub async fn test_apply_create_yaml(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let manifest_yaml = variable_set_manifest_yaml("yaml-vars", None, &[("KEY1", "val1")]);

    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Yaml,
            manifest: manifest_yaml,
        })
        .await
        .unwrap();

    let view = assert_applied_outcome(&decision, ApplyResourceOutcome::Created);
    assert_resource_view_fields(
        view,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "yaml-vars",
    );
    assert_eq!(view.metadata.generation, 1, "initial generation must be 1");

    // Semantic equivalence: same resource via get, just like after JSON apply
    let fetched = facade
        .get(
            make_selector(VARIABLE_SET_KIND, VARIABLE_SET_API_VERSION, "yaml-vars"),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    assert_eq!(fetched.metadata.uid, view.metadata.uid);
    assert_resource_view_fields(
        &fetched,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "yaml-vars",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-012
contract_test!(plan_update, super::test_plan_update);

pub async fn test_plan_update(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    // Create first
    facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("plan-upd-vars", None, &[("A", "1")]),
        })
        .await
        .unwrap();

    // Plan an update with changed spec
    let decision = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("plan-upd-vars", None, &[("A", "1"), ("B", "2")]),
        })
        .await
        .unwrap();

    assert_planning_outcome(&decision, ApplyResourceOutcome::Updated);

    let ApplyManifestPlanningDecision::Planned(plan) = &decision else {
        unreachable!()
    };
    // The spec change must appear among the reported changes
    assert!(
        plan.changes
            .iter()
            .any(|c| matches!(c.kind, kamu_resources::ApplyManifestChangeKind::Spec)),
        "plan must report a spec change"
    );
    // Resource in store must remain unchanged (no side effect)
    let stored = facade
        .get(
            make_selector(VARIABLE_SET_KIND, VARIABLE_SET_API_VERSION, "plan-upd-vars"),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    let stored_spec: serde_json::Value = stored.spec;
    assert!(
        stored_spec["variables"]["B"].is_null(),
        "planning must not persist the update"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-013
contract_test!(plan_unchanged, super::test_plan_unchanged);

pub async fn test_plan_unchanged(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let manifest = variable_set_manifest_json("plan-same-vars", None, &[("X", "42")]);

    facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: manifest.clone(),
        })
        .await
        .unwrap();

    let decision = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await
        .unwrap();

    assert_planning_outcome(&decision, ApplyResourceOutcome::Untouched);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-014
contract_test!(
    plan_rejects_malformed_manifest,
    super::test_plan_rejects_malformed_manifest
);

pub async fn test_plan_rejects_malformed_manifest(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: "not valid json {{{".to_string(),
        })
        .await;

    assert!(
        matches!(result, Err(ApplyManifestError::ParseManifest(_))),
        "malformed JSON must produce ParseManifest error, got: {result:?}"
    );

    let result_yaml = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Yaml,
            manifest: ": : invalid yaml \t\0".to_string(),
        })
        .await;

    assert!(
        matches!(result_yaml, Err(ApplyManifestError::ParseManifest(_))),
        "malformed YAML must produce ParseManifest error, got: {result_yaml:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-015
contract_test!(
    plan_rejects_schema_invalid_manifest,
    super::test_plan_rejects_schema_invalid_manifest
);

pub async fn test_plan_rejects_schema_invalid_manifest(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    // Missing `spec` field entirely — fails spec deserialization
    let bad_manifest = serde_json::json!({
        "apiVersion": VARIABLE_SET_API_VERSION,
        "kind": VARIABLE_SET_KIND,
        "metadata": {"name": "schema-invalid-vars"}
        // no "spec"
    })
    .to_string();

    let result = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: bad_manifest,
        })
        .await;

    assert!(
        matches!(
            result,
            Err(ApplyManifestError::ParseManifest(_)
                | ApplyManifestError::InvalidSpec(_)
                | ApplyManifestError::InvalidMetadata(_))
        ),
        "schema-invalid manifest must fail with parse/spec/metadata error, got: {result:?}"
    );

    // No resource should have been persisted
    let get = facade
        .get(
            make_selector(
                VARIABLE_SET_KIND,
                VARIABLE_SET_API_VERSION,
                "schema-invalid-vars",
            ),
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(get, Err(GetResourceError::LookupProblem(_))),
        "resource must not exist after schema-invalid plan"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-016 / RF-025
// VariableSetResource rejects business-invalid specs. An empty variables map
// fails VariableSetSpec::validate() at parse time, surfaced as InvalidSpec.
contract_test!(
    apply_rejects_business_invalid_spec,
    super::test_apply_rejects_business_invalid_spec
);

pub async fn test_apply_rejects_business_invalid_spec(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    // Empty variables map fails VariableSetSpec::validate() at spec decode time;
    // the facade surfaces this as ApplyManifestError::InvalidSpec.
    let empty_vars = serde_json::json!({
        "apiVersion": VARIABLE_SET_API_VERSION,
        "kind": VARIABLE_SET_KIND,
        "metadata": {"name": "biz-invalid-vars"},
        "spec": {"variables": {}}
    })
    .to_string();

    let plan_result = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: empty_vars.clone(),
        })
        .await;
    assert!(
        matches!(plan_result, Err(ApplyManifestError::InvalidSpec(_))),
        "plan with empty variables must return Err(InvalidSpec), got: {plan_result:?}"
    );

    let apply_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: empty_vars,
        })
        .await;
    assert!(
        matches!(apply_result, Err(ApplyManifestError::InvalidSpec(_))),
        "apply with empty variables must return Err(InvalidSpec), got: {apply_result:?}"
    );

    // Resource must not have been created
    let get = facade
        .get(
            make_selector(
                VARIABLE_SET_KIND,
                VARIABLE_SET_API_VERSION,
                "biz-invalid-vars",
            ),
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(get, Err(GetResourceError::LookupProblem(_))),
        "resource must not exist after rejected apply"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
