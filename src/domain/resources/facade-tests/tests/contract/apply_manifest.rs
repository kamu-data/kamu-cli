// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    ApplyManifestApplicationDecision,
    ApplyManifestPlanningDecision,
    ApplyResourceOutcome,
};
use kamu_resources_facade::{
    ApplyManifestError,
    ApplyManifestRequest,
    GetResourceError,
    ResourceManifestFormat,
    ResourceRef,
    ResourceSelector,
    SpecViewMode,
};
use pretty_assertions::{assert_eq, assert_matches};

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

// RF-011
contract_test!(plan_create_yaml, super::test_plan_create_yaml);

pub async fn test_plan_create_yaml(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let manifest = variable_set_manifest_yaml("my-yaml-vars", None, &[("FOO", "bar")]);

    let decision = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Yaml,
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
        "my-yaml-vars",
    );

    // Verify no side effect - resource must not exist yet
    let get_result = facade
        .get(
            make_selector(VARIABLE_SET_KIND, VARIABLE_SET_API_VERSION, "my-yaml-vars"),
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(get_result, Err(GetResourceError::LookupProblem(_))),
        "resource must not exist after planning"
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
        "headers": {"name": "schema-invalid-vars"}
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
                | ApplyManifestError::InvalidHeaders(_))
        ),
        "schema-invalid manifest must fail with parse/spec/headers error, got: {result:?}"
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
// deserializes successfully but fails VariableSetSpec::validate() inside the
// lifecycle try_create/try_update_spec, producing a BusinessValidationFailed
// rejection rather than an InvalidSpec error.
contract_test!(
    apply_rejects_business_invalid_spec,
    super::test_apply_rejects_business_invalid_spec
);

pub async fn test_apply_rejects_business_invalid_spec(h: &impl FacadeContractHarness) {
    use kamu_resources::{ApplyManifestRejection, ApplyResourceRejectionCategory};

    let facade = h.facade_for(TestAccount::Alice);

    // Empty variables map deserializes correctly but fails
    // VariableSetSpec::validate() inside the lifecycle; the facade surfaces
    // this as Ok(Rejected(BusinessValidationFailed)).
    let empty_vars = serde_json::json!({
        "apiVersion": VARIABLE_SET_API_VERSION,
        "kind": VARIABLE_SET_KIND,
        "headers": {"name": "biz-invalid-vars"},
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
        matches!(
            plan_result,
            Ok(ApplyManifestPlanningDecision::Rejected(
                ApplyManifestRejection {
                    category: ApplyResourceRejectionCategory::BusinessValidationFailed,
                    ..
                }
            ))
        ),
        "plan with empty variables must return Ok(Rejected(BusinessValidationFailed)), got: \
         {plan_result:?}"
    );

    let apply_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: empty_vars,
        })
        .await;
    assert!(
        matches!(
            apply_result,
            Ok(ApplyManifestApplicationDecision::Rejected(
                ApplyManifestRejection {
                    category: ApplyResourceRejectionCategory::BusinessValidationFailed,
                    ..
                }
            ))
        ),
        "apply with empty variables must return Ok(Rejected(BusinessValidationFailed)), got: \
         {apply_result:?}"
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
    assert_eq!(view.headers.generation, 1, "initial generation must be 1");

    let uid = view.headers.uid;

    // Verify resource is readable via get
    let fetched = facade
        .get(
            make_selector(VARIABLE_SET_KIND, VARIABLE_SET_API_VERSION, "alpha"),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    assert_eq!(fetched.headers.uid, uid, "uid must match after apply");
    assert_resource_view_fields(
        &fetched,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "alpha",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-021
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
    assert_eq!(view.headers.generation, 1, "initial generation must be 1");

    // Semantic equivalence: same resource via get, just like after JSON apply
    let fetched = facade
        .get(
            make_selector(VARIABLE_SET_KIND, VARIABLE_SET_API_VERSION, "yaml-vars"),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    assert_eq!(fetched.headers.uid, view.headers.uid);
    assert_resource_view_fields(
        &fetched,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "yaml-vars",
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
    let original_uid = created.headers.uid;

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
        updated.headers.uid, original_uid,
        "uid must be preserved on update"
    );
    assert_eq!(
        updated.headers.name, "upd-vars",
        "name must be preserved on update"
    );
    assert!(
        updated.headers.updated_at >= created.headers.updated_at,
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
    assert_eq!(fetched.headers.uid, original_uid);
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
    let uid = first.headers.uid;
    let generation = first.headers.generation;

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
        second.headers.uid, uid,
        "uid must be preserved on no-op apply"
    );
    assert_eq!(
        second.headers.generation, generation,
        "generation must not change on no-op apply"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-024 (deferred): apply rejects immutable field change.
// Requires a resource kind with at least one immutable spec or headers field
// that the facade contract forbids changing after creation. No current resource
// kind (VariableSet, SecretSet) has such a field. Add once a suitable kind
// exists.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-026
contract_test!(
    apply_rejects_duplicate_header_key,
    super::test_apply_rejects_duplicate_header_key
);

pub async fn test_apply_rejects_duplicate_header_key(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    // YAML: serde_yaml visits both duplicate key occurrences so the custom
    // deserialize_string_entries visitor detects the second one and returns a
    // parse error. Expected: Err(ParseManifest).
    let yaml_with_dup_label = indoc::indoc!(
        r#"
        apiVersion: "kamu.dev/v1alpha1"
        kind: VariableSet
        headers:
          name: dup-label-yaml
          labels:
            env: prod
            env: staging
        spec:
          variables:
            KEY: value
        "#
    )
    .to_string();

    let yaml_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Yaml,
            manifest: yaml_with_dup_label,
        })
        .await;

    assert!(
        matches!(yaml_result, Err(ApplyManifestError::ParseManifest(_))),
        "YAML with duplicate label key must fail with ParseManifest, got: {yaml_result:?}"
    );

    // JSON: use a raw string literal so the duplicate key reaches the parser
    // as actual duplicate JSON object members (the serde_json::json! macro
    // cannot preserve duplicates — it deduplicates at macro-expansion time).
    // The custom deserializer detects the duplicate and returns a parse error,
    // matching YAML behavior. Both formats are consistently rejected.
    let json_with_dup_label = indoc::indoc!(
        r#"{
            "apiVersion": "kamu.dev/v1alpha1",
            "kind": "VariableSet",
            "headers": {
                "name": "dup-label-json",
                "labels": {"env": "prod", "env": "staging"}
            },
            "spec": {"variables": {"KEY": "value"}}
        }"#
    )
    .to_string();

    let json_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: json_with_dup_label,
        })
        .await;

    assert_matches!(
        json_result,
        Err(ApplyManifestError::ParseManifest(_)),
        "JSON with duplicate label key must fail with ParseManifest"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-143 / apply error taxonomy — InvalidHeaders
// Empty resource name fails headers validation before the use case runs.
// Both local and remote facades must return Err(InvalidHeaders(_)) with the
// same variant identity (not demoted to Internal on the remote path).
contract_test!(
    apply_rejects_invalid_headers,
    super::test_apply_rejects_invalid_headers
);

pub async fn test_apply_rejects_invalid_headers(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let empty_name_manifest = serde_json::json!({
        "apiVersion": VARIABLE_SET_API_VERSION,
        "kind": VARIABLE_SET_KIND,
        "headers": {"name": ""},
        "spec": {"variables": {"K": {"value": "v"}}}
    })
    .to_string();

    let plan_result = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: empty_name_manifest.clone(),
        })
        .await;
    assert!(
        matches!(plan_result, Err(ApplyManifestError::InvalidHeaders(_))),
        "plan with empty name must return Err(InvalidHeaders), got: {plan_result:?}"
    );

    let apply_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: empty_name_manifest,
        })
        .await;
    assert!(
        matches!(apply_result, Err(ApplyManifestError::InvalidHeaders(_))),
        "apply with empty name must return Err(InvalidHeaders), got: {apply_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-143 / apply error taxonomy — InvalidSpec carries kind and api_version
// Verifies that the remote facade reconstructs InvalidSpec with the correct
// kind and api_version fields (previously both were empty strings on the
// remote path due to the lossy ResourceApplyError shape).
// Uses a spec where `variables` is a string instead of an object — this fails
// JSON deserialization and therefore hits InvalidSpec, not
// BusinessValidationFailed.
contract_test!(
    apply_invalid_spec_carries_kind_and_api_version,
    super::test_apply_invalid_spec_carries_kind_and_api_version
);

pub async fn test_apply_invalid_spec_carries_kind_and_api_version(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    // `variables` is a string, not an object — fails serde deserialization →
    // InvalidSpec
    let malformed_spec = serde_json::json!({
        "apiVersion": VARIABLE_SET_API_VERSION,
        "kind": VARIABLE_SET_KIND,
        "headers": {"name": "spec-kind-check"},
        "spec": {"variables": "not-an-object"}
    })
    .to_string();

    let result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: malformed_spec,
        })
        .await;

    match result {
        Err(ApplyManifestError::InvalidSpec(e)) => {
            assert_eq!(
                e.kind, VARIABLE_SET_KIND,
                "InvalidSpec must carry the correct kind"
            );
            assert_eq!(
                e.api_version, VARIABLE_SET_API_VERSION,
                "InvalidSpec must carry the correct api_version"
            );
        }
        other => panic!("expected Err(InvalidSpec), got: {other:?}"),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-143 note: ImmutableFieldChanged, ReferencedObjectMissing, and
// LifecycleRuleConflict rejection categories are defined in the schema but not
// naturally triggerable through the current resource kinds (VariableSet,
// SecretSet). BusinessValidationFailed is now triggerable via empty variables
// (or empty secrets) — see apply_rejects_business_invalid_spec above. The
// remaining three are deferred until a resource kind is added that can trigger
// them.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
