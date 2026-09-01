// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::VariableSetResource;
use kamu_resources::{
    ApplyManifestApplicationDecision,
    ApplyManifestPlanningDecision,
    ApplyResourceOutcome,
    RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI,
    RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
    ResourceRef,
    ResourceSchemaProvider,
    ResourceWarning,
    TypeName,
    WARNING_CODE_RESOURCE_FREEFORM_ANNOTATIONS,
    WARNING_CODE_RESOURCE_FREEFORM_LABELS,
    WARNING_CODE_RESOURCE_LABEL_NOT_INDEXED,
};
use kamu_resources_facade::{
    ApplyManifestError,
    ApplyManifestRequest,
    ResourceHeadersValidationProblemCode,
    ResourceLookupProblem,
    ResourceManifestFormat,
    SpecViewOpts,
};
use pretty_assertions::{assert_eq, assert_matches};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_SCHEMA_STR,
    assert_applied_outcome,
    assert_planning_outcome,
    assert_resource_view_fields,
    assert_single_batch_problem,
    assert_single_batch_success,
    secret_set_manifest_json,
    variable_set_manifest_json,
    variable_set_manifest_yaml,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_selector(resource_type: &str, _schema: &str, name: &str) -> ResourceRef {
    ResourceRef {
        account: None,
        r#type: Some(resource_type.parse::<TypeName>().unwrap().into()),
        id: None,
        did: None,
        name: Some(name.parse().unwrap()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_warning_codes(warnings: &[ResourceWarning], expected_codes: &[&str]) {
    let codes = warnings
        .iter()
        .map(|warning| warning.code.as_str())
        .collect::<Vec<_>>();

    for expected_code in expected_codes {
        assert!(
            codes.contains(expected_code),
            "expected warning code '{expected_code}', got: {codes:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_invalid_headers_code(
    result: Result<impl std::fmt::Debug, ApplyManifestError>,
    expected_code: ResourceHeadersValidationProblemCode,
) {
    assert_matches!(
        result,
        Err(ApplyManifestError::InvalidHeaders(err)) if err.code == expected_code
    );
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
    assert_resource_view_fields(&plan.resource, VariableSetResource::schema(), "my-vars");

    // Verify no side effect - resource must not exist yet
    let get_result = facade
        .get(
            vec![make_selector(
                VARIABLE_SET_CANONICAL_SELECTOR,
                VARIABLE_SET_SCHEMA_STR,
                "my-vars",
            )],
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(get_result),
        ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::AnyTypeNameNotFound(_),
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
        VariableSetResource::schema(),
        "my-yaml-vars",
    );

    // Verify no side effect - resource must not exist yet
    let get_result = facade
        .get(
            vec![make_selector(
                VARIABLE_SET_CANONICAL_SELECTOR,
                VARIABLE_SET_SCHEMA_STR,
                "my-yaml-vars",
            )],
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(get_result),
        ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::AnyTypeNameNotFound(_),
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
    // The canonical documents must reflect the pending spec change: an existing
    // resource has a `before`, and the new variable is present only in `after`.
    let documents = plan.documents().unwrap();
    assert!(
        documents.before.is_some(),
        "an update must carry the pre-apply canonical manifest"
    );
    assert!(
        documents.has_changes(),
        "plan must report a difference between before and after"
    );
    assert_eq!(documents.after["spec"]["variables"]["B"]["value"], "2");
    assert!(
        documents.before.as_ref().unwrap()["spec"]["variables"]["B"].is_null(),
        "the new variable must not appear on the `before` side"
    );
    // Resource in store must remain unchanged (no side effect)
    let stored = assert_single_batch_success(
        facade
            .get(
                vec![make_selector(
                    VARIABLE_SET_CANONICAL_SELECTOR,
                    VARIABLE_SET_SCHEMA_STR,
                    "plan-upd-vars",
                )],
                SpecViewOpts::ENCRYPTED,
            )
            .await
            .unwrap(),
    );
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

    assert_matches!(
        result,
        Err(ApplyManifestError::ParseManifest(_)),
        "malformed JSON must produce ParseManifest error, got: {result:?}"
    );

    let result_yaml = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Yaml,
            manifest: ": : invalid yaml \t\0".to_string(),
        })
        .await;

    assert_matches!(
        result_yaml,
        Err(ApplyManifestError::ParseManifest(_)),
        "malformed YAML must produce ParseManifest error, got: {result_yaml:?}"
    );
}

// RF-015
contract_test!(
    plan_rejects_schema_invalid_manifest,
    super::test_plan_rejects_schema_invalid_manifest
);

pub async fn test_plan_rejects_schema_invalid_manifest(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    // Missing `spec` field entirely — fails spec deserialization
    let bad_manifest = serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA_STR,
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

    assert_matches!(
        result,
        Err(ApplyManifestError::ParseManifest(_)
            | ApplyManifestError::InvalidSpec(_)
            | ApplyManifestError::InvalidHeaders(_)),
        "schema-invalid manifest must fail with parse/spec/headers error, got: {result:?}"
    );

    // No resource should have been persisted
    let get = facade
        .get(
            vec![make_selector(
                VARIABLE_SET_CANONICAL_SELECTOR,
                VARIABLE_SET_SCHEMA_STR,
                "schema-invalid-vars",
            )],
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(get),
        ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::AnyTypeNameNotFound(_),
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
        "$schema": VARIABLE_SET_SCHEMA_STR,
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
    assert_matches!(
        plan_result,
        Ok(ApplyManifestPlanningDecision::Rejected(
            ApplyManifestRejection {
                category: ApplyResourceRejectionCategory::BusinessValidationFailed,
                ..
            }
        )),
        "plan with empty variables must return Ok(Rejected(BusinessValidationFailed)), got: \
         {plan_result:?}"
    );

    let apply_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: empty_vars,
        })
        .await;
    assert_matches!(
        apply_result,
        Ok(ApplyManifestApplicationDecision::Rejected(
            ApplyManifestRejection {
                category: ApplyResourceRejectionCategory::BusinessValidationFailed,
                ..
            }
        )),
        "apply with empty variables must return Ok(Rejected(BusinessValidationFailed)), got: \
         {apply_result:?}"
    );

    // Resource must not have been created
    let get = facade
        .get(
            vec![make_selector(
                VARIABLE_SET_CANONICAL_SELECTOR,
                VARIABLE_SET_SCHEMA_STR,
                "biz-invalid-vars",
            )],
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(get),
        ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::AnyTypeNameNotFound(_),
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
    assert_resource_view_fields(view, VariableSetResource::schema(), "alpha");
    assert_eq!(view.headers.generation, 1, "initial generation must be 1");

    let id = view.headers.id;

    // Verify resource is readable via get
    let fetched = assert_single_batch_success(
        facade
            .get(
                vec![make_selector(
                    VARIABLE_SET_CANONICAL_SELECTOR,
                    VARIABLE_SET_SCHEMA_STR,
                    "alpha",
                )],
                SpecViewOpts::ENCRYPTED,
            )
            .await
            .unwrap(),
    );
    assert_eq!(fetched.headers.id, id, "id must match after apply");
    assert_resource_view_fields(&fetched, VariableSetResource::schema(), "alpha");
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
    assert_resource_view_fields(view, VariableSetResource::schema(), "yaml-vars");
    assert_eq!(view.headers.generation, 1, "initial generation must be 1");

    // Semantic equivalence: same resource via get, just like after JSON apply
    let fetched = assert_single_batch_success(
        facade
            .get(
                vec![make_selector(
                    VARIABLE_SET_CANONICAL_SELECTOR,
                    VARIABLE_SET_SCHEMA_STR,
                    "yaml-vars",
                )],
                SpecViewOpts::ENCRYPTED,
            )
            .await
            .unwrap(),
    );
    assert_eq!(fetched.headers.id, view.headers.id);
    assert_resource_view_fields(&fetched, VariableSetResource::schema(), "yaml-vars");
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
    let original_id = created.headers.id;

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

    // id preserved
    assert_eq!(
        updated.headers.id, original_id,
        "id must be preserved on update"
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
    let fetched = assert_single_batch_success(
        facade
            .get(
                vec![make_selector(
                    VARIABLE_SET_CANONICAL_SELECTOR,
                    VARIABLE_SET_SCHEMA_STR,
                    "upd-vars",
                )],
                SpecViewOpts::ENCRYPTED,
            )
            .await
            .unwrap(),
    );
    assert_eq!(fetched.headers.id, original_id);
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
    let id: kamu_resources::ResourceID = first.headers.id;
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

    assert_eq!(second.headers.id, id, "id must be preserved on no-op apply");
    assert_eq!(
        second.headers.generation, generation,
        "generation must not change on no-op apply"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-024 (deferred): apply rejects immutable field change.
// Requires a resource type with at least one immutable spec or headers field
// that the facade contract forbids changing after creation. No current resource
// type (VariableSet, SecretSet) has such a field. Add once a suitable type
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
    let yaml_with_dup_label = indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA_STR}
        headers:
          name: dup-label-yaml
          labels:
            env: prod
            env: staging
        spec:
          variables:
            KEY: value
        "#
    );

    let yaml_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Yaml,
            manifest: yaml_with_dup_label,
        })
        .await;

    assert_matches!(
        yaml_result,
        Err(ApplyManifestError::ParseManifest(_)),
        "YAML with duplicate label key must fail with ParseManifest, got: {yaml_result:?}"
    );

    // JSON: use a raw string literal so the duplicate key reaches the parser
    // as actual duplicate JSON object members (the serde_json::json! macro
    // cannot preserve duplicates — it deduplicates at macro-expansion time).
    // The custom deserializer detects the duplicate and returns a parse error,
    // matching YAML behavior. Both formats are consistently rejected.
    let json_with_dup_label = indoc::indoc!(
        r#"{
            "$schema": VARIABLE_SET_SCHEMA,
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

// RF-026A (extension): an invalid label/annotation key is now rejected at
// manifest-parse time (via `TypeRef`'s own `FromStr`), not by
// `ResourceHeadersInput` validation — confirms this is a compile-time-style
// rejection (`ParseManifest`), not a semantic `InvalidHeaders` problem.
contract_test!(
    apply_rejects_invalid_header_key,
    super::test_apply_rejects_invalid_header_key
);

pub async fn test_apply_rejects_invalid_header_key(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    // Spaces and `!` are not valid in a short `TypeName`, and the key does not
    // start with `https:`, so `TypeRef::from_str` rejects it during
    // deserialization of the manifest itself.
    let yaml_with_invalid_label_key = indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA_STR}
        headers:
          name: invalid-label-key-yaml
          labels:
            "not a valid key!": prod
        spec:
          variables:
            KEY: value
        "#
    );

    let result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Yaml,
            manifest: yaml_with_invalid_label_key,
        })
        .await;

    assert_matches!(
        result,
        Err(ApplyManifestError::ParseManifest(_)),
        "invalid label key must fail with ParseManifest, got: {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-026B (extension): a successful apply carrying free-form label keys and a
// free-form annotation preserves them under their authored short names.
contract_test!(
    apply_round_trips_populated_labels_annotations,
    super::test_apply_round_trips_populated_labels_annotations
);

pub async fn test_apply_round_trips_populated_labels_annotations(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let manifest = indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA_STR}
        headers:
          name: labeled-vars
          labels:
            env: prod
            team:
              name: data-platform
              oncall:
                - alice
                - bob
          annotations:
            owner: https://github.com/open-data-fabric
        spec:
          variables:
            KEY: value
        "#
    );

    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Yaml,
            manifest,
        })
        .await
        .unwrap();

    let view = assert_applied_outcome(&decision, ApplyResourceOutcome::Created);
    assert_resource_view_fields(view, VariableSetResource::schema(), "labeled-vars");

    assert_eq!(view.headers.labels.entries.len(), 2);
    assert_eq!(
        view.headers.labels.entries.get(&"env".parse().unwrap()),
        Some(&serde_json::json!("prod"))
    );
    assert_eq!(
        view.headers.labels.entries.get(&"team".parse().unwrap()),
        Some(&serde_json::json!({ "name": "data-platform", "oncall": ["alice", "bob"] }))
    );
    assert_eq!(
        view.headers
            .annotations
            .entries
            .get(&"owner".parse().unwrap()),
        Some(&serde_json::json!("https://github.com/open-data-fabric"))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-026C
contract_test!(
    apply_header_extension_warnings_are_reported,
    super::test_apply_header_extension_warnings_are_reported
);

pub async fn test_apply_header_extension_warnings_are_reported(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let manifest = indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA_STR}
        headers:
          name: extension-warning-vars
          labels:
            arbitrary:
              nested: true
          annotations:
            description: Has a description
            note: check later
        spec:
          variables:
            KEY:
              value: value
        "#
    );

    let plan_decision = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Yaml,
            manifest: manifest.clone(),
        })
        .await
        .unwrap();

    let ApplyManifestPlanningDecision::Planned(plan) = &plan_decision else {
        panic!("expected Planned decision, got Rejected");
    };
    assert_warning_codes(
        &plan.warnings,
        &[
            WARNING_CODE_RESOURCE_LABEL_NOT_INDEXED,
            WARNING_CODE_RESOURCE_FREEFORM_LABELS,
            WARNING_CODE_RESOURCE_FREEFORM_ANNOTATIONS,
        ],
    );

    let apply_decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Yaml,
            manifest,
        })
        .await
        .unwrap();

    let ApplyManifestApplicationDecision::Applied(result) = &apply_decision else {
        panic!("expected Applied decision, got Rejected");
    };
    assert_warning_codes(
        &result.warnings,
        &[
            WARNING_CODE_RESOURCE_LABEL_NOT_INDEXED,
            WARNING_CODE_RESOURCE_FREEFORM_LABELS,
            WARNING_CODE_RESOURCE_FREEFORM_ANNOTATIONS,
        ],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-026D
contract_test!(
    apply_header_extension_canonicalization_precedes_diffing,
    super::test_apply_header_extension_canonicalization_precedes_diffing
);

pub async fn test_apply_header_extension_canonicalization_precedes_diffing(
    h: &impl FacadeContractHarness,
) {
    let facade = h.facade_for(TestAccount::Alice);

    let create_manifest = serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA_STR,
        "headers": {
            "name": "canonical-label-vars",
            "labels": {
                RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI: "prod"
            },
            "annotations": {
                RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI: "Has a description"
            }
        },
        "spec": {"variables": {"KEY": {"value": "value"}}}
    })
    .to_string();

    facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: create_manifest,
        })
        .await
        .unwrap();

    let reapply_manifest = serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA_STR,
        "headers": {
            "name": "canonical-label-vars",
            "labels": {
                "environment": "prod"
            },
            "annotations": {
                "description": "Has a description"
            }
        },
        "spec": {"variables": {"KEY": {"value": "value"}}}
    })
    .to_string();

    let decision = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: reapply_manifest,
        })
        .await
        .unwrap();

    let ApplyManifestPlanningDecision::Planned(plan) = &decision else {
        panic!("expected Planned decision, got Rejected");
    };
    assert_eq!(plan.outcome, ApplyResourceOutcome::Untouched);
    // Canonicalization happens before the documents are built, so re-spelling a
    // registered label/annotation key produces byte-identical documents rather
    // than a header difference.
    let documents = plan.documents().unwrap();
    assert_eq!(
        documents.before.as_ref(),
        Some(&documents.after),
        "spelling-only label/annotation key changes must not alter the canonical manifest"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-026E
contract_test!(
    apply_rejects_invalid_registered_header_extension_value,
    super::test_apply_rejects_invalid_registered_header_extension_value
);

pub async fn test_apply_rejects_invalid_registered_header_extension_value(
    h: &impl FacadeContractHarness,
) {
    let facade = h.facade_for(TestAccount::Alice);

    let manifest = serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA_STR,
        "headers": {
            "name": "invalid-extension-vars",
            "labels": {
                "environment": {"not": "a string"}
            }
        },
        "spec": {"variables": {"KEY": {"value": "value"}}}
    })
    .to_string();

    let plan_result = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: manifest.clone(),
        })
        .await;
    assert_invalid_headers_code(
        plan_result,
        ResourceHeadersValidationProblemCode::ResourceExtensionSchema,
    );

    let apply_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await;
    assert_invalid_headers_code(
        apply_result,
        ResourceHeadersValidationProblemCode::ResourceExtensionSchema,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-026F
contract_test!(
    apply_rejects_overlong_description_via_annotation_schema,
    super::test_apply_rejects_overlong_description_via_annotation_schema
);

pub async fn test_apply_rejects_overlong_description_via_annotation_schema(
    h: &impl FacadeContractHarness,
) {
    let facade = h.facade_for(TestAccount::Alice);

    let manifest = serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA_STR,
        "headers": {
            "name": "overlong-description-vars",
            "annotations": {
                "description": "x".repeat(4097)
            }
        },
        "spec": {"variables": {"KEY": {"value": "value"}}}
    })
    .to_string();

    let plan_result = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: manifest.clone(),
        })
        .await;
    assert_invalid_headers_code(
        plan_result,
        ResourceHeadersValidationProblemCode::ResourceExtensionSchema,
    );

    let apply_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await;
    assert_invalid_headers_code(
        apply_result,
        ResourceHeadersValidationProblemCode::ResourceExtensionSchema,
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
        "$schema": VARIABLE_SET_SCHEMA_STR,
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
    assert_matches!(
        plan_result,
        Err(ApplyManifestError::InvalidHeaders(_)),
        "plan with empty name must return Err(InvalidHeaders), got: {plan_result:?}"
    );

    let apply_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: empty_name_manifest,
        })
        .await;
    assert_matches!(
        apply_result,
        Err(ApplyManifestError::InvalidHeaders(_)),
        "apply with empty name must return Err(InvalidHeaders), got: {apply_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// A non-empty name that violates the ODF `ResourceName` grammar (underscores
// are not part of the hostname-like charset) must also surface as
// Err(InvalidHeaders(_)), not as a manifest parse failure or an internal
// error, on both local and remote facades.
contract_test!(
    apply_rejects_grammatically_invalid_name,
    super::test_apply_rejects_grammatically_invalid_name
);

pub async fn test_apply_rejects_grammatically_invalid_name(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let invalid_name_manifest = serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA_STR,
        "headers": {"name": "invalid_name"},
        "spec": {"variables": {"K": {"value": "v"}}}
    })
    .to_string();

    let plan_result = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: invalid_name_manifest.clone(),
        })
        .await;
    assert_matches!(
        plan_result,
        Err(ApplyManifestError::InvalidHeaders(_)),
        "plan with grammatically invalid name must return Err(InvalidHeaders), got: \
         {plan_result:?}"
    );

    let apply_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: invalid_name_manifest,
        })
        .await;
    assert_matches!(
        apply_result,
        Err(ApplyManifestError::InvalidHeaders(_)),
        "apply with grammatically invalid name must return Err(InvalidHeaders), got: \
         {apply_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-143 / apply error taxonomy — InvalidSpec carries schema
// Verifies that the remote facade reconstructs InvalidSpec with the correct
// schema fields.
// Uses a spec where `variables` is a string instead of an object — this fails
// JSON deserialization and therefore hits InvalidSpec, not
// BusinessValidationFailed.
contract_test!(
    apply_invalid_spec_carries_schema,
    super::test_apply_invalid_spec_carries_schema
);

pub async fn test_apply_invalid_spec_carries_schema(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    // `variables` is a string, not an object — fails serde deserialization →
    // InvalidSpec
    let malformed_spec = serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA_STR,
        "headers": {"name": "spec-schema-check"},
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
                e.schema.as_str(),
                VARIABLE_SET_SCHEMA_STR,
                "InvalidSpec must carry the correct schema"
            );
        }
        other => panic!("expected Err(InvalidSpec), got: {other:?}"),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-143 note: ImmutableFieldChanged, ReferencedObjectMissing, and
// LifecycleRuleConflict rejection categories are defined in the schema but not
// naturally triggerable through the current resource types (VariableSet,
// SecretSet). BusinessValidationFailed is now triggerable via empty variables
// (or empty secrets) — see apply_rejects_business_invalid_spec above. The
// remaining three are deferred until a resource type is added that can trigger
// them.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-027
contract_test!(
    apply_documents_never_expose_secret_plaintext,
    super::test_apply_documents_never_expose_secret_plaintext
);

/// The framework invariant lists "diffs" among the places `SecretSet` plaintext
/// must never appear. The canonical documents are the diff's raw material and
/// travel over the wire, so they are exactly such a place.
///
/// The spec sanitizer encrypts before the planner runs, so this holds by
/// construction today — the test exists because that ordering is easy to break
/// from a distance, and nothing else would catch it.
pub async fn test_apply_documents_never_expose_secret_plaintext(h: &impl FacadeContractHarness) {
    const SENTINEL: &str = "SUPER-SECRET-SENTINEL-VALUE";

    let facade = h.facade_for(TestAccount::Alice);

    // Plan a create: `before` is absent, `after` must already be ciphertext.
    let decision = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: secret_set_manifest_json("diff-secrets", None, &[("API_TOKEN", SENTINEL)]),
        })
        .await
        .unwrap();

    let ApplyManifestPlanningDecision::Planned(plan) = &decision else {
        panic!("expected Planned decision, got {decision:?}");
    };
    assert_documents_free_of(&plan.documents().unwrap(), SENTINEL, "plan of a create");

    // Apply it for real, then plan an update, so the `before` side is populated
    // from stored state — the other way plaintext could reach a document.
    facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: secret_set_manifest_json("diff-secrets", None, &[("API_TOKEN", SENTINEL)]),
        })
        .await
        .unwrap();

    let decision = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: secret_set_manifest_json(
                "diff-secrets",
                None,
                &[("API_TOKEN", SENTINEL), ("OTHER", "second-value")],
            ),
        })
        .await
        .unwrap();

    let ApplyManifestPlanningDecision::Planned(plan) = &decision else {
        panic!("expected Planned decision, got {decision:?}");
    };
    assert!(
        plan.documents().unwrap().before.is_some(),
        "an update must carry the pre-apply canonical manifest"
    );
    assert_documents_free_of(&plan.documents().unwrap(), SENTINEL, "plan of an update");
}

fn assert_documents_free_of(
    documents: &kamu_resources::ApplyManifestDocuments,
    sentinel: &str,
    context: &str,
) {
    if let Some(before) = &documents.before {
        let before = serde_json::to_string(before).unwrap();
        assert!(
            !before.contains(sentinel),
            "{context}: plaintext leaked into the `before` document"
        );
    }

    let after = serde_json::to_string(&documents.after).unwrap();
    assert!(
        !after.contains(sentinel),
        "{context}: plaintext leaked into the `after` document"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-028
contract_test!(
    apply_documents_match_rendered_manifest,
    super::test_apply_documents_match_rendered_manifest
);

/// The apply diff and `render_manifests` must agree on what a resource "is".
///
/// Both go through the same canonicalization, so a user who reads a diff and
/// then runs `kamu get -o yaml` sees the same document. This pins that shared
/// path so a future divergence fails here rather than confusing a user.
pub async fn test_apply_documents_match_rendered_manifest(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("parity-vars", None, &[("A", "1")]),
        })
        .await
        .unwrap();

    // Re-planning the same manifest is `Untouched`, so `before` is exactly the
    // stored resource's canonical form.
    let decision = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("parity-vars", None, &[("A", "1")]),
        })
        .await
        .unwrap();

    let ApplyManifestPlanningDecision::Planned(plan) = &decision else {
        panic!("expected Planned decision, got {decision:?}");
    };
    assert_eq!(plan.outcome, ApplyResourceOutcome::Untouched);

    let rendered = assert_single_batch_success(
        facade
            .render_manifests(
                vec![make_selector(
                    VARIABLE_SET_CANONICAL_SELECTOR,
                    VARIABLE_SET_SCHEMA_STR,
                    "parity-vars",
                )],
                ResourceManifestFormat::Json,
                SpecViewOpts::default(),
            )
            .await
            .unwrap(),
    );

    let rendered: serde_json::Value = serde_json::from_str(&rendered.manifest).unwrap();

    assert_eq!(
        plan.documents().unwrap().before.as_ref(),
        Some(&rendered),
        "the apply `before` document must equal the rendered manifest"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-029
contract_test!(
    apply_documents_before_is_absent_only_for_creates,
    super::test_apply_documents_before_is_absent_only_for_creates
);

/// `before == None` must mean "this resource did not exist", and nothing else.
///
/// Both facades are covered because they build the documents by different
/// routes — the local one canonicalizes a resource pair, the remote one decodes
/// what the server sent — and a regression in either would silently report an
/// update as a create.
pub async fn test_apply_documents_before_is_absent_only_for_creates(
    h: &impl FacadeContractHarness,
) {
    let facade = h.facade_for(TestAccount::Alice);

    // Create: no prior state, so `before` is absent and there is a difference.
    let decision = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("before-semantics", None, &[("A", "1")]),
        })
        .await
        .unwrap();
    let documents = decision.expect_planned().documents().unwrap();
    assert!(documents.before.is_none(), "a create must have no `before`");
    assert!(documents.has_changes());

    facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("before-semantics", None, &[("A", "1")]),
        })
        .await
        .unwrap();

    // Unchanged re-apply: `before` is present and equal, so there is no
    // difference. This is the case a placeholder document would get wrong.
    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("before-semantics", None, &[("A", "1")]),
        })
        .await
        .unwrap();
    let result = decision.expect_applied();
    assert_eq!(result.outcome, ApplyResourceOutcome::Untouched);

    let documents = result.documents().unwrap();
    assert!(
        documents.before.is_some(),
        "an unchanged apply must still carry `before`"
    );
    assert!(
        !documents.has_changes(),
        "an unchanged apply must report no difference"
    );

    // Update: `before` present, and the two sides differ.
    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("before-semantics", None, &[("A", "2")]),
        })
        .await
        .unwrap();
    let documents = decision.expect_applied().documents().unwrap();
    assert!(documents.before.is_some(), "an update must carry `before`");
    assert!(documents.has_changes());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
