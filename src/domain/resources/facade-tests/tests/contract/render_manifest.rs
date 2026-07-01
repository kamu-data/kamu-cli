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
    ApplyManifestRequest,
    RenderResourceManifestError,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourceRef,
    ResourceSelector,
    SpecViewMode,
};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_KIND,
    VARIABLE_SET_SCHEMA,
    assert_applied_outcome,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn by_name(name: &str) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        resource_ref: ResourceRef::ByName(name.parse().unwrap()),
    }
}

fn by_id(id: &kamu_resources::ResourceID) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        resource_ref: ResourceRef::ById(*id),
    }
}

async fn create_resource(h: &impl FacadeContractHarness, name: &str) -> kamu_resources::ResourceID {
    let facade = h.facade_for(TestAccount::Alice);
    let manifest = variable_set_manifest_json(name, None, &[("K", "v")]);
    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await
        .unwrap();
    let result = assert_applied_outcome(&decision, ApplyResourceOutcome::Created);
    result.headers.id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-070
contract_test!(render_json_by_name, super::test_render_json_by_name);

pub async fn test_render_json_by_name(h: &impl FacadeContractHarness) {
    create_resource(h, "render-json-name").await;
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .render_manifest(
            by_name("render-json-name"),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();

    assert_eq!(result.format, ResourceManifestFormat::Json);
    let parsed: serde_json::Value =
        serde_json::from_str(&result.manifest).expect("must be valid JSON");
    assert_eq!(parsed["$schema"], VARIABLE_SET_SCHEMA, "schema mismatch");
    assert_eq!(parsed["$schema"], VARIABLE_SET_SCHEMA, "schema mismatch");
    assert_eq!(
        parsed["headers"]["name"], "render-json-name",
        "name mismatch"
    );
    // UID must NOT appear in rendered manifests (render → apply round-trip
    // contract)
    assert!(
        parsed["headers"]["id"].is_null(),
        "rendered manifest must not include id"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-071
contract_test!(render_yaml_by_uid, super::test_render_yaml_by_uid);

pub async fn test_render_yaml_by_uid(h: &impl FacadeContractHarness) {
    let id = create_resource(h, "render-yaml-id").await;
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .render_manifest(
            by_id(&id),
            ResourceManifestFormat::Yaml,
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();

    assert_eq!(result.format, ResourceManifestFormat::Yaml);
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&result.manifest).expect("must be valid YAML");
    let parsed = serde_json::to_value(yaml).unwrap();
    assert_eq!(parsed["$schema"], VARIABLE_SET_SCHEMA, "schema mismatch");
    assert_eq!(parsed["$schema"], VARIABLE_SET_SCHEMA);
    assert_eq!(parsed["headers"]["name"], "render-yaml-id");
    assert!(
        parsed["headers"]["id"].is_null(),
        "rendered manifest must not include id"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-072
contract_test!(
    rendered_manifest_can_be_reapplied,
    super::test_rendered_manifest_can_be_reapplied
);

pub async fn test_rendered_manifest_can_be_reapplied(h: &impl FacadeContractHarness) {
    let id = create_resource(h, "render-reapply").await;
    let facade = h.facade_for(TestAccount::Alice);

    let rendered = facade
        .render_manifest(
            by_id(&id),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();

    // Plan with the rendered manifest — must be Untouched
    let plan = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: rendered.manifest.clone(),
        })
        .await
        .unwrap();
    assert!(
        matches!(
            plan,
            ApplyManifestPlanningDecision::Planned(ref p)
            if p.outcome == kamu_resources::ApplyResourceOutcome::Untouched
        ),
        "re-planning rendered manifest must be Untouched, got: {plan:?}"
    );

    // Apply with the rendered manifest — must also be Untouched
    let apply = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: rendered.manifest,
        })
        .await
        .unwrap();
    assert!(
        matches!(
            apply,
            ApplyManifestApplicationDecision::Applied(ref r)
            if r.outcome == kamu_resources::ApplyResourceOutcome::Untouched
        ),
        "re-applying rendered manifest must be Untouched, got: {apply:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-073
contract_test!(
    render_missing_resource_returns_not_found,
    super::test_render_missing_resource_returns_not_found
);

pub async fn test_render_missing_resource_returns_not_found(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let absent_uid = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());

    let by_name_result = facade
        .render_manifest(
            by_name("render-no-such"),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(
            by_name_result,
            Err(RenderResourceManifestError::LookupProblem(
                ResourceLookupProblem::NameNotFound(_)
            ))
        ),
        "expected NameNotFound, got: {by_name_result:?}"
    );

    let by_uid_result = facade
        .render_manifest(
            by_id(&absent_uid),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(
            by_uid_result,
            Err(RenderResourceManifestError::LookupProblem(
                ResourceLookupProblem::IDNotFound(_)
            ))
        ),
        "expected IDNotFound, got: {by_uid_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-074
contract_test!(
    render_wrong_schema_returns_mismatch,
    super::test_render_wrong_schema_returns_mismatch
);

pub async fn test_render_wrong_schema_returns_mismatch(h: &impl FacadeContractHarness) {
    use crate::helpers::SECRET_SET_KIND;

    let id = create_resource(h, "render-mismatch").await;
    let facade = h.facade_for(TestAccount::Alice);

    let wrong_schema_selector = ResourceSelector {
        account: None,
        kind: SECRET_SET_KIND.to_string(),
        resource_ref: ResourceRef::ById(id),
    };
    let result = facade
        .render_manifest(
            wrong_schema_selector,
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(
            result,
            Err(RenderResourceManifestError::LookupProblem(
                ResourceLookupProblem::SchemaMismatch(_)
            ))
        ),
        "expected SchemaMismatch, got: {result:?}"
    );

    let wrong_kind = ResourceSelector {
        account: None,
        kind: SECRET_SET_KIND.to_string(),
        resource_ref: ResourceRef::ById(id),
    };
    let result = facade
        .render_manifest(
            wrong_kind,
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(
            result,
            Err(RenderResourceManifestError::LookupProblem(
                ResourceLookupProblem::SchemaMismatch(_)
            ))
        ),
        "expected SchemaMismatch, got: {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
