// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Error consistency tests (RF-140..141).
//!
//! Validates that equivalent lookup failures produce equivalent
//! `ResourceLookupProblem` variants across all single-resource APIs (RF-140)
//! and batch APIs (RF-141).

use kamu_resources::ApplyResourceOutcome;
use kamu_resources_facade::{
    ApplyManifestRequest,
    DeleteResourceError,
    GetResourceError,
    RenderResourceManifestError,
    ResourceBatchSelector,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourceRef,
    ResourceSelector,
    SpecViewMode,
};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_API_VERSION,
    VARIABLE_SET_KIND,
    assert_applied_outcome,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn by_name(name: &str) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_ref: ResourceRef::ByName(name.to_string()),
    }
}

fn by_id(uid: &kamu_resources::ResourceUID) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_ref: ResourceRef::ById(*uid),
    }
}

fn batch_by_name(name: &str) -> ResourceBatchSelector {
    ResourceBatchSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_refs: vec![ResourceRef::ByName(name.to_string())],
    }
}

fn batch_by_id(uid: kamu_resources::ResourceUID) -> ResourceBatchSelector {
    ResourceBatchSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_refs: vec![ResourceRef::ById(uid)],
    }
}

async fn create_resource(
    h: &impl FacadeContractHarness,
    name: &str,
) -> kamu_resources::ResourceUID {
    let facade = h.facade_for(TestAccount::Alice);
    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json(name, None, &[("K", "v")]),
        })
        .await
        .unwrap();
    assert_applied_outcome(&decision, ApplyResourceOutcome::Created)
        .metadata
        .uid
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-140: single-resource lookup error taxonomy is consistent across get,
// get_identity, render_manifest, and delete.
contract_test!(
    single_resource_lookup_taxonomy,
    super::test_single_resource_lookup_taxonomy
);

pub async fn test_single_resource_lookup_taxonomy(h: &impl FacadeContractHarness) {
    let uid = create_resource(h, "taxonomy-single").await;
    let absent_uid = kamu_resources::ResourceUID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    // --- NameNotFound ---
    let missing_name = "taxonomy-missing";

    let get = facade
        .get(by_name(missing_name), SpecViewMode::Encrypted)
        .await;
    assert!(
        matches!(
            get,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::NameNotFound(_)
            ))
        ),
        "get: expected NameNotFound, got {get:?}"
    );

    let get_id = facade.get_identity(by_name(missing_name)).await;
    assert!(
        matches!(
            get_id,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::NameNotFound(_)
            ))
        ),
        "get_identity: expected NameNotFound, got {get_id:?}"
    );

    let render = facade
        .render_manifest(
            by_name(missing_name),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(
            render,
            Err(RenderResourceManifestError::LookupProblem(
                ResourceLookupProblem::NameNotFound(_)
            ))
        ),
        "render_manifest: expected NameNotFound, got {render:?}"
    );

    let del = facade.delete(by_name(missing_name)).await;
    assert!(
        matches!(
            del,
            Err(DeleteResourceError::LookupProblem(
                ResourceLookupProblem::NameNotFound(_)
            ))
        ),
        "delete: expected NameNotFound, got {del:?}"
    );

    // --- UIDNotFound ---
    let get = facade
        .get(by_id(&absent_uid), SpecViewMode::Encrypted)
        .await;
    assert!(
        matches!(
            get,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::UIDNotFound(_)
            ))
        ),
        "get: expected UIDNotFound, got {get:?}"
    );

    let get_id = facade.get_identity(by_id(&absent_uid)).await;
    assert!(
        matches!(
            get_id,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::UIDNotFound(_)
            ))
        ),
        "get_identity: expected UIDNotFound, got {get_id:?}"
    );

    let render = facade
        .render_manifest(
            by_id(&absent_uid),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(
            render,
            Err(RenderResourceManifestError::LookupProblem(
                ResourceLookupProblem::UIDNotFound(_)
            ))
        ),
        "render_manifest: expected UIDNotFound, got {render:?}"
    );

    let del = facade.delete(by_id(&absent_uid)).await;
    assert!(
        matches!(
            del,
            Err(DeleteResourceError::LookupProblem(
                ResourceLookupProblem::UIDNotFound(_)
            ))
        ),
        "delete: expected UIDNotFound, got {del:?}"
    );

    // --- ApiVersionMismatch ---
    let wrong_version = ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some("v0.never.existed".to_string()),
        resource_ref: ResourceRef::ById(uid),
    };

    let get = facade
        .get(wrong_version.clone(), SpecViewMode::Encrypted)
        .await;
    assert!(
        matches!(
            get,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::ApiVersionMismatch(_)
            ))
        ),
        "get: expected ApiVersionMismatch, got {get:?}"
    );

    let get_id = facade.get_identity(wrong_version.clone()).await;
    assert!(
        matches!(
            get_id,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::ApiVersionMismatch(_)
            ))
        ),
        "get_identity: expected ApiVersionMismatch, got {get_id:?}"
    );

    let render = facade
        .render_manifest(
            wrong_version.clone(),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(
            render,
            Err(RenderResourceManifestError::LookupProblem(
                ResourceLookupProblem::ApiVersionMismatch(_)
            ))
        ),
        "render_manifest: expected ApiVersionMismatch, got {render:?}"
    );

    let del = facade.delete(wrong_version).await;
    assert!(
        matches!(
            del,
            Err(DeleteResourceError::LookupProblem(
                ResourceLookupProblem::ApiVersionMismatch(_)
            ))
        ),
        "delete: expected ApiVersionMismatch, got {del:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-141: batch lookup problem taxonomy mirrors the single-resource taxonomy.
contract_test!(batch_lookup_taxonomy, super::test_batch_lookup_taxonomy);

pub async fn test_batch_lookup_taxonomy(h: &impl FacadeContractHarness) {
    let absent_uid = kamu_resources::ResourceUID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    // --- NameNotFound in get_many ---
    let resp = facade
        .get_many(
            batch_by_name("taxonomy-batch-missing"),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    assert_eq!(resp.problems.len(), 1);
    assert!(
        matches!(
            &resp.problems[0].error,
            ResourceLookupProblem::NameNotFound(_)
        ),
        "get_many: expected NameNotFound problem, got {:?}",
        resp.problems[0].error
    );

    // --- UIDNotFound in get_many ---
    let resp = facade
        .get_many(batch_by_id(absent_uid), SpecViewMode::Encrypted)
        .await
        .unwrap();
    assert_eq!(resp.problems.len(), 1);
    assert!(
        matches!(
            &resp.problems[0].error,
            ResourceLookupProblem::UIDNotFound(_)
        ),
        "get_many: expected UIDNotFound problem, got {:?}",
        resp.problems[0].error
    );

    // --- NameNotFound in get_identities ---
    let resp = facade
        .get_identities(batch_by_name("taxonomy-batch-missing-id"))
        .await
        .unwrap();
    assert_eq!(resp.problems.len(), 1);
    assert!(
        matches!(
            &resp.problems[0].error,
            ResourceLookupProblem::NameNotFound(_)
        ),
        "get_identities: expected NameNotFound problem, got {:?}",
        resp.problems[0].error
    );

    // --- NameNotFound in render_manifests ---
    let resp = facade
        .render_manifests(
            batch_by_name("taxonomy-batch-missing-render"),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    assert_eq!(resp.problems.len(), 1);
    assert!(
        matches!(
            &resp.problems[0].error,
            ResourceLookupProblem::NameNotFound(_)
        ),
        "render_manifests: expected NameNotFound problem, got {:?}",
        resp.problems[0].error
    );

    // --- NameNotFound in delete_many ---
    let resp = facade
        .delete_many(batch_by_name("taxonomy-batch-missing-del"))
        .await
        .unwrap();
    assert_eq!(resp.problems.len(), 1);
    assert!(
        matches!(
            &resp.problems[0].error,
            ResourceLookupProblem::NameNotFound(_)
        ),
        "delete_many: expected NameNotFound problem, got {:?}",
        resp.problems[0].error
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-143: apply rejection taxonomy
//
// Verifies that representative apply failures produce stable error variants on
// both `plan_apply_manifest` and `apply_manifest`.
//
// Empty variables map is caught at spec-decode time →
// `ApplyManifestError::InvalidSpec`. This is a pre-planning validation error,
// not a planning-level Rejected decision.
contract_test!(
    apply_rejection_taxonomy,
    super::test_apply_rejection_taxonomy
);

pub async fn test_apply_rejection_taxonomy(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    // Empty variables map fails VariableSetSpec::validate() at decode time;
    // both plan and apply must return Err(InvalidSpec).
    let empty_vars_manifest = indoc::indoc!(
        r#"{
            "apiVersion": "kamu.dev/v1alpha1",
            "kind": "VariableSet",
            "metadata": {"name": "tax-biz-invalid"},
            "spec": {"variables": {}}
        }"#
    )
    .to_string();

    let plan_result = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: empty_vars_manifest.clone(),
        })
        .await;
    assert!(
        matches!(
            plan_result,
            Err(kamu_resources_facade::ApplyManifestError::InvalidSpec(_))
        ),
        "plan: expected Err(InvalidSpec), got: {plan_result:?}"
    );

    let apply_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: empty_vars_manifest,
        })
        .await;
    assert!(
        matches!(
            apply_result,
            Err(kamu_resources_facade::ApplyManifestError::InvalidSpec(_))
        ),
        "apply: expected Err(InvalidSpec), got: {apply_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
