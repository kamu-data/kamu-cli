// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Error consistency tests (RF-140..142..143).
//!
//! RF-140: equivalent lookup failures produce equivalent
//! `ResourceLookupProblem` variants across all single-resource APIs.
//! RF-141: batch APIs mirror the same per-item problem taxonomy.
//! RF-142: bad-account errors are returned as typed outcomes from every
//! account-accepting API (not demoted to internal errors).

use database_common::PaginationOpts;
use kamu_resources::{ApplyResourceOutcome, ResourceManifestAccount};
use kamu_resources_facade::{
    ApplyManifestError,
    ApplyManifestRequest,
    BatchResourceError,
    DeleteResourceError,
    GetResourceError,
    ListAllResourcesError,
    ListAllResourcesRequest,
    ListResourcesError,
    ListResourcesRequest,
    RenderResourceManifestError,
    ResourceBatchSelector,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourceRef,
    ResourceSelector,
    ResourcesSummaryError,
    ResourcesSummaryRequest,
    SpecViewMode,
};
use pretty_assertions::assert_matches;

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
        .headers
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
    assert_matches!(
        get,
        Err(GetResourceError::LookupProblem(
            ResourceLookupProblem::NameNotFound(_)
        )),
        "get: expected NameNotFound"
    );

    let get_id = facade.get_identity(by_name(missing_name)).await;
    assert_matches!(
        get_id,
        Err(GetResourceError::LookupProblem(
            ResourceLookupProblem::NameNotFound(_)
        )),
        "get_identity: expected NameNotFound"
    );

    let render = facade
        .render_manifest(
            by_name(missing_name),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert_matches!(
        render,
        Err(RenderResourceManifestError::LookupProblem(
            ResourceLookupProblem::NameNotFound(_)
        )),
        "render_manifest: expected NameNotFound"
    );

    let del = facade.delete(by_name(missing_name)).await;
    assert_matches!(
        del,
        Err(DeleteResourceError::LookupProblem(
            ResourceLookupProblem::NameNotFound(_)
        )),
        "delete: expected NameNotFound"
    );

    // --- UIDNotFound ---
    let get = facade
        .get(by_id(&absent_uid), SpecViewMode::Encrypted)
        .await;
    assert_matches!(
        get,
        Err(GetResourceError::LookupProblem(
            ResourceLookupProblem::UIDNotFound(_)
        )),
        "get: expected UIDNotFound"
    );

    let get_id = facade.get_identity(by_id(&absent_uid)).await;
    assert_matches!(
        get_id,
        Err(GetResourceError::LookupProblem(
            ResourceLookupProblem::UIDNotFound(_)
        )),
        "get_identity: expected UIDNotFound"
    );

    let render = facade
        .render_manifest(
            by_id(&absent_uid),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert_matches!(
        render,
        Err(RenderResourceManifestError::LookupProblem(
            ResourceLookupProblem::UIDNotFound(_)
        )),
        "render_manifest: expected UIDNotFound"
    );

    let del = facade.delete(by_id(&absent_uid)).await;
    assert_matches!(
        del,
        Err(DeleteResourceError::LookupProblem(
            ResourceLookupProblem::UIDNotFound(_)
        )),
        "delete: expected UIDNotFound"
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
    assert_matches!(
        get,
        Err(GetResourceError::LookupProblem(
            ResourceLookupProblem::ApiVersionMismatch(_)
        )),
        "get: expected ApiVersionMismatch"
    );

    let get_id = facade.get_identity(wrong_version.clone()).await;
    assert_matches!(
        get_id,
        Err(GetResourceError::LookupProblem(
            ResourceLookupProblem::ApiVersionMismatch(_)
        )),
        "get_identity: expected ApiVersionMismatch"
    );

    let render = facade
        .render_manifest(
            wrong_version.clone(),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert_matches!(
        render,
        Err(RenderResourceManifestError::LookupProblem(
            ResourceLookupProblem::ApiVersionMismatch(_)
        )),
        "render_manifest: expected ApiVersionMismatch"
    );

    let del = facade.delete(wrong_version).await;
    assert_matches!(
        del,
        Err(DeleteResourceError::LookupProblem(
            ResourceLookupProblem::ApiVersionMismatch(_)
        )),
        "delete: expected ApiVersionMismatch"
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
    assert_matches!(
        &resp.problems[0].error,
        ResourceLookupProblem::NameNotFound(_),
        "get_many: expected NameNotFound problem"
    );

    // --- UIDNotFound in get_many ---
    let resp = facade
        .get_many(batch_by_id(absent_uid), SpecViewMode::Encrypted)
        .await
        .unwrap();
    assert_eq!(resp.problems.len(), 1);
    assert_matches!(
        &resp.problems[0].error,
        ResourceLookupProblem::UIDNotFound(_),
        "get_many: expected UIDNotFound problem"
    );

    // --- NameNotFound in get_identities ---
    let resp = facade
        .get_identities(batch_by_name("taxonomy-batch-missing-id"))
        .await
        .unwrap();
    assert_eq!(resp.problems.len(), 1);
    assert_matches!(
        &resp.problems[0].error,
        ResourceLookupProblem::NameNotFound(_),
        "get_identities: expected NameNotFound problem"
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
    assert_matches!(
        &resp.problems[0].error,
        ResourceLookupProblem::NameNotFound(_),
        "render_manifests: expected NameNotFound problem"
    );

    // --- NameNotFound in delete_many ---
    let resp = facade
        .delete_many(batch_by_name("taxonomy-batch-missing-del"))
        .await
        .unwrap();
    assert_eq!(resp.problems.len(), 1);
    assert_matches!(
        &resp.problems[0].error,
        ResourceLookupProblem::NameNotFound(_),
        "delete_many: expected NameNotFound problem"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-142: bad-account errors surface as typed `BadAccount` outcomes from every
// account-accepting API — not as internal errors. This test is specifically
// designed to catch GraphQL schema gaps like the single-delete case where
// `ResourceBadAccountProblem` was missing from `ResourceDeleteOutcome`.
contract_test!(bad_account_taxonomy, super::test_bad_account_taxonomy);

pub async fn test_bad_account_taxonomy(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let unknown_account = ResourceManifestAccount {
        name: Some("unknown-resource-contract-account".to_string()),
        id: None,
    };

    // --- get ---
    let result = facade
        .get(
            ResourceSelector {
                account: Some(unknown_account.clone()),
                kind: VARIABLE_SET_KIND.to_string(),
                api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
                resource_ref: ResourceRef::ByName("bad-acct-get".to_string()),
            },
            SpecViewMode::Encrypted,
        )
        .await;
    assert_matches!(
        result,
        Err(GetResourceError::BadAccount(_)),
        "get: expected BadAccount"
    );

    // --- get_many ---
    let result = facade
        .get_many(
            ResourceBatchSelector {
                account: Some(unknown_account.clone()),
                kind: VARIABLE_SET_KIND.to_string(),
                api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
                resource_refs: vec![ResourceRef::ByName("bad-acct-get-many".to_string())],
            },
            SpecViewMode::Encrypted,
        )
        .await;
    assert_matches!(
        result,
        Err(BatchResourceError::BadAccount(_)),
        "get_many: expected BadAccount"
    );

    // --- render_manifest ---
    let result = facade
        .render_manifest(
            ResourceSelector {
                account: Some(unknown_account.clone()),
                kind: VARIABLE_SET_KIND.to_string(),
                api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
                resource_ref: ResourceRef::ByName("bad-acct-render".to_string()),
            },
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert_matches!(
        result,
        Err(RenderResourceManifestError::BadAccount(_)),
        "render_manifest: expected BadAccount"
    );

    // --- delete ---
    let result = facade
        .delete(ResourceSelector {
            account: Some(unknown_account.clone()),
            kind: VARIABLE_SET_KIND.to_string(),
            api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
            resource_ref: ResourceRef::ByName("bad-acct-delete".to_string()),
        })
        .await;
    assert_matches!(
        result,
        Err(DeleteResourceError::BadAccount(_)),
        "delete: expected BadAccount"
    );

    // --- delete_many ---
    let result = facade
        .delete_many(ResourceBatchSelector {
            account: Some(unknown_account.clone()),
            kind: VARIABLE_SET_KIND.to_string(),
            api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
            resource_refs: vec![ResourceRef::ByName("bad-acct-delete-many".to_string())],
        })
        .await;
    assert_matches!(
        result,
        Err(BatchResourceError::BadAccount(_)),
        "delete_many: expected BadAccount"
    );

    // --- list ---
    let result = facade
        .list(ListResourcesRequest {
            account: Some(unknown_account.clone()),
            kind: VARIABLE_SET_KIND.to_string(),
            pagination: PaginationOpts::from_max_results(1),
        })
        .await;
    assert_matches!(
        result,
        Err(ListResourcesError::BadAccount(_)),
        "list: expected BadAccount"
    );

    // --- list_all ---
    let result = facade
        .list_all(ListAllResourcesRequest {
            account: Some(unknown_account.clone()),
            pagination: PaginationOpts::from_max_results(1),
        })
        .await;
    assert_matches!(
        result,
        Err(ListAllResourcesError::BadAccount(_)),
        "list_all: expected BadAccount"
    );

    // --- summary ---
    let result = facade
        .summary(ResourcesSummaryRequest {
            account: Some(unknown_account.clone()),
        })
        .await;
    assert_matches!(
        result,
        Err(ResourcesSummaryError::BadAccount(_)),
        "summary: expected BadAccount"
    );

    // --- apply_manifest ---
    let result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: serde_json::json!({
                "apiVersion": VARIABLE_SET_API_VERSION,
                "kind": VARIABLE_SET_KIND,
                "headers": {
                    "name": "bad-acct-apply",
                    "account": { "name": "unknown-resource-contract-account" }
                },
                "spec": {"variables": {"KEY": {"value": "val"}}}
            })
            .to_string(),
        })
        .await;
    assert_matches!(
        result,
        Err(ApplyManifestError::BadAccount(_)),
        "apply_manifest: expected BadAccount"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-143: apply rejection taxonomy
//
// Verifies that representative apply failures produce stable error variants on
// both `plan_apply_manifest` and `apply_manifest`.
//
// Empty variables map deserializes successfully but fails
// VariableSetSpec::validate() inside the lifecycle →
// Ok(Rejected(BusinessValidationFailed)).
contract_test!(
    apply_rejection_taxonomy,
    super::test_apply_rejection_taxonomy
);

pub async fn test_apply_rejection_taxonomy(h: &impl FacadeContractHarness) {
    use kamu_resources::{
        ApplyManifestApplicationDecision,
        ApplyManifestPlanningDecision,
        ApplyManifestRejection,
        ApplyResourceRejectionCategory,
    };

    let facade = h.facade_for(TestAccount::Alice);

    // Empty variables map deserializes correctly but fails
    // VariableSetSpec::validate() inside the lifecycle; both plan and apply
    // return Ok(Rejected(BusinessValidationFailed)).
    let empty_vars_manifest = indoc::indoc!(
        r#"{
            "apiVersion": "kamu.dev/v1alpha1",
            "kind": "VariableSet",
            "headers": {"name": "tax-biz-invalid"},
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
    assert_matches!(
        plan_result,
        Ok(ApplyManifestPlanningDecision::Rejected(
            ApplyManifestRejection {
                category: ApplyResourceRejectionCategory::BusinessValidationFailed,
                ..
            }
        )),
        "plan: expected Ok(Rejected(BusinessValidationFailed))"
    );

    let apply_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: empty_vars_manifest,
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
        "apply: expected Ok(Rejected(BusinessValidationFailed))"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
