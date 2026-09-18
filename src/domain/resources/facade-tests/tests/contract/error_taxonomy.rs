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
use kamu_resources::{ResourceAccountRef, ResourceRef, ResourceSelector, TypeName};
use kamu_resources_facade::{
    ApplyManifestError,
    ApplyManifestRequest,
    BatchResourceError,
    ListResourcesError,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourcesSummaryError,
    ResourcesSummaryRequest,
    SearchResourcesRequest,
    SpecViewOpts,
};
use pretty_assertions::assert_matches;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_SCHEMA_STR,
    assert_single_batch_problem,
    create_variable_set,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn by_name(name: &str) -> ResourceRef {
    ResourceRef {
        account: None,
        r#type: Some(
            VARIABLE_SET_CANONICAL_SELECTOR
                .parse::<TypeName>()
                .unwrap()
                .into(),
        ),
        id: None,
        did: None,
        name: Some(name.parse().unwrap()),
    }
}

fn by_id(id: &kamu_resources::ResourceID) -> ResourceRef {
    ResourceRef {
        account: None,
        r#type: Some(
            VARIABLE_SET_CANONICAL_SELECTOR
                .parse::<TypeName>()
                .unwrap()
                .into(),
        ),
        id: Some(*id),
        did: None,
        name: None,
    }
}

fn batch_by_name(name: &str) -> Vec<ResourceRef> {
    vec![by_name(name)]
}

/// A one-element batch naming `account`, for the bad-account paths.
fn batch_by_name_for_account(name: &str, account: ResourceAccountRef) -> Vec<ResourceRef> {
    vec![ResourceRef {
        account: Some(account),
        ..by_name(name)
    }]
}

fn batch_by_id(id: kamu_resources::ResourceID) -> Vec<ResourceRef> {
    vec![by_id(&id)]
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-140: single-resource lookup error taxonomy is consistent across get,
// get_handle, render_manifest, and delete.
contract_test!(
    single_resource_lookup_taxonomy,
    super::test_single_resource_lookup_taxonomy
);

pub async fn test_single_resource_lookup_taxonomy(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "taxonomy-single").await;
    let absent_uid = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    // --- NameNotFound ---
    let missing_name = "taxonomy-missing";

    let get = facade
        .get(vec![by_name(missing_name)], SpecViewOpts::ENCRYPTED)
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(get),
        ResourceLookupProblem::NameNotFound(_),
        "get: expected NameNotFound"
    );

    let get_id = facade
        .get_handles(vec![by_name(missing_name)])
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(get_id),
        ResourceLookupProblem::NameNotFound(_),
        "get_handle: expected NameNotFound"
    );

    let render = facade
        .render_manifests(
            vec![by_name(missing_name)],
            ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(render),
        ResourceLookupProblem::NameNotFound(_),
        "render_manifest: expected NameNotFound"
    );

    let del = facade.delete(vec![by_name(missing_name)]).await.unwrap();
    assert_matches!(
        assert_single_batch_problem(del),
        ResourceLookupProblem::NameNotFound(_),
        "delete: expected NameNotFound"
    );

    // --- IDNotFound ---
    let get = facade
        .get(vec![by_id(&absent_uid)], SpecViewOpts::ENCRYPTED)
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(get),
        ResourceLookupProblem::IDNotFound(_),
        "get: expected IDNotFound"
    );

    let get_id = facade.get_handles(vec![by_id(&absent_uid)]).await.unwrap();
    assert_matches!(
        assert_single_batch_problem(get_id),
        ResourceLookupProblem::IDNotFound(_),
        "get_handle: expected IDNotFound"
    );

    let render = facade
        .render_manifests(
            vec![by_id(&absent_uid)],
            ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(render),
        ResourceLookupProblem::IDNotFound(_),
        "render_manifest: expected IDNotFound"
    );

    let del = facade.delete(vec![by_id(&absent_uid)]).await.unwrap();
    assert_matches!(
        assert_single_batch_problem(del),
        ResourceLookupProblem::IDNotFound(_),
        "delete: expected IDNotFound"
    );

    // --- SchemaMismatch ---
    let wrong_schema_selector = ResourceRef {
        account: None,
        r#type: Some(
            SECRET_SET_CANONICAL_SELECTOR
                .parse::<TypeName>()
                .unwrap()
                .into(),
        ),
        id: Some(id),
        did: None,
        name: None,
    };

    let get = facade
        .get(vec![wrong_schema_selector.clone()], SpecViewOpts::ENCRYPTED)
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(get),
        ResourceLookupProblem::SchemaMismatch(_),
        "get: expected SchemaMismatch"
    );

    let get_id = facade
        .get_handles(vec![wrong_schema_selector.clone()])
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(get_id),
        ResourceLookupProblem::SchemaMismatch(_),
        "get_handle: expected SchemaMismatch"
    );

    let render = facade
        .render_manifests(
            vec![wrong_schema_selector.clone()],
            ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(render),
        ResourceLookupProblem::SchemaMismatch(_),
        "render_manifest: expected SchemaMismatch"
    );

    let del = facade.delete(vec![wrong_schema_selector]).await.unwrap();
    assert_matches!(
        assert_single_batch_problem(del),
        ResourceLookupProblem::SchemaMismatch(_),
        "delete: expected SchemaMismatch"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The remote facade has the ref rejected server-side, so it comes back as a
/// request error. Pin the reason so a transport failure cannot pass for a rule
/// being enforced.
fn assert_remote_refusal(error: &BatchResourceError, api: &str) {
    assert_matches!(
        error,
        BatchResourceError::RemoteRequest(_),
        "{api}: remote must refuse the ref rather than resolve it"
    );

    let message = error.to_string();
    assert!(
        message.contains("must also specify a `type`"),
        "{api}: refusal must name the missing type as the reason, got: {message}"
    );
}

contract_test!(
    untyped_name_ref_is_refused,
    super::test_untyped_name_ref_is_refused
);

/// A ref addressing by `name` without a `type` is refused rather than resolved:
/// `(account, type, name)` is ODF's uniqueness key and two types may hold one
/// name under the same account, so a bare name identifies nothing (RFC-018
/// § References).
///
/// A *contract* test because the two implementations refuse in different places
/// — the local facade in `resolve_ref_schema`, the remote one server-side in
/// `validate_ref` — and both must refuse, distinguishably from `EmptyRef`.
pub async fn test_untyped_name_ref_is_refused(h: &impl FacadeContractHarness) {
    // Seed the name under a real type, so a miss cannot be mistaken for the
    // resource simply not existing: the name resolves fine *with* a type.
    create_variable_set(h, TestAccount::Alice, "untyped-ref-target").await;
    let facade = h.facade_for(TestAccount::Alice);

    let untyped = ResourceRef {
        account: None,
        r#type: None,
        id: None,
        did: None,
        name: Some("untyped-ref-target".parse().unwrap()),
    };

    // --- get ---
    match facade
        .get(vec![untyped.clone()], SpecViewOpts::ENCRYPTED)
        .await
    {
        // Local: refused per item, and specifically not as `EmptyRef`.
        Ok(response) => assert_matches!(
            assert_single_batch_problem(response),
            ResourceLookupProblem::UntypedName,
            "get: a named ref without a type must be refused as UntypedName"
        ),
        // Remote: refused server-side by the GraphQL adapter's `validate_ref`,
        // so it surfaces as a request error rather than a per-item problem.
        // Assert the reason, not just the variant: a generic transport failure
        // would satisfy the shape while proving nothing about the rule.
        Err(e) => assert_remote_refusal(&e, "get"),
    }

    // --- get_handles: the same ref must not resolve through a second entry
    // point, which is what a per-pipeline check would let slip.
    match facade.get_handles(vec![untyped.clone()]).await {
        Ok(response) => assert_matches!(
            assert_single_batch_problem(response),
            ResourceLookupProblem::UntypedName,
            "get_handles: a named ref without a type must be refused as UntypedName"
        ),
        Err(e) => assert_remote_refusal(&e, "get_handles"),
    }

    // --- the same name *with* its type still resolves, so the refusal is
    // about the missing type and not about the resource being absent.
    let typed = facade
        .get_handles(vec![by_name("untyped-ref-target")])
        .await
        .unwrap();
    assert_eq!(
        typed.successes.len(),
        1,
        "the same name must still resolve when the type is supplied: {typed:#?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-141: batch lookup problem taxonomy mirrors the single-resource taxonomy.
contract_test!(batch_lookup_taxonomy, super::test_batch_lookup_taxonomy);

pub async fn test_batch_lookup_taxonomy(h: &impl FacadeContractHarness) {
    let absent_uid = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    // --- NameNotFound in get ---
    let resp = facade
        .get(
            batch_by_name("taxonomy-batch-missing"),
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();
    assert_eq!(resp.problems.len(), 1);
    assert_matches!(
        &resp.problems[0].error,
        ResourceLookupProblem::NameNotFound(_),
        "get: expected NameNotFound problem"
    );

    // --- IDNotFound in get ---
    let resp = facade
        .get(batch_by_id(absent_uid), SpecViewOpts::ENCRYPTED)
        .await
        .unwrap();
    assert_eq!(resp.problems.len(), 1);
    assert_matches!(
        &resp.problems[0].error,
        ResourceLookupProblem::IDNotFound(_),
        "get: expected IDNotFound problem"
    );

    // --- NameNotFound in get_handles ---
    let resp = facade
        .get_handles(batch_by_name("taxonomy-batch-missing-id"))
        .await
        .unwrap();
    assert_eq!(resp.problems.len(), 1);
    assert_matches!(
        &resp.problems[0].error,
        ResourceLookupProblem::NameNotFound(_),
        "get_handles: expected NameNotFound problem"
    );

    // --- NameNotFound in render_manifests ---
    let resp = facade
        .render_manifests(
            batch_by_name("taxonomy-batch-missing-render"),
            ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();
    assert_eq!(resp.problems.len(), 1);
    assert_matches!(
        &resp.problems[0].error,
        ResourceLookupProblem::NameNotFound(_),
        "render_manifests: expected NameNotFound problem"
    );

    // --- NameNotFound in delete ---
    let resp = facade
        .delete(batch_by_name("taxonomy-batch-missing-del"))
        .await
        .unwrap();
    assert_eq!(resp.problems.len(), 1);
    assert_matches!(
        &resp.problems[0].error,
        ResourceLookupProblem::NameNotFound(_),
        "delete: expected NameNotFound problem"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-142: account-resolution errors surface as typed `AccountResolution`
// outcomes from every account-accepting API. A gap on one outcome union would
// demote them there to an internal error.
contract_test!(
    account_resolution_taxonomy,
    super::test_account_resolution_taxonomy
);

pub async fn test_account_resolution_taxonomy(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let unknown_account = ResourceAccountRef {
        id: None,
        did: None,
        name: Some(odf::AccountName::new_unchecked(
            "unknown-resource-contract-account",
        )),
    };

    // --- get ---
    let result = facade
        .get(
            vec![ResourceRef {
                account: Some(unknown_account.clone()),
                r#type: Some(
                    VARIABLE_SET_CANONICAL_SELECTOR
                        .parse::<TypeName>()
                        .unwrap()
                        .into(),
                ),
                id: None,
                did: None,
                name: Some("bad-acct-get".parse().unwrap()),
            }],
            SpecViewOpts::ENCRYPTED,
        )
        .await;
    assert_matches!(
        result,
        Err(BatchResourceError::AccountResolution(_)),
        "get: expected AccountResolution"
    );

    // --- get, multi-ref ---
    let result = facade
        .get(
            batch_by_name_for_account("bad-acct-get-many", unknown_account.clone()),
            SpecViewOpts::ENCRYPTED,
        )
        .await;
    assert_matches!(
        result,
        Err(BatchResourceError::AccountResolution(_)),
        "get with several refs: expected AccountResolution"
    );

    // --- render_manifests ---
    let result = facade
        .render_manifests(
            vec![ResourceRef {
                account: Some(unknown_account.clone()),
                r#type: Some(
                    VARIABLE_SET_CANONICAL_SELECTOR
                        .parse::<TypeName>()
                        .unwrap()
                        .into(),
                ),
                id: None,
                did: None,
                name: Some("bad-acct-render".parse().unwrap()),
            }],
            ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
        )
        .await;
    assert_matches!(
        result,
        Err(BatchResourceError::AccountResolution(_)),
        "render_manifests: expected AccountResolution"
    );

    // --- delete ---
    let result = facade
        .delete(vec![ResourceRef {
            account: Some(unknown_account.clone()),
            r#type: Some(
                VARIABLE_SET_CANONICAL_SELECTOR
                    .parse::<TypeName>()
                    .unwrap()
                    .into(),
            ),
            id: None,
            did: None,
            name: Some("bad-acct-delete".parse().unwrap()),
        }])
        .await;
    assert_matches!(
        result,
        Err(BatchResourceError::AccountResolution(_)),
        "delete: expected AccountResolution"
    );

    // --- delete, multi-ref ---
    let result = facade
        .delete(batch_by_name_for_account(
            "bad-acct-delete-many",
            unknown_account.clone(),
        ))
        .await;
    assert_matches!(
        result,
        Err(BatchResourceError::AccountResolution(_)),
        "delete: expected AccountResolution"
    );

    // --- search ---
    let result = facade
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                account: Some(unknown_account.clone()),
                ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
            }],
            pagination: PaginationOpts::from_max_results(1),
        })
        .await;
    assert_matches!(
        result,
        Err(ListResourcesError::AccountResolution(_)),
        "search: expected AccountResolution"
    );

    // --- search_handles ---
    let result = facade
        .search_handles(SearchResourcesRequest {
            pagination: PaginationOpts::from_max_results(1),
            selectors: vec![ResourceSelector {
                account: Some(unknown_account.clone()),
                ..ResourceSelector::default()
            }],
        })
        .await;
    assert_matches!(
        result,
        Err(ListResourcesError::AccountResolution(_)),
        "search_handles: expected AccountResolution"
    );

    // --- summary ---
    let result = facade
        .summary(ResourcesSummaryRequest {
            account: Some(unknown_account.clone()),
        })
        .await;
    assert_matches!(
        result,
        Err(ResourcesSummaryError::AccountResolution(_)),
        "summary: expected AccountResolution"
    );

    // --- apply_manifest ---
    let result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: serde_json::json!({
                "$schema": VARIABLE_SET_SCHEMA_STR,
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
        Err(ApplyManifestError::AccountResolution(_)),
        "apply_manifest: expected AccountResolution"
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

    let empty_vars_manifest = serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA_STR,
        "headers": {"name": "tax-biz-invalid"},
        "spec": {"variables": {}}
    })
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
