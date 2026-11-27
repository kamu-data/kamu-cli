// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_resources::{
    ApplyManifestApplicationDecision,
    ApplyResourceOutcome,
    ApplyResourceRejectionCategory,
    Resource,
    ResourceID,
    ResourceRef,
    ResourceSelector,
    ResourceVisibility,
    TypeName,
    TypeRef,
};
use kamu_resources_facade::{
    ApplyManifestError,
    ApplyManifestRequest,
    BatchResourceError,
    ListResourcesError,
    ResourceLookupProblem,
    ResourceManifestFormat,
    SearchResourcesRequest,
    SpecViewOpts,
};
use kamu_resources_services::testing::TestResource;
use messaging_outbox::OutboxProvider;
use pretty_assertions::{assert_eq, assert_matches};
use uuid::Uuid;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, LocalFacadeHarnessOpts, TestAccount};
use crate::helpers::{
    VARIABLE_SET_CANONICAL_SELECTOR,
    assert_access_denied,
    assert_plan_and_apply_rejected,
    assert_single_batch_problem,
    create_variable_set,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-199
contract_test!(
    placement_puts_a_non_owners_resource_in_the_placed_account,
    super::test_placement_puts_a_non_owners_resource_in_the_placed_account,
    opts = super::LocalFacadeHarnessOpts::with_guarded_test_resources()
);

pub async fn test_placement_puts_a_non_owners_resource_in_the_placed_account(
    h: &impl FacadeContractHarness,
) {
    let bob = h.account_name(TestAccount::Bob);
    let value = format!("place:{bob}");

    // Without an account, and naming the placed one
    for account in [None, Some(&bob)] {
        let name = format!("placed-{}", account.is_some());
        let result = apply(h, TestAccount::Alice, account, &name, &value).await;

        let ApplyManifestApplicationDecision::Applied(result) = result.unwrap() else {
            panic!("expected the apply to be applied");
        };
        assert_eq!(result.outcome, ApplyResourceOutcome::Created);
        assert_eq!(result.resource.headers.account.name, bob);
        assert_eq!(get(h, TestAccount::Bob, None, &name).await.len(), 1);

        // Applied again, the manifest finds the resource in the placed account
        let reapplied = apply(h, TestAccount::Alice, account, &name, &value)
            .await
            .unwrap()
            .expect_applied();
        assert_eq!(reapplied.outcome, ApplyResourceOutcome::Untouched);
        let updated = apply(
            h,
            TestAccount::Alice,
            account,
            &name,
            &format!("{value} changed"),
        )
        .await
        .unwrap()
        .expect_applied();
        assert_eq!(updated.outcome, ApplyResourceOutcome::Updated);
        assert_eq!(updated.resource.headers.id, result.resource.headers.id);
        assert_eq!(updated.resource.headers.account.name, bob);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-200
contract_test!(
    manifest_naming_another_account_than_the_placement_is_rejected,
    super::test_manifest_naming_another_account_than_the_placement_is_rejected,
    opts = super::LocalFacadeHarnessOpts::with_guarded_test_resources()
);

pub async fn test_manifest_naming_another_account_than_the_placement_is_rejected(
    h: &impl FacadeContractHarness,
) {
    let alice = h.account_name(TestAccount::Alice);
    let value = format!("place:{}", h.account_name(TestAccount::Bob));

    let result = apply(h, TestAccount::Alice, Some(&alice), "misplaced", &value).await;

    let ApplyManifestApplicationDecision::Rejected(rejection) = result.unwrap() else {
        panic!("expected the apply to be rejected");
    };
    assert_eq!(
        rejection.category,
        ApplyResourceRejectionCategory::BusinessValidationFailed
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-201
contract_test!(
    guard_decides_create_in_another_account,
    super::test_guard_decides_create_in_another_account,
    opts = super::LocalFacadeHarnessOpts::with_guarded_test_resources()
);

pub async fn test_guard_decides_create_in_another_account(h: &impl FacadeContractHarness) {
    let bob = h.account_name(TestAccount::Bob);

    let result = apply(h, TestAccount::Alice, Some(&bob), "unplaced", "plain").await;

    assert_apply_access_denied(&result.unwrap_err());

    // The guard decides before the ID is looked up: one naming nothing, or a
    // resource of another type, gets the same denial
    let variable_set = create_variable_set(h, TestAccount::Bob, "variables").await;
    let facade = h.facade_for(TestAccount::Alice);
    for id in [ResourceID::new(Uuid::new_v4()), variable_set] {
        let request = manifest_request(
            Some(&bob),
            "unplaced",
            "plain",
            serde_json::json!({ "id": id }),
        );

        let planned = facade.plan_apply_manifest(request.clone()).await;
        assert_apply_access_denied(&planned.unwrap_err());
        let applied = facade.apply_manifest(request).await;
        assert_apply_access_denied(&applied.unwrap_err());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-209
contract_test!(
    guard_decides_update_by_the_current_resource,
    super::test_guard_decides_update_by_the_current_resource,
    opts = super::LocalFacadeHarnessOpts::with_guarded_test_resources()
);

pub async fn test_guard_decides_update_by_the_current_resource(h: &impl FacadeContractHarness) {
    let bob = h.account_name(TestAccount::Bob);
    let placed_value = format!("place:{bob}");
    create(h, TestAccount::Bob, "unplaced", "plain").await;
    let placed = create(h, TestAccount::Bob, "placed", &placed_value).await;

    // The new spec alone would be allowed
    let denied = apply(h, TestAccount::Alice, Some(&bob), "unplaced", &placed_value).await;
    assert_apply_access_denied(&denied.unwrap_err());
    assert_eq!(
        get(h, TestAccount::Bob, None, "unplaced").await[0].spec,
        serde_json::json!({ "value": "plain" })
    );

    let updated = apply(
        h,
        TestAccount::Alice,
        Some(&bob),
        "placed",
        &format!("{placed_value} changed"),
    )
    .await
    .unwrap()
    .expect_applied();
    assert_eq!(updated.outcome, ApplyResourceOutcome::Updated);
    assert_eq!(updated.resource.headers.id, placed);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-210
contract_test!(
    placement_rejection_rejects_plan_and_apply,
    super::test_placement_rejection_rejects_plan_and_apply,
    opts = super::LocalFacadeHarnessOpts::with_guarded_test_resources()
);

pub async fn test_placement_rejection_rejects_plan_and_apply(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let message = assert_plan_and_apply_rejected(
        facade.as_ref(),
        manifest_request(None, "unplaceable", "place:nobody", serde_json::json!({})),
        ApplyResourceRejectionCategory::ReferencedObjectMissing,
    )
    .await;

    assert!(message.contains("nobody"), "{message}");
    assert_eq!(
        get(h, TestAccount::Alice, None, "unplaceable").await.len(),
        0
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-202
contract_test!(
    get_in_another_account_is_decided_per_resource,
    super::test_get_in_another_account_is_decided_per_resource,
    opts = super::LocalFacadeHarnessOpts::with_guarded_test_resources()
);

pub async fn test_get_in_another_account_is_decided_per_resource(h: &impl FacadeContractHarness) {
    let alice = h.account_name(TestAccount::Alice);
    let bob = h.account_name(TestAccount::Bob);
    let shared = create(h, TestAccount::Bob, "shared", &format!("read:{alice}")).await;
    let private = create(h, TestAccount::Bob, "private", "plain").await;

    // One resource named twice is checked once and answered for both refs
    let response = h
        .facade_for(TestAccount::Alice)
        .get(
            vec![
                by_name(Some(&bob), "shared"),
                by_name(Some(&bob), "private"),
                by_id(&bob, shared),
            ],
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await
        .unwrap();

    assert_eq!(
        response
            .successes
            .iter()
            .map(|success| (success.request_index, success.item.headers.name.as_str()))
            .collect::<Vec<_>>(),
        [(0, "shared"), (2, "shared")]
    );
    assert_eq!(response.problems.len(), 1);
    assert_eq!(response.problems[0].request_index, 1);
    assert_matches!(
        response.problems[0].error,
        ResourceLookupProblem::NameNotFound(_)
    );

    // A hidden ID, or one of another type, answers as one naming nothing
    let variable_set = create_variable_set(h, TestAccount::Bob, "variables").await;
    let missing = ResourceID::new(Uuid::new_v4());
    let problems = get_problems(
        h,
        TestAccount::Alice,
        vec![
            by_id(&bob, missing),
            by_id(&bob, private),
            by_id(&bob, variable_set),
        ],
    )
    .await;
    let [
        (0, missing_problem @ ResourceLookupProblem::IDNotFound(_)),
        (1, private_problem @ ResourceLookupProblem::IDNotFound(_)),
        (2, variable_set_problem @ ResourceLookupProblem::IDNotFound(_)),
    ] = &problems[..]
    else {
        panic!("unexpected problems: {problems:?}");
    };
    let missing_message = |id: ResourceID| {
        missing_problem
            .to_string()
            .replace(&missing.to_string(), &id.to_string())
    };
    assert_eq!(private_problem.to_string(), missing_message(private));
    assert_eq!(
        variable_set_problem.to_string(),
        missing_message(variable_set)
    );

    // A ref without a type keeps the default rule, even for a resource the
    // type's guard lets the caller read
    let err = h
        .facade_for(TestAccount::Alice)
        .get(
            vec![ResourceRef {
                r#type: None,
                ..by_id(&bob, shared)
            }],
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await
        .unwrap_err();
    assert_matches!(
        err,
        BatchResourceError::AccountAccess(_) | BatchResourceError::RemoteRequest(_)
    );
    assert_access_denied(&err);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-203
contract_test!(
    delete_in_another_account_is_decided_per_resource,
    super::test_delete_in_another_account_is_decided_per_resource,
    opts = super::LocalFacadeHarnessOpts::with_guarded_test_resources()
);

pub async fn test_delete_in_another_account_is_decided_per_resource(
    h: &impl FacadeContractHarness,
) {
    let alice = h.account_name(TestAccount::Alice);
    let bob = h.account_name(TestAccount::Bob);
    create(h, TestAccount::Bob, "deletable", &format!("delete:{alice}")).await;
    let readable = create(h, TestAccount::Bob, "readable", &format!("read:{alice}")).await;
    create(h, TestAccount::Bob, "private", "plain").await;

    let mut response = h
        .facade_for(TestAccount::Alice)
        .delete(vec![
            by_name(Some(&bob), "deletable"),
            by_name(Some(&bob), "readable"),
            by_name(Some(&bob), "private"),
            by_id(&bob, readable),
        ])
        .await
        .unwrap();

    assert_eq!(
        response
            .successes
            .iter()
            .map(|success| success.request_index)
            .collect::<Vec<_>>(),
        [0]
    );
    response
        .problems
        .sort_by_key(|problem| problem.request_index);
    let problems = response
        .problems
        .iter()
        .map(|problem| (problem.request_index, &problem.error))
        .collect::<Vec<_>>();
    // The remote facade gets the kind and the detail of the denial too
    let [
        (1, ResourceLookupProblem::Access(odf::AccessError::Unauthorized(by_name))),
        (2, ResourceLookupProblem::NameNotFound(_)),
        (3, ResourceLookupProblem::Access(odf::AccessError::Unauthorized(denial_by_id))),
    ] = problems[..]
    else {
        panic!("unexpected problems: {problems:?}");
    };
    let denial = format!("Current subject is not allowed to use resources of account '{bob}'");
    assert_eq!(by_name.to_string(), denial);
    assert_eq!(denial_by_id.to_string(), denial);
    assert_eq!(get(h, TestAccount::Bob, None, "readable").await.len(), 1);
    assert_eq!(get(h, TestAccount::Bob, None, "private").await.len(), 1);

    // A deleted resource is not found, even by one who may read but not delete it
    delete(h, TestAccount::Bob, readable).await;
    h.settle().await;
    let response = h
        .facade_for(TestAccount::Alice)
        .delete(vec![by_id(&bob, readable)])
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(response),
        ResourceLookupProblem::IDNotFound(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-204
contract_test!(
    listing_another_account_needs_the_guards_filter,
    super::test_listing_another_account_needs_the_guards_filter,
    opts = super::LocalFacadeHarnessOpts::with_guarded_test_resources()
);

pub async fn test_listing_another_account_needs_the_guards_filter(h: &impl FacadeContractHarness) {
    let bob = h.account_name(TestAccount::Bob);
    create(h, TestAccount::Bob, "public-listed", "plain").await;
    create(h, TestAccount::Bob, "private", "plain").await;

    let facade = h.facade_for(TestAccount::Alice);
    let search = |selector: ResourceSelector| {
        facade.search(SearchResourcesRequest::new(
            vec![ResourceSelector {
                account: Some(bob.clone().into()),
                ..selector
            }],
            PaginationOpts::from_max_results(100),
        ))
    };

    // A selector without a type keeps the default rule. A pattern that may
    // match what the caller may not read is refused
    for selector in [
        ResourceSelector::of_type(test_resource_type()),
        ResourceSelector::default(),
        ResourceSelector::name_pattern(test_resource_type(), "%"),
    ] {
        let err = search(selector).await.unwrap_err();
        assert_matches!(
            err,
            ListResourcesError::AccountAccess(_) | ListResourcesError::RemoteRequest(_)
        );
        assert_access_denied(&err);
    }

    let listed = search(ResourceSelector::name_pattern(
        test_resource_type(),
        "public-%",
    ))
    .await
    .unwrap();
    assert_eq!(
        listed
            .items
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        ["public-listed"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-205
contract_test!(
    name_matching_hidden_resources_answers_only_with_readable_ones,
    super::test_name_matching_hidden_resources_answers_only_with_readable_ones,
    opts = super::LocalFacadeHarnessOpts {
        outbox_provider: super::OutboxProvider::Dispatching,
        ..super::LocalFacadeHarnessOpts::with_guarded_test_resources()
    }
);

pub async fn test_name_matching_hidden_resources_answers_only_with_readable_ones(
    h: &impl FacadeContractHarness,
) {
    let alice = h.account_name(TestAccount::Alice);
    let bob = h.account_name(TestAccount::Bob);
    // While a resource is being deleted, its name can find a new one too
    let readable = create(h, TestAccount::Bob, "twin", &format!("read:{alice}")).await;
    delete(h, TestAccount::Bob, readable).await;
    create(h, TestAccount::Bob, "twin", "plain").await;
    for _ in 0..2 {
        let hidden = create(h, TestAccount::Bob, "hidden", "plain").await;
        delete(h, TestAccount::Bob, hidden).await;
    }

    let refs = vec![by_name(Some(&bob), "twin"), by_name(Some(&bob), "hidden")];
    let facade = h.facade_for(TestAccount::Alice);
    let got = facade
        .get(
            refs.clone(),
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::IncludeDeleting,
        )
        .await
        .unwrap();
    let handles = facade
        .get_handles(refs, ResourceVisibility::IncludeDeleting)
        .await
        .unwrap();

    // The hidden resources are not counted: one name not found, not two
    assert_eq!(
        got.successes
            .iter()
            .map(|success| (success.request_index, success.item.headers.id))
            .collect::<Vec<_>>(),
        [(0, readable)]
    );
    assert_eq!(
        handles
            .successes
            .iter()
            .map(|success| (success.request_index, success.item.id))
            .collect::<Vec<_>>(),
        [(0, readable)]
    );
    for problems in [&got.problems, &handles.problems] {
        assert_matches!(
            problems
                .iter()
                .map(|problem| (problem.request_index, &problem.error))
                .collect::<Vec<_>>()[..],
            [(1, ResourceLookupProblem::NameNotFound(_))]
        );
    }
}

// RF-206
contract_test!(
    owners_the_caller_may_not_read_are_not_found,
    super::test_owners_the_caller_may_not_read_are_not_found,
    opts = super::LocalFacadeHarnessOpts::with_guarded_test_resources()
);

pub async fn test_owners_the_caller_may_not_read_are_not_found(h: &impl FacadeContractHarness) {
    let alice = h.account_name(TestAccount::Alice);
    let bob = h.account_name(TestAccount::Bob);
    let shared = create(h, TestAccount::Bob, "shared", &format!("read:{alice}")).await;
    let private = create(h, TestAccount::Bob, "private", "plain").await;
    let deleted = create(h, TestAccount::Bob, "deleted", "plain").await;
    delete(h, TestAccount::Bob, deleted).await;
    let facade = h.facade_for(TestAccount::Alice);
    let owned_request = |owner_references: serde_json::Value| {
        manifest_request(
            Some(&bob),
            "owned",
            &format!("place:{bob}"),
            serde_json::json!({ "ownerReferences": owner_references }),
        )
    };
    let by_short_name =
        |name: &str| format!("{}:{name}", TestResource::CANONICAL_SELECTOR_NAME_STR);

    // A readable owner is accepted, and checked against the reference
    assert_plan_and_apply_rejected(
        facade.as_ref(),
        owned_request(serde_json::json!([{ "id": shared, "name": "another" }])),
        ApplyResourceRejectionCategory::BusinessValidationFailed,
    )
    .await;
    let applied = facade
        .apply_manifest(owned_request(serde_json::json!([{ "id": shared }])))
        .await
        .unwrap();
    assert_matches!(applied, ApplyManifestApplicationDecision::Applied(_));

    // An unreadable one is rejected as a missing one is, whatever the
    // reference says about it
    let rejected_as_missing = async |owner_references: serde_json::Value| {
        assert_plan_and_apply_rejected(
            facade.as_ref(),
            owned_request(owner_references),
            ApplyResourceRejectionCategory::ReferencedObjectMissing,
        )
        .await
    };
    let missing_id = ResourceID::new(Uuid::new_v4());
    let missing_by_id = rejected_as_missing(serde_json::json!([{ "id": missing_id }])).await;
    let missing_by_name = rejected_as_missing(serde_json::json!([by_short_name("missing")])).await;
    let missing_id_message =
        |id: ResourceID| missing_by_id.replace(&missing_id.to_string(), &id.to_string());
    // An owner of a type whose guard refuses the whole account
    let variable_set = create_variable_set(h, TestAccount::Bob, "variables").await;
    let variable_set_by_name = |name: &str| format!("{VARIABLE_SET_CANONICAL_SELECTOR}:{name}");
    let missing_variable_set_by_name =
        rejected_as_missing(serde_json::json!([variable_set_by_name("missing")])).await;

    for (owner_references, expected) in [
        (
            serde_json::json!([{ "id": private }]),
            missing_id_message(private),
        ),
        (
            serde_json::json!([{ "id": private, "name": "another" }]),
            missing_id_message(private),
        ),
        (
            serde_json::json!([{ "id": private, "type": "AnotherType" }]),
            missing_id_message(private),
        ),
        (
            serde_json::json!([{ "id": deleted }]),
            missing_id_message(deleted),
        ),
        (
            serde_json::json!([by_short_name("private")]),
            missing_by_name.replace("missing", "private"),
        ),
        (
            serde_json::json!([{ "id": private }, by_short_name("private")]),
            missing_id_message(private),
        ),
        (
            serde_json::json!([{ "id": variable_set }]),
            missing_id_message(variable_set),
        ),
        (
            serde_json::json!([variable_set_by_name("variables")]),
            missing_variable_set_by_name.replace("missing", "variables"),
        ),
    ] {
        assert_eq!(rejected_as_missing(owner_references).await, expected);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Plan or apply fails as an account access denial
#[track_caller]
fn assert_apply_access_denied(err: &ApplyManifestError) {
    assert_matches!(
        err,
        ApplyManifestError::AccountAccess(_) | ApplyManifestError::RemoteRequest(_)
    );
    assert_access_denied(err);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn test_resource_type() -> TypeRef {
    TestResource::CANONICAL_SELECTOR_NAME_STR
        .parse::<TypeName>()
        .unwrap()
        .into()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn by_name(account: Option<&odf::AccountName>, name: &str) -> ResourceRef {
    ResourceRef {
        account: account.map(|account| account.clone().into()),
        r#type: Some(test_resource_type()),
        id: None,
        did: None,
        name: Some(name.parse().unwrap()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn by_id(account: &odf::AccountName, id: ResourceID) -> ResourceRef {
    ResourceRef {
        account: Some(account.clone().into()),
        r#type: Some(test_resource_type()),
        id: Some(id),
        did: None,
        name: None,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A manifest of `TestResource`, with `extra_headers` added to its headers
fn manifest_request(
    account: Option<&odf::AccountName>,
    name: &str,
    value: &str,
    extra_headers: serde_json::Value,
) -> ApplyManifestRequest {
    let mut headers = extra_headers;
    headers["name"] = serde_json::json!(name);
    if let Some(account) = account {
        headers["account"] = serde_json::json!({ "name": account.as_str() });
    }

    ApplyManifestRequest {
        format: ResourceManifestFormat::Json,
        manifest: serde_json::json!({
            "$schema": TestResource::SCHEMA_STR,
            "headers": headers,
            "spec": { "value": value },
        })
        .to_string(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn apply(
    h: &impl FacadeContractHarness,
    actor: TestAccount,
    account: Option<&odf::AccountName>,
    name: &str,
    value: &str,
) -> Result<ApplyManifestApplicationDecision, ApplyManifestError> {
    h.facade_for(actor)
        .apply_manifest(manifest_request(
            account,
            name,
            value,
            serde_json::json!({}),
        ))
        .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create(
    h: &impl FacadeContractHarness,
    owner: TestAccount,
    name: &str,
    value: &str,
) -> ResourceID {
    let ApplyManifestApplicationDecision::Applied(result) =
        apply(h, owner, None, name, value).await.unwrap()
    else {
        panic!("expected {name} to be created");
    };
    result.resource.headers.id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn delete(h: &impl FacadeContractHarness, owner: TestAccount, id: ResourceID) {
    let account = h.account_name(owner);
    let response = h
        .facade_for(owner)
        .delete(vec![by_id(&account, id)])
        .await
        .unwrap();
    assert_eq!(response.successes.len(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn get(
    h: &impl FacadeContractHarness,
    actor: TestAccount,
    account: Option<&odf::AccountName>,
    name: &str,
) -> Vec<Resource> {
    h.facade_for(actor)
        .get(
            vec![by_name(account, name)],
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await
        .unwrap()
        .successes
        .into_iter()
        .map(|success| success.item)
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The problems of a `get` by `refs`, in request order, asserting no ref succeeds
async fn get_problems(
    h: &impl FacadeContractHarness,
    actor: TestAccount,
    refs: Vec<ResourceRef>,
) -> Vec<(usize, ResourceLookupProblem)> {
    let response = h
        .facade_for(actor)
        .get(refs, SpecViewOpts::ENCRYPTED, ResourceVisibility::Active)
        .await
        .unwrap();
    assert_eq!(response.successes.len(), 0, "{:?}", response.successes);

    let mut problems = response
        .problems
        .into_iter()
        .map(|problem| (problem.request_index, problem.error))
        .collect::<Vec<_>>();
    problems.sort_by_key(|(request_index, _)| *request_index);
    problems
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
