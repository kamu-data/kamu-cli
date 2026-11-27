// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use database_common::PaginationOpts;
use kamu_resources::{
    ApplyResourceOutcome,
    ApplyResourceRejectionCategory,
    MAX_OWNER_REFERENCES,
    Resource,
    ResourceID,
    ResourceOwnerRef,
    ResourceRef,
    ResourceSelector,
    ResourceVisibility,
};
use kamu_resources_facade::{
    ApplyManifestError,
    ApplyManifestRequest,
    ListResourcesError,
    ResourceFacade,
    ResourceHeadersValidationProblemCode,
    ResourceInvalidHeadersError,
    ResourceLookupProblem,
    ResourceManifestFormat,
    SearchResourcesRequest,
    SpecViewOpts,
    UnsupportedSelectorFieldError,
};
use pretty_assertions::{assert_eq, assert_matches};
use uuid::Uuid;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_CANONICAL_SELECTOR,
    SECRET_SET_SCHEMA_STR,
    VARIABLE_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_SCHEMA_STR,
    apply_manifest_and_get_id,
    assert_plan_and_apply_rejected,
    assert_single_batch_success,
    create_secret_set,
    create_variable_set,
    get_resource,
    ref_by_id,
    variable_set_ref_by_name,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-191
contract_test!(
    owners_are_stored_and_rendered,
    super::test_owners_are_stored_and_rendered
);

pub async fn test_owners_are_stored_and_rendered(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let first = create_variable_set(h, TestAccount::Alice, "first").await;
    let second = create_variable_set(h, TestAccount::Alice, "second").await;

    let id = create_owned(
        h,
        "owned",
        serde_json::json!(["VariableSet:first", { "type": "vs", "id": second }]),
    )
    .await;

    let resource = get_resource(facade.as_ref(), ref_by_id(id)).await;
    let owners = resource.headers.owner_references.as_ref().unwrap();
    assert_eq!(
        owners
            .iter()
            .map(|owner| (owner.id, owner.name.as_str(), owner.r#type.as_str()))
            .collect::<Vec<_>>(),
        [
            (first, "first", VARIABLE_SET_SCHEMA_STR),
            (second, "second", VARIABLE_SET_SCHEMA_STR),
        ]
    );
    for owner in owners {
        assert_eq!(owner.account, resource.headers.account);
    }

    let rendered = rendered_manifest(facade.as_ref(), id).await;
    let manifest: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(
        manifest["headers"]["ownerReferences"],
        serde_json::json!([
            { "type": VARIABLE_SET_SCHEMA_STR, "id": first, "name": "first" },
            { "type": VARIABLE_SET_SCHEMA_STR, "id": second, "name": "second" },
        ])
    );
    assert_eq!(
        reapply(facade.as_ref(), rendered).await,
        ApplyResourceOutcome::Untouched
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-211
contract_test!(
    owners_of_one_name_in_several_types_are_told_apart,
    super::test_owners_of_one_name_in_several_types_are_told_apart
);

pub async fn test_owners_of_one_name_in_several_types_are_told_apart(
    h: &impl FacadeContractHarness,
) {
    let facade = h.facade_for(TestAccount::Alice);
    let variable_set = create_variable_set(h, TestAccount::Alice, "shared-name").await;
    let secret_set =
        create_secret_set(h, TestAccount::Alice, "shared-name", &[("S", "secret")]).await;
    let other_variable_set = create_variable_set(h, TestAccount::Alice, "other-name").await;

    let id = create_owned(
        h,
        "owned",
        serde_json::json!([
            format!("{VARIABLE_SET_CANONICAL_SELECTOR}:shared-name"),
            format!("{SECRET_SET_CANONICAL_SELECTOR}:shared-name"),
            format!("{VARIABLE_SET_CANONICAL_SELECTOR}:other-name"),
        ]),
    )
    .await;

    let resource = get_resource(facade.as_ref(), ref_by_id(id)).await;
    assert_eq!(
        resource
            .headers
            .owner_references
            .unwrap()
            .iter()
            .map(|owner| (owner.id, owner.name.as_str(), owner.r#type.as_str()))
            .collect::<Vec<_>>(),
        [
            (variable_set, "shared-name", VARIABLE_SET_SCHEMA_STR),
            (secret_set, "shared-name", SECRET_SET_SCHEMA_STR),
            (other_variable_set, "other-name", VARIABLE_SET_SCHEMA_STR),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-192
contract_test!(
    wrong_owners_are_rejected,
    super::test_wrong_owners_are_rejected
);

pub async fn test_wrong_owners_are_rejected(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let owner = create_variable_set(h, TestAccount::Alice, "owner").await;
    let foreign = create_variable_set(h, TestAccount::Bob, "foreign").await;
    let owned = create_owned(h, "owned", serde_json::json!([])).await;
    let deleting = create_variable_set(h, TestAccount::Alice, "deleting").await;
    facade.delete(vec![ref_by_id(deleting)]).await.unwrap();

    use ApplyResourceRejectionCategory as C;
    for (owner_references, expected) in [
        (
            serde_json::json!([{ "id": ResourceID::new(Uuid::new_v4()) }]),
            C::ReferencedObjectMissing,
        ),
        (
            serde_json::json!(["VariableSet:missing"]),
            C::ReferencedObjectMissing,
        ),
        (
            serde_json::json!([{ "id": foreign }]),
            C::ReferencedObjectMissing,
        ),
        (
            serde_json::json!([{ "id": deleting }]),
            C::ReferencedObjectMissing,
        ),
        (
            serde_json::json!(["VariableSet:bob/foreign"]),
            C::BusinessValidationFailed,
        ),
        (
            serde_json::json!([{ "id": owner, "type": "SecretSet" }]),
            C::BusinessValidationFailed,
        ),
        (
            serde_json::json!([{ "id": owner, "name": "another" }]),
            C::BusinessValidationFailed,
        ),
        (
            serde_json::json!([{ "id": owner }, "VariableSet:owner"]),
            C::BusinessValidationFailed,
        ),
        (
            serde_json::json!([{ "id": owned }]),
            C::BusinessValidationFailed,
        ),
    ] {
        assert_plan_and_apply_rejected(
            facade.as_ref(),
            request(owned_manifest("owned", &owner_references)),
            expected,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-193
contract_test!(
    unresolvable_owners_are_invalid_headers,
    super::test_unresolvable_owners_are_invalid_headers
);

pub async fn test_unresolvable_owners_are_invalid_headers(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    use ResourceHeadersValidationProblemCode as C;
    for (owner_references, expected) in [
        (serde_json::json!([{}]), C::InvalidOwnerReference),
        (
            serde_json::json!([{ "name": "untyped" }]),
            C::InvalidOwnerReference,
        ),
        (
            serde_json::json!(["UnknownType:owner"]),
            C::InvalidOwnerReference,
        ),
        (
            serde_json::json!(vec!["VariableSet:owner"; MAX_OWNER_REFERENCES + 1]),
            C::TooManyOwnerReferences,
        ),
    ] {
        let manifest = owned_manifest("owned", &owner_references);

        for result in [
            facade
                .plan_apply_manifest(request(manifest.clone()))
                .await
                .map(|_| ()),
            facade
                .apply_manifest(request(manifest.clone()))
                .await
                .map(|_| ()),
        ] {
            assert_matches!(
                result,
                Err(ApplyManifestError::InvalidHeaders(ResourceInvalidHeadersError {
                    code,
                    ..
                })) if code == expected,
                "{owner_references}"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-194
contract_test!(
    owner_is_kept_after_its_deletion,
    super::test_owner_is_kept_after_its_deletion
);

pub async fn test_owner_is_kept_after_its_deletion(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let owner = create_variable_set(h, TestAccount::Alice, "owner").await;
    let id = create_owned(h, "owned", serde_json::json!([{ "id": owner }])).await;
    facade.delete(vec![ref_by_id(owner)]).await.unwrap();
    h.settle().await;

    let rendered = rendered_manifest(facade.as_ref(), id).await;
    assert_eq!(
        reapply(facade.as_ref(), rendered.clone()).await,
        ApplyResourceOutcome::Untouched
    );

    let mut changed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    changed["spec"]["variables"]["K"]["value"] = "changed".into();
    assert_eq!(
        reapply(facade.as_ref(), changed.to_string()).await,
        ApplyResourceOutcome::Updated
    );

    // Omitting the references drops them
    changed["headers"]
        .as_object_mut()
        .unwrap()
        .remove("ownerReferences");
    assert_eq!(
        reapply(facade.as_ref(), changed.to_string()).await,
        ApplyResourceOutcome::Updated
    );
    assert_eq!(
        search_owned(
            facade.as_ref(),
            ref_by_id(owner),
            ResourceVisibility::IncludeDeleted
        )
        .await
        .unwrap(),
        BTreeSet::new()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-197
contract_test!(
    owners_follow_the_renamed_account,
    super::test_owners_follow_the_renamed_account
);

pub async fn test_owners_follow_the_renamed_account(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let owner = create_variable_set(h, TestAccount::Alice, "owner").await;
    let id = create_owned(h, "owned", serde_json::json!([{ "id": owner }])).await;

    let new_name = odf::AccountName::new_unchecked("renamed");
    h.rename_account(TestAccount::Alice, &new_name).await;
    let account_id = h.account_id(TestAccount::Alice);

    let mut manifest: serde_json::Value = serde_json::from_str(&owned_manifest(
        "owned",
        &serde_json::json!([{ "id": owner }]),
    ))
    .unwrap();
    manifest["headers"]["account"] = serde_json::json!({ "did": account_id.to_string() });
    let manifest = manifest.to_string();

    let planned = facade
        .plan_apply_manifest(request(manifest.clone()))
        .await
        .unwrap()
        .expect_planned();
    assert_owners_account_name(&planned.resource, &new_name);

    let applied = facade
        .apply_manifest(request(manifest))
        .await
        .unwrap()
        .expect_applied();
    assert_owners_account_name(&applied.resource, &new_name);

    let found = get_resource(
        facade.as_ref(),
        ResourceRef {
            account: Some(ResourceOwnerRef::from(account_id)),
            ..ref_by_id(id)
        },
    )
    .await;
    assert_owners_account_name(&found, &new_name);

    // A session that began before the rename still names the account the old way
    let found = get_resource(facade.as_ref(), ref_by_id(id)).await;
    assert_owners_account_name(&found, &h.account_name(TestAccount::Alice));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-195
contract_test!(search_by_owner, super::test_search_by_owner);

pub async fn test_search_by_owner(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let owner = create_variable_set(h, TestAccount::Alice, "owner").await;
    let other = create_variable_set(h, TestAccount::Alice, "other").await;
    create_owned(h, "of-owner", serde_json::json!([{ "id": owner }])).await;
    create_owned(
        h,
        "of-both",
        serde_json::json!([{ "id": owner }, { "id": other }]),
    )
    .await;
    create_owned(h, "of-other", serde_json::json!([{ "id": other }])).await;
    let deleted = create_owned(h, "deleted", serde_json::json!([{ "id": owner }])).await;
    facade.delete(vec![ref_by_id(deleted)]).await.unwrap();
    h.settle().await;

    let names = |names: &[&str]| names.iter().copied().map(String::from).collect();

    use ResourceVisibility as V;
    for reference in [
        ref_by_id(owner),
        variable_set_ref_by_name("owner"),
        ResourceRef {
            id: Some(owner),
            ..variable_set_ref_by_name("OWNER")
        },
        ResourceRef {
            account: Some(ResourceOwnerRef::from(h.account_name(TestAccount::Alice))),
            ..variable_set_ref_by_name("owner")
        },
    ] {
        for (visibility, expected) in [
            (V::Active, names(&["of-both", "of-owner"])),
            (V::DeletedOnly, names(&["deleted"])),
            (
                V::IncludeDeleted,
                names(&["deleted", "of-both", "of-owner"]),
            ),
        ] {
            assert_eq!(
                search_owned(facade.as_ref(), reference.clone(), visibility)
                    .await
                    .unwrap(),
                expected
            );
        }
    }

    // Narrows what the selectors match, and the total
    for (selector, expected) in [
        (
            ResourceSelector::name_pattern(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                "of-o%".to_string(),
            ),
            names(&["of-owner"]),
        ),
        (
            ResourceSelector::of_type(SECRET_SET_CANONICAL_SELECTOR.parse().unwrap()),
            names(&[]),
        ),
    ] {
        let request = SearchResourcesRequest {
            selectors: vec![selector],
            ..search_request(ref_by_id(owner), V::Active)
        };
        assert_eq!(search(facade.as_ref(), request).await.unwrap(), expected);
    }

    // Bob sees nothing of Alice's
    assert_eq!(
        search_owned(
            h.facade_for(TestAccount::Bob).as_ref(),
            ref_by_id(owner),
            V::IncludeDeleted
        )
        .await
        .unwrap(),
        BTreeSet::new()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-196
contract_test!(
    owner_to_search_by_is_resolved,
    super::test_owner_to_search_by_is_resolved
);

pub async fn test_owner_to_search_by_is_resolved(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let facade = facade.as_ref();
    let owner = create_variable_set(h, TestAccount::Alice, "owner").await;
    create_owned(h, "owned", serde_json::json!([{ "id": owner }])).await;
    facade.delete(vec![ref_by_id(owner)]).await.unwrap();
    h.settle().await;

    use ListResourcesError as E;
    use ResourceLookupProblem as P;
    use ResourceVisibility as V;

    // An ID finds the resources of an owner that is gone
    assert_eq!(
        search_owned(facade, ref_by_id(owner), V::Active)
            .await
            .unwrap(),
        BTreeSet::from(["owned".to_string()])
    );
    assert_eq!(
        search_owned(
            facade,
            ref_by_id(ResourceID::new(Uuid::new_v4())),
            V::Active
        )
        .await
        .unwrap(),
        BTreeSet::new()
    );

    // A name finds live owners only, whatever the visibility
    for visibility in [V::Active, V::IncludeDeleted] {
        assert_matches!(
            search_owned(facade, variable_set_ref_by_name("owner"), visibility).await,
            Err(E::OwnerLookup(P::NameNotFound(_)))
        );
    }

    assert_matches!(
        search_owned(
            facade,
            ResourceRef {
                id: Some(owner),
                ..variable_set_ref_by_name("another")
            },
            V::Active
        )
        .await,
        Err(E::OwnerLookup(P::NameMismatch(_)))
    );
    assert_matches!(
        search_owned(
            facade,
            ResourceRef {
                r#type: Some(SECRET_SET_CANONICAL_SELECTOR.parse().unwrap()),
                ..ref_by_id(owner)
            },
            V::Active
        )
        .await,
        Err(E::OwnerLookup(P::SchemaMismatch(_)))
    );
    assert_matches!(
        search_owned(
            facade,
            ResourceRef {
                r#type: Some("UnknownType".parse().unwrap()),
                ..variable_set_ref_by_name("owner")
            },
            V::Active
        )
        .await,
        Err(E::UnsupportedSelector(_))
    );
    assert_matches!(
        search_owned(
            facade,
            ResourceRef {
                r#type: None,
                ..variable_set_ref_by_name("owner")
            },
            V::Active
        )
        .await,
        Err(E::UnsupportedSelectorField(
            UnsupportedSelectorFieldError::UntypedName
        ))
    );

    // A selector the facade cannot resolve fails the request before its owner
    // is looked up
    assert_matches!(
        search(
            facade,
            SearchResourcesRequest {
                selectors: vec![ResourceSelector {
                    did: Some(odf::metadata::Did::Odf(
                        odf::metadata::DidOdf::new_seeded_ed25519(b"owner"),
                    )),
                    ..ResourceSelector::default()
                }],
                ..search_request(variable_set_ref_by_name("missing"), V::Active)
            }
        )
        .await,
        Err(E::UnsupportedSelectorField(
            UnsupportedSelectorFieldError::Did
        ))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn request(manifest: String) -> ApplyManifestRequest {
    ApplyManifestRequest {
        format: ResourceManifestFormat::Json,
        manifest,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn owned_manifest(name: &str, owner_references: &serde_json::Value) -> String {
    serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA_STR,
        "headers": { "name": name, "ownerReferences": owner_references },
        "spec": { "variables": { "K": { "value": "v" } } },
    })
    .to_string()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_owned(
    h: &impl FacadeContractHarness,
    name: &str,
    owner_references: serde_json::Value,
) -> ResourceID {
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        owned_manifest(name, &owner_references),
    )
    .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_owners_account_name(resource: &Resource, expected: &odf::AccountName) {
    let names = resource
        .headers
        .owner_references
        .iter()
        .flatten()
        .map(|owner| &owner.account.name)
        .collect::<Vec<_>>();
    assert_eq!(names, [expected]);
    assert_eq!(resource.headers.account.name, *expected);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn rendered_manifest(facade: &dyn ResourceFacade, id: ResourceID) -> String {
    let response = facade
        .render_manifests(
            vec![ref_by_id(id)],
            ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await
        .unwrap();
    assert_single_batch_success(response).manifest
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn reapply(facade: &dyn ResourceFacade, manifest: String) -> ApplyResourceOutcome {
    facade
        .apply_manifest(request(manifest))
        .await
        .unwrap()
        .expect_applied()
        .outcome
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn search_request(owner: ResourceRef, visibility: ResourceVisibility) -> SearchResourcesRequest {
    SearchResourcesRequest {
        owner: Some(owner),
        visibility,
        ..SearchResourcesRequest::new(
            vec![ResourceSelector::default()],
            PaginationOpts {
                limit: 100,
                offset: 0,
            },
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn search_owned(
    facade: &dyn ResourceFacade,
    owner: ResourceRef,
    visibility: ResourceVisibility,
) -> Result<BTreeSet<String>, ListResourcesError> {
    search(facade, search_request(owner, visibility)).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Names found by both kinds of search, which must agree
async fn search(
    facade: &dyn ResourceFacade,
    request: SearchResourcesRequest,
) -> Result<BTreeSet<String>, ListResourcesError> {
    let handles = facade.search_handles(request.clone()).await;
    let summaries = facade.search(request).await;

    match (handles, summaries) {
        (Ok(handles), Ok(summaries)) => {
            let names = handles
                .items
                .into_iter()
                .map(|handle| handle.name.to_string())
                .collect::<BTreeSet<_>>();
            let summary_names = summaries
                .items
                .into_iter()
                .map(|summary| summary.name.to_string())
                .collect::<BTreeSet<_>>();
            assert_eq!(summary_names, names);
            assert_eq!(handles.total_count, names.len());
            assert_eq!(summaries.total_count, names.len());
            Ok(names)
        }
        (Err(handles_err), Err(summaries_err)) => {
            assert_eq!(handles_err.to_string(), summaries_err.to_string());
            Err(handles_err)
        }
        (handles, summaries) => panic!("{handles:?} vs {summaries:?}"),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
