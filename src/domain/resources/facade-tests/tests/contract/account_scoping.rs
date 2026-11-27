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
    ResourceID,
    ResourceOwnerRef,
    ResourceRef,
    ResourceSchemaProvider,
    ResourceSelector,
    ResourceVisibility,
    TypeName,
};
use kamu_resources_facade::{
    ApplyManifestError,
    ApplyManifestRequest,
    BatchResourceError,
    ListResourcesError,
    ResourceAccountResolutionError,
    ResourceAccountResolutionProblemCode,
    ResourceManifestFormat,
    ResourcesSummaryRequest,
    SpecViewOpts,
};
use pretty_assertions::{assert_eq, assert_matches};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, LocalFacadeHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_SCHEMA_STR,
    apply_manifest_and_get_id,
    assert_access_denied,
    assert_single_batch_success,
    create_variable_set,
    get_resource,
    search_request,
    sorted_handle_names,
    total_schema_count,
    variable_set_manifest_json_with_id,
    variable_set_ref_by_id,
    variable_set_ref_by_name,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_with_account_selector(
    h: &impl FacadeContractHarness,
    facade_account: TestAccount,
    name: &str,
    selector: ResourceOwnerRef,
) -> ResourceID {
    apply_manifest_and_get_id(
        h,
        facade_account,
        variable_set_manifest_json_with_account(name, &selector),
    )
    .await
}

fn variable_set_manifest_json_with_account(name: &str, account: &ResourceOwnerRef) -> String {
    let mut account_fields = Vec::new();
    if let Some(n) = &account.name {
        account_fields.push(format!(r#""name": "{n}""#));
    }
    if let Some(did) = &account.did {
        account_fields.push(format!(r#""did": "{did}""#));
    }
    let account_fields = account_fields.join(", ");
    indoc::formatdoc!(
        r#"
        {{
            "$schema": "{VARIABLE_SET_SCHEMA_STR}",
            "headers": {{
                "name": "{name}",
                "account": {{ {account_fields} }}
            }},
            "spec": {{
                "variables": {{
                    "K": {{"value": "v"}}
                }}
            }}
        }}"#
    )
}

fn account_by_name(name: &odf::AccountName) -> ResourceOwnerRef {
    ResourceOwnerRef {
        r#type: None,
        id: None,
        did: None,
        name: Some(name.clone()),
    }
}

fn account_by_id(id: odf::AccountID) -> ResourceOwnerRef {
    ResourceOwnerRef {
        r#type: None,
        id: None,
        did: Some(id),
        name: None,
    }
}

fn account_by_name_and_id(name: &odf::AccountName, id: odf::AccountID) -> ResourceOwnerRef {
    ResourceOwnerRef {
        r#type: None,
        id: None,
        did: Some(id),
        name: Some(name.clone()),
    }
}

fn unknown_account_by_name() -> ResourceOwnerRef {
    ResourceOwnerRef {
        r#type: None,
        id: None,
        did: None,
        name: Some(odf::AccountName::new_unchecked(
            "unknown-resource-contract-account",
        )),
    }
}

fn unknown_account_by_id() -> ResourceOwnerRef {
    ResourceOwnerRef {
        r#type: None,
        id: None,
        did: Some(odf::AccountID::new_generated_ed25519().1),
        name: None,
    }
}

fn selector_by_name(name: &str, account: Option<ResourceOwnerRef>) -> ResourceRef {
    ResourceRef {
        account,
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

fn batch_selector_by_name(name: &str, account: Option<ResourceOwnerRef>) -> Vec<ResourceRef> {
    vec![selector_by_name(name, account)]
}

/// Refs to the `VariableSet` of `name` and `id` in `account`: one by name, one
/// by id
fn refs_in_account(account: &odf::AccountName, name: &str, id: ResourceID) -> Vec<ResourceRef> {
    vec![
        selector_by_name(name, Some(account_by_name(account))),
        ResourceRef {
            account: Some(account_by_name(account)),
            ..variable_set_ref_by_id(id)
        },
    ]
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-120
contract_test!(
    default_account_selector_resolves_current_account,
    super::test_default_account_selector_resolves_current_account
);

pub async fn test_default_account_selector_resolves_current_account(
    h: &impl FacadeContractHarness,
) {
    create_variable_set(h, TestAccount::Alice, "acct-default").await;

    let view = assert_single_batch_success(
        h.facade_for(TestAccount::Alice)
            .get(
                vec![selector_by_name("acct-default", None)],
                SpecViewOpts::ENCRYPTED,
                ResourceVisibility::Active,
            )
            .await
            .unwrap(),
    );

    assert_eq!(view.headers.account.did, h.account_id(TestAccount::Alice));
    assert_eq!(
        view.headers.account.name,
        h.account_name(TestAccount::Alice)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-121
contract_test!(
    account_by_name_resolves_correctly,
    super::test_account_by_name_resolves_correctly
);

pub async fn test_account_by_name_resolves_correctly(h: &impl FacadeContractHarness) {
    create_with_account_selector(
        h,
        TestAccount::Alice,
        "acct-by-name",
        account_by_name(&h.account_name(TestAccount::Alice)),
    )
    .await;
    create_variable_set(h, TestAccount::Bob, "acct-by-name").await;

    let view = assert_single_batch_success(
        h.facade_for(TestAccount::Alice)
            .get(
                vec![selector_by_name(
                    "acct-by-name",
                    Some(account_by_name(&h.account_name(TestAccount::Alice))),
                )],
                SpecViewOpts::ENCRYPTED,
                ResourceVisibility::Active,
            )
            .await
            .unwrap(),
    );

    assert_eq!(view.headers.account.did, h.account_id(TestAccount::Alice));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-122
contract_test!(
    account_by_id_resolves_correctly,
    super::test_account_by_id_resolves_correctly
);

pub async fn test_account_by_id_resolves_correctly(h: &impl FacadeContractHarness) {
    create_with_account_selector(
        h,
        TestAccount::Alice,
        "acct-by-id",
        account_by_id(h.account_id(TestAccount::Alice)),
    )
    .await;

    let view = assert_single_batch_success(
        h.facade_for(TestAccount::Alice)
            .get(
                vec![selector_by_name(
                    "acct-by-id",
                    Some(account_by_id(h.account_id(TestAccount::Alice))),
                )],
                SpecViewOpts::ENCRYPTED,
                ResourceVisibility::Active,
            )
            .await
            .unwrap(),
    );

    assert_eq!(view.headers.account.did, h.account_id(TestAccount::Alice));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-122B
contract_test!(
    account_by_name_and_id_agree_resolves_correctly,
    super::test_account_by_name_and_id_agree_resolves_correctly
);

pub async fn test_account_by_name_and_id_agree_resolves_correctly(h: &impl FacadeContractHarness) {
    let agreeing = account_by_name_and_id(
        &h.account_name(TestAccount::Alice),
        h.account_id(TestAccount::Alice),
    );

    create_with_account_selector(h, TestAccount::Alice, "acct-both-agree", agreeing.clone()).await;

    let view = assert_single_batch_success(
        h.facade_for(TestAccount::Alice)
            .get(
                vec![selector_by_name("acct-both-agree", Some(agreeing))],
                SpecViewOpts::ENCRYPTED,
                ResourceVisibility::Active,
            )
            .await
            .unwrap(),
    );

    assert_eq!(view.headers.account.did, h.account_id(TestAccount::Alice));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-123
contract_test!(
    account_name_id_mismatch_is_rejected,
    super::test_account_name_id_mismatch_is_rejected
);

pub async fn test_account_name_id_mismatch_is_rejected(h: &impl FacadeContractHarness) {
    let mismatch = account_by_name_and_id(
        &h.account_name(TestAccount::Bob),
        h.account_id(TestAccount::Alice),
    );
    let manifest = variable_set_manifest_json_with_account("acct-mismatch", &mismatch);

    let apply_result = h
        .facade_for(TestAccount::Alice)
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await;
    assert_matches!(
        apply_result,
        Err(ApplyManifestError::AccountResolution(_)),
        "mismatched account must be rejected with AccountResolution, got: {apply_result:?}"
    );

    let get_result = h
        .facade_for(TestAccount::Alice)
        .get(
            vec![selector_by_name("acct-mismatch", Some(mismatch))],
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await;
    assert_matches!(
        get_result,
        Err(BatchResourceError::AccountResolution(_)),
        "mismatched account selector must be rejected"
    );

    let list = h
        .facade_for(TestAccount::Alice)
        .search(search_request(vec![ResourceSelector::of_type(
            VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
        )]))
        .await
        .unwrap()
        .items;
    assert!(list.iter().all(|item| item.name != "acct-mismatch"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-124
contract_test!(
    unknown_account_is_rejected,
    super::test_unknown_account_is_rejected
);

pub async fn test_unknown_account_is_rejected(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let by_name = facade
        .search(search_request(vec![ResourceSelector {
            account: Some(unknown_account_by_name()),
            ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
        }]))
        .await;
    // Type-less, so the account rides on an `AnyType` scope rather than a
    // per-type row.
    let by_id = facade
        .search(search_request(vec![ResourceSelector {
            account: Some(unknown_account_by_id()),
            ..ResourceSelector::default()
        }]))
        .await;

    assert_matches!(
        by_name,
        Err(ListResourcesError::AccountResolution(_)),
        "unknown account name must be rejected with AccountResolution, got: {by_name:?}"
    );
    assert_matches!(
        by_id,
        Err(ListResourcesError::AccountResolution(_)),
        "unknown account id must be rejected with AccountResolution, got: {by_id:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-125
contract_test!(
    account_isolation_across_read_apis,
    super::test_account_isolation_across_read_apis
);

pub async fn test_account_isolation_across_read_apis(h: &impl FacadeContractHarness) {
    let alice_id = create_variable_set(h, TestAccount::Alice, "acct-isolated").await;
    let bob_id = create_variable_set(h, TestAccount::Bob, "acct-isolated").await;
    create_variable_set(h, TestAccount::Alice, "acct-alice-only").await;
    create_variable_set(h, TestAccount::Bob, "acct-bob-only").await;

    let alice = h.facade_for(TestAccount::Alice);
    let bob = h.facade_for(TestAccount::Bob);

    let alice_view = assert_single_batch_success(
        alice
            .get(
                vec![selector_by_name("acct-isolated", None)],
                SpecViewOpts::ENCRYPTED,
                ResourceVisibility::Active,
            )
            .await
            .unwrap(),
    );
    let bob_view = assert_single_batch_success(
        bob.get(
            vec![selector_by_name("acct-isolated", None)],
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await
        .unwrap(),
    );
    assert_eq!(alice_view.headers.id, alice_id);
    assert_eq!(bob_view.headers.id, bob_id);

    let alice_handle = assert_single_batch_success(
        alice
            .get_handles(
                vec![selector_by_name("acct-isolated", None)],
                ResourceVisibility::Active,
            )
            .await
            .unwrap(),
    );
    let bob_handle = assert_single_batch_success(
        bob.get_handles(
            vec![selector_by_name("acct-isolated", None)],
            ResourceVisibility::Active,
        )
        .await
        .unwrap(),
    );
    assert_eq!(alice_handle.id, alice_id);
    assert_eq!(bob_handle.id, bob_id);

    let alice_batch = alice
        .get_handles(
            batch_selector_by_name("acct-isolated", None),
            ResourceVisibility::Active,
        )
        .await
        .unwrap();
    let bob_batch = bob
        .get(
            batch_selector_by_name("acct-isolated", None),
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await
        .unwrap();
    assert_eq!(alice_batch.successes[0].item.id, alice_id);
    assert_eq!(bob_batch.successes[0].item.headers.id, bob_id);

    let alice_list = alice
        .search_handles(search_request(vec![ResourceSelector::of_type(
            VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
        )]))
        .await
        .unwrap()
        .items;
    let bob_list = bob
        .search_handles(search_request(vec![ResourceSelector::of_type(
            VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
        )]))
        .await
        .unwrap()
        .items;
    assert_eq!(
        sorted_handle_names(alice_list),
        vec!["acct-alice-only", "acct-isolated"]
    );
    assert_eq!(
        sorted_handle_names(bob_list),
        vec!["acct-bob-only", "acct-isolated"]
    );

    let alice_search = alice
        .search_handles(search_request(vec![ResourceSelector::name_pattern(
            VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            "acct-%".to_string(),
        )]))
        .await
        .unwrap()
        .items;
    let bob_search = bob
        .search_handles(search_request(vec![ResourceSelector::name_pattern(
            VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            "acct-%".to_string(),
        )]))
        .await
        .unwrap()
        .items;
    assert_eq!(
        sorted_handle_names(alice_search),
        vec!["acct-alice-only", "acct-isolated"]
    );
    assert_eq!(
        sorted_handle_names(bob_search),
        vec!["acct-bob-only", "acct-isolated"]
    );

    let alice_all = alice
        .search_handles(search_request(
            // A default selector narrows by nothing, so it spans every type.
            vec![ResourceSelector::default()],
        ))
        .await
        .unwrap()
        .items;
    let bob_all = bob
        .search(search_request(vec![ResourceSelector::default()]))
        .await
        .unwrap()
        .items;
    assert_eq!(
        sorted_handle_names(alice_all),
        vec!["acct-alice-only", "acct-isolated"]
    );
    let mut bob_all_names: Vec<String> = bob_all
        .into_iter()
        .map(|item| item.name.to_string())
        .collect();
    bob_all_names.sort();
    assert_eq!(bob_all_names, vec!["acct-bob-only", "acct-isolated"]);

    assert_eq!(
        total_schema_count(
            alice
                .summary(ResourcesSummaryRequest { account: None })
                .await
                .unwrap(),
            VariableSetResource::type_uri(),
        ),
        2
    );
    assert_eq!(
        total_schema_count(
            bob.summary(ResourcesSummaryRequest { account: None })
                .await
                .unwrap(),
            VariableSetResource::type_uri(),
        ),
        2
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-126
contract_test!(
    empty_account_selector_is_rejected,
    super::test_empty_account_selector_is_rejected
);

/// An explicit `"account": {}` in the manifest — distinct from omitting the
/// `account` key entirely (which defaults to the caller's own account, see
/// RF-120). `ResourceOwnerRef` is a struct with all-optional fields, so
/// `{}` parses fine (all `None`); it is instead rejected by account
/// resolution (`ResourceAccountResolverImpl`), which looks the account up by
/// `did` or `name` and requires one of them.
pub async fn test_empty_account_selector_is_rejected(h: &impl FacadeContractHarness) {
    let manifest = indoc::formatdoc!(
        r#"
        {{
            "$schema": "{VARIABLE_SET_SCHEMA_STR}",
            "headers": {{
                "name": "acct-empty-selector",
                "account": {{}}
            }},
            "spec": {{
                "variables": {{
                    "K": {{"value": "v"}}
                }}
            }}
        }}"#
    );

    let result = h
        .facade_for(TestAccount::Alice)
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await;

    assert_matches!(
        result,
        Err(ApplyManifestError::AccountResolution(
            ResourceAccountResolutionError {
                code: ResourceAccountResolutionProblemCode::EmptySelector,
                ..
            }
        )),
        "an empty account selector `{{}}` must be rejected during account resolution, got: \
         {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-207
contract_test!(
    apply_by_id_finds_resources_of_its_own_account_only,
    super::test_apply_by_id_finds_resources_of_its_own_account_only
);

pub async fn test_apply_by_id_finds_resources_of_its_own_account_only(
    h: &impl FacadeContractHarness,
) {
    let bob_id = create_variable_set(h, TestAccount::Bob, "bob-own").await;
    let bob_facade = h.facade_for(TestAccount::Bob);
    let bob_spec = get_resource(bob_facade.as_ref(), variable_set_ref_by_id(bob_id))
        .await
        .spec;

    let facade = h.facade_for(TestAccount::Alice);
    let request = ApplyManifestRequest {
        format: ResourceManifestFormat::Json,
        manifest: variable_set_manifest_json_with_id(bob_id, "bob-own", "v2"),
    };
    // A remote facade reports a single apply as an untyped request error
    let not_found = format!("Resource with id {bob_id} was not found");
    let planned = facade.plan_apply_manifest(request.clone()).await;
    let applied = facade.apply_manifest(request).await;

    for error in [planned.unwrap_err(), applied.unwrap_err()] {
        assert_matches!(
            error,
            ApplyManifestError::IDNotFound(_) | ApplyManifestError::RemoteRequest(_)
        );
        assert!(error.to_string().contains(&not_found), "{error}");
    }
    assert_eq!(
        get_resource(bob_facade.as_ref(), variable_set_ref_by_id(bob_id))
            .await
            .spec,
        bob_spec
    );
    assert!(
        facade
            .get_handles(
                vec![variable_set_ref_by_name("bob-own")],
                ResourceVisibility::Active,
            )
            .await
            .unwrap()
            .successes
            .is_empty()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-208
contract_test!(
    batch_refs_to_another_account_are_denied_as_a_whole,
    super::test_batch_refs_to_another_account_are_denied_as_a_whole
);

/// Under the default rule a ref naming an account the caller may not use
/// fails the whole call, beside a ref the caller may use, and the denial
/// names none of the resources
pub async fn test_batch_refs_to_another_account_are_denied_as_a_whole(
    h: &impl FacadeContractHarness,
) {
    let bob_id = create_variable_set(h, TestAccount::Bob, "bob-secret").await;
    create_variable_set(h, TestAccount::Alice, "alice-own").await;
    let facade = h.facade_for(TestAccount::Alice);
    let mut refs = vec![variable_set_ref_by_name("alice-own")];
    refs.extend(refs_in_account(
        &h.account_name(TestAccount::Bob),
        "bob-secret",
        bob_id,
    ));

    let errors = [
        facade
            .get(
                refs.clone(),
                SpecViewOpts::ENCRYPTED,
                ResourceVisibility::Active,
            )
            .await
            .unwrap_err(),
        facade
            .get_handles(refs.clone(), ResourceVisibility::Active)
            .await
            .unwrap_err(),
        facade
            .render_manifests(
                refs.clone(),
                ResourceManifestFormat::Json,
                SpecViewOpts::ENCRYPTED,
                ResourceVisibility::Active,
            )
            .await
            .unwrap_err(),
        facade.delete(refs).await.unwrap_err(),
    ];

    for err in errors {
        assert_matches!(
            err,
            BatchResourceError::AccountAccess(_) | BatchResourceError::RemoteRequest(_)
        );
        assert_access_denied(&err);
        let message = err.to_string();
        assert!(
            !message.contains("bob-secret") && !message.contains(&bob_id.to_string()),
            "the denial must not leak resources: {message}"
        );
    }
    get_resource(facade.as_ref(), variable_set_ref_by_name("alice-own")).await;
    get_resource(
        h.facade_for(TestAccount::Bob).as_ref(),
        variable_set_ref_by_id(bob_id),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-208B
#[test_log::test(tokio::test)]
async fn test_admin_may_use_batch_refs_to_another_account() {
    let h = LocalFacadeHarness::new().await;
    let bob_id = create_variable_set(&h, TestAccount::Bob, "bob-own").await;
    let admin = h.facade_for_admin();
    let refs = refs_in_account(&h.account_name(TestAccount::Bob), "bob-own", bob_id);

    let got = admin
        .get(
            refs.clone(),
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await
        .unwrap();
    let handles = admin
        .get_handles(refs.clone(), ResourceVisibility::Active)
        .await
        .unwrap();
    let manifests = admin
        .render_manifests(
            refs.clone(),
            ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await
        .unwrap();

    for (successes, problems) in [
        (got.successes.len(), got.problems.len()),
        (handles.successes.len(), handles.problems.len()),
        (manifests.successes.len(), manifests.problems.len()),
    ] {
        assert_eq!((successes, problems), (2, 0));
    }
    let deleted = assert_single_batch_success(admin.delete(refs[1..].to_vec()).await.unwrap());
    assert_eq!(deleted.id, bob_id);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
