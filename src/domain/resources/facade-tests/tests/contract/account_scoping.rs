// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_configuration::VariableSetResource;
use kamu_resources::{
    ResourceAccountRef,
    ResourceID,
    ResourceRef,
    ResourceSchemaProvider,
    ResourceSelector,
    TypeName,
};
use kamu_resources_facade::{
    ApplyManifestError,
    ApplyManifestRequest,
    BatchResourceError,
    ListResourcesError,
    ResolveManifestAccountError,
    ResourceManifestFormat,
    ResourcesSummaryRequest,
    SearchResourcesRequest,
    SpecViewMode,
};
use pretty_assertions::{assert_eq, assert_matches};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_SCHEMA_STR,
    apply_manifest_and_get_id,
    assert_single_batch_success,
    create_variable_set,
    sorted_handle_names,
    total_schema_count,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_with_account_selector(
    h: &impl FacadeContractHarness,
    facade_account: TestAccount,
    name: &str,
    selector: ResourceAccountRef,
) -> ResourceID {
    apply_manifest_and_get_id(
        h,
        facade_account,
        variable_set_manifest_json_with_account(name, &selector),
    )
    .await
}

fn variable_set_manifest_json_with_account(name: &str, account: &ResourceAccountRef) -> String {
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

fn account_by_name(name: &odf::AccountName) -> ResourceAccountRef {
    ResourceAccountRef {
        id: None,
        did: None,
        name: Some(name.clone()),
    }
}

fn account_by_id(id: odf::AccountID) -> ResourceAccountRef {
    ResourceAccountRef {
        id: None,
        did: Some(id),
        name: None,
    }
}

fn account_by_name_and_id(name: &odf::AccountName, id: odf::AccountID) -> ResourceAccountRef {
    ResourceAccountRef {
        id: None,
        did: Some(id),
        name: Some(name.clone()),
    }
}

fn unknown_account_by_name() -> ResourceAccountRef {
    ResourceAccountRef {
        id: None,
        did: None,
        name: Some(odf::AccountName::new_unchecked(
            "unknown-resource-contract-account",
        )),
    }
}

fn unknown_account_by_id() -> ResourceAccountRef {
    ResourceAccountRef {
        id: None,
        did: Some(odf::AccountID::new_generated_ed25519().1),
        name: None,
    }
}

fn selector_by_name(name: &str, account: Option<ResourceAccountRef>) -> ResourceRef {
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

fn batch_selector_by_name(name: &str, account: Option<ResourceAccountRef>) -> Vec<ResourceRef> {
    vec![selector_by_name(name, account)]
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
                SpecViewMode::Encrypted,
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
                SpecViewMode::Encrypted,
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
                SpecViewMode::Encrypted,
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
                SpecViewMode::Encrypted,
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
    assert!(
        matches!(apply_result, Err(ApplyManifestError::BadAccount(_))),
        "mismatched account must be rejected with BadAccount, got: {apply_result:?}"
    );

    let get_result = h
        .facade_for(TestAccount::Alice)
        .get(
            vec![selector_by_name("acct-mismatch", Some(mismatch))],
            SpecViewMode::Encrypted,
        )
        .await;
    assert_matches!(
        get_result,
        Err(BatchResourceError::BadAccount(_)),
        "mismatched account selector must be rejected"
    );

    let list = h
        .facade_for(TestAccount::Alice)
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: Some(unknown_account_by_name()),
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;
    let by_id = facade
        .search(SearchResourcesRequest {
            account: Some(unknown_account_by_id()),
            pagination: PaginationOpts::from_max_results(1000),
            selectors: vec![ResourceSelector::default()],
        })
        .await;

    assert!(
        matches!(by_name, Err(ListResourcesError::BadAccount(_))),
        "unknown account name must be rejected with BadAccount, got: {by_name:?}"
    );
    assert!(
        matches!(by_id, Err(ListResourcesError::BadAccount(_))),
        "unknown account id must be rejected with BadAccount, got: {by_id:?}"
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
                SpecViewMode::Encrypted,
            )
            .await
            .unwrap(),
    );
    let bob_view = assert_single_batch_success(
        bob.get(
            vec![selector_by_name("acct-isolated", None)],
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap(),
    );
    assert_eq!(alice_view.headers.id, alice_id);
    assert_eq!(bob_view.headers.id, bob_id);

    let alice_handle = assert_single_batch_success(
        alice
            .get_handles(vec![selector_by_name("acct-isolated", None)])
            .await
            .unwrap(),
    );
    let bob_handle = assert_single_batch_success(
        bob.get_handles(vec![selector_by_name("acct-isolated", None)])
            .await
            .unwrap(),
    );
    assert_eq!(alice_handle.id, alice_id);
    assert_eq!(bob_handle.id, bob_id);

    let alice_batch = alice
        .get_handles(batch_selector_by_name("acct-isolated", None))
        .await
        .unwrap();
    let bob_batch = bob
        .get(
            batch_selector_by_name("acct-isolated", None),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    assert_eq!(alice_batch.successes[0].item.id, alice_id);
    assert_eq!(bob_batch.successes[0].item.headers.id, bob_id);

    let alice_list = alice
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap()
        .items;
    let bob_list = bob
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
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
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector::name_pattern(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                "acct-%".to_string(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap()
        .items;
    let bob_search = bob
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector::name_pattern(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                "acct-%".to_string(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
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
        .search_handles(SearchResourcesRequest {
            // Spans every type, as `list_all_handles` did.
            selectors: vec![ResourceSelector::default()],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap()
        .items;
    let bob_all = bob
        .search(SearchResourcesRequest {
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            selectors: vec![ResourceSelector::default()],
        })
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
            VariableSetResource::schema(),
        ),
        2
    );
    assert_eq!(
        total_schema_count(
            bob.summary(ResourcesSummaryRequest { account: None })
                .await
                .unwrap(),
            VariableSetResource::schema(),
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
/// RF-120). `ResourceAccountRef` is a struct with all-optional fields, so
/// `{}` parses fine (all `None`); it is instead rejected by account
/// resolution (`ResourceAccountResolverImpl`), which requires at least one of
/// `id`, `did`, or `name` to be present.
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
        Err(ApplyManifestError::BadAccount(
            ResolveManifestAccountError::EmptySelector
        )),
        "an empty account selector `{{}}` must be rejected during account resolution, got: \
         {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
