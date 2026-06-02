// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_resources::{ResourceManifestAccount, ResourceUID};
use kamu_resources_facade::{
    ApplyManifestRequest,
    GetResourceError,
    ListAllResourceIdentitiesRequest,
    ListAllResourcesRequest,
    ListResourceIdentitiesRequest,
    ListResourcesRequest,
    ResourceBatchSelector,
    ResourceManifestFormat,
    ResourceRef,
    ResourceSelector,
    ResourcesSummaryRequest,
    SearchResourceIdentitiesRequest,
    SpecViewMode,
};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_API_VERSION,
    VARIABLE_SET_KIND,
    apply_manifest_and_get_uid,
    sorted_identity_names,
    total_kind_count,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_default_account_resource(
    h: &impl FacadeContractHarness,
    account: TestAccount,
    name: &str,
) -> ResourceUID {
    apply_manifest_and_get_uid(
        h,
        account,
        variable_set_manifest_json(name, None, &[("K", "v")]),
    )
    .await
}

async fn create_with_account_selector(
    h: &impl FacadeContractHarness,
    facade_account: TestAccount,
    name: &str,
    selector: ResourceManifestAccount,
) -> ResourceUID {
    apply_manifest_and_get_uid(
        h,
        facade_account,
        variable_set_manifest_json_with_account(name, &selector),
    )
    .await
}

fn variable_set_manifest_json_with_account(
    name: &str,
    account: &ResourceManifestAccount,
) -> String {
    let mut account_fields = Vec::new();
    if let Some(n) = &account.name {
        account_fields.push(format!(r#""name": "{n}""#));
    }
    if let Some(id) = &account.id {
        account_fields.push(format!(r#""id": "{id}""#));
    }
    let account_fields = account_fields.join(", ");
    let api = VARIABLE_SET_API_VERSION;
    let kind = VARIABLE_SET_KIND;
    indoc::formatdoc!(
        r#"
        {{
            "apiVersion": "{api}",
            "kind": "{kind}",
            "metadata": {{
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

fn account_by_name(name: &odf::AccountName) -> ResourceManifestAccount {
    ResourceManifestAccount {
        name: Some(name.to_string()),
        id: None,
    }
}

fn account_by_id(id: odf::AccountID) -> ResourceManifestAccount {
    ResourceManifestAccount {
        name: None,
        id: Some(id),
    }
}

fn account_by_name_and_id(name: &odf::AccountName, id: odf::AccountID) -> ResourceManifestAccount {
    ResourceManifestAccount {
        name: Some(name.to_string()),
        id: Some(id),
    }
}

fn unknown_account_by_name() -> ResourceManifestAccount {
    ResourceManifestAccount {
        name: Some("unknown-resource-contract-account".to_string()),
        id: None,
    }
}

fn unknown_account_by_id() -> ResourceManifestAccount {
    ResourceManifestAccount {
        name: None,
        id: Some(odf::AccountID::new_generated_ed25519().1),
    }
}

fn selector_by_name(name: &str, account: Option<ResourceManifestAccount>) -> ResourceSelector {
    ResourceSelector {
        account,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_ref: ResourceRef::ByName(name.to_string()),
    }
}

fn batch_selector_by_name(
    name: &str,
    account: Option<ResourceManifestAccount>,
) -> ResourceBatchSelector {
    ResourceBatchSelector {
        account,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_refs: vec![ResourceRef::ByName(name.to_string())],
    }
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
    create_default_account_resource(h, TestAccount::Alice, "acct-default").await;

    let view = h
        .facade_for(TestAccount::Alice)
        .get(
            selector_by_name("acct-default", None),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();

    assert_eq!(view.account.id, h.account_id(TestAccount::Alice));
    assert_eq!(view.account.name, Some(h.account_name(TestAccount::Alice)));
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
    create_default_account_resource(h, TestAccount::Bob, "acct-by-name").await;

    let view = h
        .facade_for(TestAccount::Alice)
        .get(
            selector_by_name(
                "acct-by-name",
                Some(account_by_name(&h.account_name(TestAccount::Alice))),
            ),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();

    assert_eq!(view.account.id, h.account_id(TestAccount::Alice));
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

    let view = h
        .facade_for(TestAccount::Alice)
        .get(
            selector_by_name(
                "acct-by-id",
                Some(account_by_id(h.account_id(TestAccount::Alice))),
            ),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();

    assert_eq!(view.account.id, h.account_id(TestAccount::Alice));
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
    assert!(apply_result.is_err(), "mismatched account must be rejected");

    let get_result = h
        .facade_for(TestAccount::Alice)
        .get(
            selector_by_name("acct-mismatch", Some(mismatch)),
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(get_result, Err(GetResourceError::BadAccount(_))),
        "mismatched account selector must be rejected, got: {get_result:?}"
    );

    let list = h
        .facade_for(TestAccount::Alice)
        .list(ListResourcesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
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
        .list(ListResourcesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: Some(unknown_account_by_name()),
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;
    let by_id = facade
        .list_all(ListAllResourcesRequest {
            account: Some(unknown_account_by_id()),
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;

    assert!(by_name.is_err(), "unknown account name must be rejected");
    assert!(by_id.is_err(), "unknown account id must be rejected");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-125
contract_test!(
    account_isolation_across_read_apis,
    super::test_account_isolation_across_read_apis
);

pub async fn test_account_isolation_across_read_apis(h: &impl FacadeContractHarness) {
    let alice_uid = create_default_account_resource(h, TestAccount::Alice, "acct-isolated").await;
    let bob_uid = create_default_account_resource(h, TestAccount::Bob, "acct-isolated").await;
    create_default_account_resource(h, TestAccount::Alice, "acct-alice-only").await;
    create_default_account_resource(h, TestAccount::Bob, "acct-bob-only").await;

    let alice = h.facade_for(TestAccount::Alice);
    let bob = h.facade_for(TestAccount::Bob);

    let alice_view = alice
        .get(
            selector_by_name("acct-isolated", None),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    let bob_view = bob
        .get(
            selector_by_name("acct-isolated", None),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    assert_eq!(alice_view.metadata.uid, alice_uid);
    assert_eq!(bob_view.metadata.uid, bob_uid);

    let alice_identity = alice
        .get_identity(selector_by_name("acct-isolated", None))
        .await
        .unwrap();
    let bob_identity = bob
        .get_identity(selector_by_name("acct-isolated", None))
        .await
        .unwrap();
    assert_eq!(alice_identity.uid, alice_uid);
    assert_eq!(bob_identity.uid, bob_uid);

    let alice_batch = alice
        .get_identities(batch_selector_by_name("acct-isolated", None))
        .await
        .unwrap();
    let bob_batch = bob
        .get_many(
            batch_selector_by_name("acct-isolated", None),
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    assert_eq!(alice_batch.successes[0].item.uid, alice_uid);
    assert_eq!(bob_batch.successes[0].item.metadata.uid, bob_uid);

    let alice_list = alice
        .list_identities(ListResourceIdentitiesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
    let bob_list = bob
        .list_identities(ListResourceIdentitiesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
    assert_eq!(
        sorted_identity_names(alice_list),
        vec!["acct-alice-only", "acct-isolated"]
    );
    assert_eq!(
        sorted_identity_names(bob_list),
        vec!["acct-bob-only", "acct-isolated"]
    );

    let alice_search = alice
        .search_identities(SearchResourceIdentitiesRequest {
            kinds: vec![VARIABLE_SET_KIND.to_string()],
            exact_names: None,
            name_pattern: Some("acct-%".to_string()),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
    let bob_search = bob
        .search_identities(SearchResourceIdentitiesRequest {
            kinds: vec![VARIABLE_SET_KIND.to_string()],
            exact_names: None,
            name_pattern: Some("acct-%".to_string()),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
    assert_eq!(
        sorted_identity_names(alice_search.items),
        vec!["acct-alice-only", "acct-isolated"]
    );
    assert_eq!(
        sorted_identity_names(bob_search.items),
        vec!["acct-bob-only", "acct-isolated"]
    );

    let alice_all = alice
        .list_all_identities(ListAllResourceIdentitiesRequest {
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
    let bob_all = bob
        .list_all(ListAllResourcesRequest {
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
    assert_eq!(
        sorted_identity_names(alice_all),
        vec!["acct-alice-only", "acct-isolated"]
    );
    let mut bob_all_names: Vec<String> = bob_all.into_iter().map(|item| item.name).collect();
    bob_all_names.sort();
    assert_eq!(bob_all_names, vec!["acct-bob-only", "acct-isolated"]);

    assert_eq!(
        total_kind_count(
            alice
                .summary(ResourcesSummaryRequest { account: None })
                .await
                .unwrap(),
            VARIABLE_SET_KIND,
        ),
        2
    );
    assert_eq!(
        total_kind_count(
            bob.summary(ResourcesSummaryRequest { account: None })
                .await
                .unwrap(),
            VARIABLE_SET_KIND,
        ),
        2
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
