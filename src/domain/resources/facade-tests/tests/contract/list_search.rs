// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_resources_facade::{
    ListResourceIdentitiesRequest,
    ListResourcesError,
    ListResourcesRequest,
    SearchResourceIdentitiesRequest,
};
use pretty_assertions::{assert_eq, assert_matches};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_KIND,
    SECRET_SET_SCHEMA,
    VARIABLE_SET_KIND,
    VARIABLE_SET_SCHEMA,
    apply_manifest_and_get_id,
    normalize_identity_views,
    normalize_summary_views,
    secret_set_manifest_json,
    sorted_identity_names,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_resource(h: &impl FacadeContractHarness, account: TestAccount, name: &str) {
    apply_manifest_and_get_id(
        h,
        account,
        variable_set_manifest_json(name, None, &[("K", "v")]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-080
contract_test!(
    list_summaries_for_account,
    super::test_list_summaries_for_account
);

pub async fn test_list_summaries_for_account(h: &impl FacadeContractHarness) {
    // Create resources in each account
    create_resource(h, TestAccount::Alice, "list-alice-1").await;
    create_resource(h, TestAccount::Alice, "list-alice-2").await;
    create_resource(h, TestAccount::Bob, "list-bob-1").await;

    let facade = h.facade_for(TestAccount::Alice);

    let mut summaries = facade
        .list(ListResourcesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None, // default = alice
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    normalize_summary_views(&mut summaries);

    // Only alice's resources
    let names: Vec<&str> = summaries.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"list-alice-1"), "alice-1 must be listed");
    assert!(names.contains(&"list-alice-2"), "alice-2 must be listed");
    assert!(
        !names.contains(&"list-bob-1"),
        "bob-1 must not appear in alice's list"
    );

    for s in &summaries {
        assert_eq!(s.schema, VARIABLE_SET_SCHEMA, "schema must match");
        assert_eq!(s.schema, VARIABLE_SET_SCHEMA, "schema must match");
        assert!(!s.id.to_string().is_empty(), "id must be set");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-081
contract_test!(
    list_identities_for_account,
    super::test_list_identities_for_account
);

pub async fn test_list_identities_for_account(h: &impl FacadeContractHarness) {
    create_resource(h, TestAccount::Alice, "idlist-alice-1").await;
    create_resource(h, TestAccount::Alice, "idlist-alice-2").await;
    create_resource(h, TestAccount::Bob, "idlist-bob-1").await;

    let facade = h.facade_for(TestAccount::Alice);

    let mut identities = facade
        .list_identities(ListResourceIdentitiesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    normalize_identity_views(&mut identities);

    let names: Vec<&str> = identities.iter().map(|i| i.name.as_str()).collect();
    assert!(names.contains(&"idlist-alice-1"));
    assert!(names.contains(&"idlist-alice-2"));
    assert!(!names.contains(&"idlist-bob-1"));

    for i in &identities {
        assert_eq!(i.schema, VARIABLE_SET_SCHEMA);
        assert_eq!(i.schema, VARIABLE_SET_SCHEMA);
        assert!(!i.canonical_kind_name.is_empty());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-082
contract_test!(
    list_supports_pagination_limit,
    super::test_list_supports_pagination_limit
);

pub async fn test_list_supports_pagination_limit(h: &impl FacadeContractHarness) {
    for name in ["list-limit-1", "list-limit-2", "list-limit-3"] {
        create_resource(h, TestAccount::Alice, name).await;
    }

    let facade = h.facade_for(TestAccount::Alice);
    let summaries = facade
        .list(ListResourcesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts::from_page(0, 2),
        })
        .await
        .unwrap();

    assert_eq!(summaries.len(), 2);
    assert!(
        summaries
            .iter()
            .all(|summary| summary.schema == VARIABLE_SET_SCHEMA)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-083
contract_test!(
    list_supports_pagination_offset,
    super::test_list_supports_pagination_offset
);

pub async fn test_list_supports_pagination_offset(h: &impl FacadeContractHarness) {
    for name in ["list-offset-1", "list-offset-2", "list-offset-3"] {
        create_resource(h, TestAccount::Alice, name).await;
    }

    let facade = h.facade_for(TestAccount::Alice);
    let first_page = facade
        .list(ListResourcesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts::from_page(0, 2),
        })
        .await
        .unwrap();
    let second_page = facade
        .list(ListResourcesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts::from_page(1, 2),
        })
        .await
        .unwrap();

    assert_eq!(first_page.len(), 2);
    assert_eq!(second_page.len(), 1);

    let first_names: Vec<_> = first_page.iter().map(|s| s.name.as_str()).collect();
    let second_names: Vec<_> = second_page.iter().map(|s| s.name.as_str()).collect();
    assert!(
        first_names.iter().all(|name| !second_names.contains(name)),
        "offset page must not repeat first page items"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-084
contract_test!(
    list_identities_pagination_mirrors_list,
    super::test_list_identities_pagination_mirrors_list
);

pub async fn test_list_identities_pagination_mirrors_list(h: &impl FacadeContractHarness) {
    for name in ["id-page-1", "id-page-2", "id-page-3"] {
        create_resource(h, TestAccount::Alice, name).await;
    }

    let facade = h.facade_for(TestAccount::Alice);
    let summaries = facade
        .list(ListResourcesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts::from_page(1, 2),
        })
        .await
        .unwrap();
    let identities = facade
        .list_identities(ListResourceIdentitiesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts::from_page(1, 2),
        })
        .await
        .unwrap();

    assert_eq!(
        summaries
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>(),
        identities
            .iter()
            .map(|i| i.name.as_str())
            .collect::<Vec<_>>()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-085
contract_test!(
    list_empty_account_returns_empty,
    super::test_list_empty_account_returns_empty
);

pub async fn test_list_empty_account_returns_empty(h: &impl FacadeContractHarness) {
    create_resource(h, TestAccount::Alice, "list-empty-alice").await;
    let facade = h.facade_for(TestAccount::Bob);

    let summaries = facade
        .list(ListResourcesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
    let identities = facade
        .list_identities(ListResourceIdentitiesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert!(summaries.is_empty());
    assert!(identities.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-086
contract_test!(
    list_unsupported_kind_returns_error,
    super::test_list_unsupported_kind_returns_error
);

pub async fn test_list_unsupported_kind_returns_error(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let summaries = facade
        .list(ListResourcesRequest {
            kind: "NoSuchResourceKind".to_string(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;
    let identities = facade
        .list_identities(ListResourceIdentitiesRequest {
            kind: "NoSuchResourceKind".to_string(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;

    assert!(
        matches!(summaries, Err(ListResourcesError::UnsupportedDescriptor(_))),
        "unsupported kind must be rejected, got: {summaries:?}"
    );
    assert!(
        matches!(
            identities,
            Err(ListResourcesError::UnsupportedDescriptor(_))
        ),
        "unsupported kind must be rejected, got: {identities:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-090
contract_test!(search_by_exact_names, super::test_search_by_exact_names);

pub async fn test_search_by_exact_names(h: &impl FacadeContractHarness) {
    create_resource(h, TestAccount::Alice, "search-exact-alpha").await;
    create_resource(h, TestAccount::Alice, "search-exact-beta").await;
    create_resource(h, TestAccount::Bob, "search-exact-alpha").await;

    let facade = h.facade_for(TestAccount::Alice);
    let response = facade
        .search_identities(SearchResourceIdentitiesRequest {
            kinds: vec![VARIABLE_SET_KIND.to_string()],
            exact_names: Some(vec![
                "search-exact-alpha".parse().unwrap(),
                "search-exact-beta".parse().unwrap(),
            ]),
            name_pattern: None,
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(response.total_count, 2);
    assert_eq!(
        sorted_identity_names(response.items),
        vec!["search-exact-alpha", "search-exact-beta"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-091
contract_test!(
    search_exact_names_ignores_missing,
    super::test_search_exact_names_ignores_missing
);

pub async fn test_search_exact_names_ignores_missing(h: &impl FacadeContractHarness) {
    create_resource(h, TestAccount::Alice, "search-missing-present").await;

    let facade = h.facade_for(TestAccount::Alice);
    let response = facade
        .search_identities(SearchResourceIdentitiesRequest {
            kinds: vec![VARIABLE_SET_KIND.to_string()],
            exact_names: Some(vec![
                "search-missing-present".parse().unwrap(),
                "search-missing-absent".parse().unwrap(),
            ]),
            name_pattern: None,
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(response.total_count, 1);
    assert_eq!(
        sorted_identity_names(response.items),
        vec!["search-missing-present"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-092
contract_test!(search_by_name_pattern, super::test_search_by_name_pattern);

pub async fn test_search_by_name_pattern(h: &impl FacadeContractHarness) {
    create_resource(h, TestAccount::Alice, "search-pattern-alpha").await;
    create_resource(h, TestAccount::Alice, "search-pattern-beta").await;
    create_resource(h, TestAccount::Alice, "search-other-alpha").await;

    let facade = h.facade_for(TestAccount::Alice);
    let response = facade
        .search_identities(SearchResourceIdentitiesRequest {
            kinds: vec![VARIABLE_SET_KIND.to_string()],
            exact_names: None,
            name_pattern: Some("search-pattern-%".to_string()),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(response.total_count, 2);
    assert_eq!(
        sorted_identity_names(response.items),
        vec!["search-pattern-alpha", "search-pattern-beta"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-093
contract_test!(search_multi_kind, super::test_search_multi_kind);

pub async fn test_search_multi_kind(h: &impl FacadeContractHarness) {
    // Create one VariableSet and one SecretSet with a shared name prefix
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("multi-kind-vs", None, &[("K", "v")]),
    )
    .await;
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        secret_set_manifest_json("multi-kind-ss", None, &[("K", "v")]),
    )
    .await;
    // A third resource that must not appear in results
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("other-resource", None, &[("K", "v")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);
    let response = facade
        .search_identities(SearchResourceIdentitiesRequest {
            kinds: vec![VARIABLE_SET_KIND.to_string(), SECRET_SET_KIND.to_string()],
            exact_names: None,
            name_pattern: Some("multi-kind-%".to_string()),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(
        response.total_count, 2,
        "must find exactly the two multi-kind resources"
    );

    let names = sorted_identity_names(response.items.clone());
    assert!(
        names.contains(&"multi-kind-vs".to_string()),
        "VariableSet resource must appear in multi-kind search"
    );
    assert!(
        names.contains(&"multi-kind-ss".to_string()),
        "SecretSet resource must appear in multi-kind search"
    );

    // Both kinds must be represented in the result
    let kinds: std::collections::HashSet<_> =
        response.items.iter().map(|i| i.schema.as_str()).collect();
    assert!(
        kinds.contains(VARIABLE_SET_SCHEMA),
        "result must include VariableSet schema"
    );
    assert!(
        kinds.contains(SECRET_SET_SCHEMA),
        "result must include SecretSet schema"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-094
contract_test!(
    search_empty_criteria_returns_error,
    super::test_search_empty_criteria_returns_error
);

pub async fn test_search_empty_criteria_returns_error(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .search_identities(SearchResourceIdentitiesRequest {
            kinds: vec![VARIABLE_SET_KIND.to_string()],
            exact_names: None,
            name_pattern: None,
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;

    assert_matches!(result, Err(ListResourcesError::InvalidSearchQuery(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-095
contract_test!(
    search_pagination_and_total_count,
    super::test_search_pagination_and_total_count
);

pub async fn test_search_pagination_and_total_count(h: &impl FacadeContractHarness) {
    for name in ["search-page-1", "search-page-2", "search-page-3"] {
        create_resource(h, TestAccount::Alice, name).await;
    }

    let facade = h.facade_for(TestAccount::Alice);
    let response = facade
        .search_identities(SearchResourceIdentitiesRequest {
            kinds: vec![VARIABLE_SET_KIND.to_string()],
            exact_names: None,
            name_pattern: Some("search-page-%".to_string()),
            account: None,
            pagination: PaginationOpts::from_page(1, 2),
        })
        .await
        .unwrap();

    assert_eq!(response.total_count, 3);
    assert_eq!(response.items.len(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-096
contract_test!(search_account_scoping, super::test_search_account_scoping);

pub async fn test_search_account_scoping(h: &impl FacadeContractHarness) {
    create_resource(h, TestAccount::Alice, "search-scope-shared").await;
    create_resource(h, TestAccount::Alice, "search-scope-alice").await;
    create_resource(h, TestAccount::Bob, "search-scope-shared").await;
    create_resource(h, TestAccount::Bob, "search-scope-bob").await;

    let alice_response = h
        .facade_for(TestAccount::Alice)
        .search_identities(SearchResourceIdentitiesRequest {
            kinds: vec![VARIABLE_SET_KIND.to_string()],
            exact_names: None,
            name_pattern: Some("search-scope-%".to_string()),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
    let bob_response = h
        .facade_for(TestAccount::Bob)
        .search_identities(SearchResourceIdentitiesRequest {
            kinds: vec![VARIABLE_SET_KIND.to_string()],
            exact_names: None,
            name_pattern: Some("search-scope-%".to_string()),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(
        sorted_identity_names(alice_response.items),
        vec!["search-scope-alice", "search-scope-shared"]
    );
    assert_eq!(
        sorted_identity_names(bob_response.items),
        vec!["search-scope-bob", "search-scope-shared"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
