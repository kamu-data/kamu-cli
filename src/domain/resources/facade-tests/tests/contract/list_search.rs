// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_resources::RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI;
use kamu_resources_facade::{
    ListResourceHandlesRequest,
    ListResourcesError,
    ListResourcesRequest,
    ResourceLabelFilterProblemCode,
    SearchResourceHandlesRequest,
};
use pretty_assertions::{assert_eq, assert_matches};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_CANONICAL_SELECTOR,
    SECRET_SET_SCHEMA_STR,
    VARIABLE_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_SCHEMA_STR,
    apply_manifest_and_get_id,
    create_variable_set_with_labels,
    label_filter,
    normalize_handles,
    normalize_summary_views,
    secret_set_manifest_json,
    sorted_handle_names,
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
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None, // default = alice
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: None,
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
        assert_eq!(
            s.schema.as_str(),
            VARIABLE_SET_SCHEMA_STR,
            "schema must match"
        );
        assert_eq!(
            s.schema.as_str(),
            VARIABLE_SET_SCHEMA_STR,
            "schema must match"
        );
        assert!(!s.id.to_string().is_empty(), "id must be set");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-081
contract_test!(
    list_handles_for_account,
    super::test_list_handles_for_account
);

pub async fn test_list_handles_for_account(h: &impl FacadeContractHarness) {
    create_resource(h, TestAccount::Alice, "idlist-alice-1").await;
    create_resource(h, TestAccount::Alice, "idlist-alice-2").await;
    create_resource(h, TestAccount::Bob, "idlist-bob-1").await;

    let facade = h.facade_for(TestAccount::Alice);

    let mut handles = facade
        .list_handles(ListResourceHandlesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    normalize_handles(&mut handles);

    let names: Vec<&str> = handles.iter().map(|i| i.name.as_str()).collect();
    assert!(names.contains(&"idlist-alice-1"));
    assert!(names.contains(&"idlist-alice-2"));
    assert!(!names.contains(&"idlist-bob-1"));

    for i in &handles {
        assert_eq!(i.r#type.as_str(), VARIABLE_SET_SCHEMA_STR);
        assert!(
            !kamu_resources::resource_type_name(&i.r#type)
                .unwrap()
                .as_str()
                .is_empty()
        );
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
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_page(0, 2),
            label_filter: None,
        })
        .await
        .unwrap();

    assert_eq!(summaries.len(), 2);
    assert!(
        summaries
            .iter()
            .all(|summary| summary.schema.as_str() == VARIABLE_SET_SCHEMA_STR)
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
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_page(0, 2),
            label_filter: None,
        })
        .await
        .unwrap();
    let second_page = facade
        .list(ListResourcesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_page(1, 2),
            label_filter: None,
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
    list_handles_pagination_mirrors_list,
    super::test_list_handles_pagination_mirrors_list
);

pub async fn test_list_handles_pagination_mirrors_list(h: &impl FacadeContractHarness) {
    for name in ["id-page-1", "id-page-2", "id-page-3"] {
        create_resource(h, TestAccount::Alice, name).await;
    }

    let facade = h.facade_for(TestAccount::Alice);
    let summaries = facade
        .list(ListResourcesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_page(1, 2),
            label_filter: None,
        })
        .await
        .unwrap();
    let handles = facade
        .list_handles(ListResourceHandlesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_page(1, 2),
        })
        .await
        .unwrap();

    assert_eq!(
        summaries
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>(),
        handles.iter().map(|i| i.name.as_str()).collect::<Vec<_>>()
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
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: None,
        })
        .await
        .unwrap();
    let handles = facade
        .list_handles(ListResourceHandlesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert!(summaries.is_empty());
    assert!(handles.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-086
contract_test!(
    list_unsupported_kind_returns_error,
    super::test_list_unsupported_kind_returns_error
);

pub async fn test_list_unsupported_kind_returns_error(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let unsupported_selector = "NoSuchResourceKind";

    let summaries = facade
        .list(ListResourcesRequest {
            raw_type_selector: unsupported_selector.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: None,
        })
        .await;
    let handles = facade
        .list_handles(ListResourceHandlesRequest {
            raw_type_selector: unsupported_selector.parse().unwrap(),
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;

    assert_matches!(
        summaries,
        Err(ListResourcesError::UnsupportedSelector(
            kamu_resources::UnsupportedResourceSelectorError::NotFound { raw_selector }
        )) if raw_selector.as_str() == unsupported_selector,
        "unsupported selector must be rejected, got: {summaries:?}"
    );
    assert_matches!(
        handles,
        Err(ListResourcesError::UnsupportedSelector(
            kamu_resources::UnsupportedResourceSelectorError::NotFound { raw_selector }
        )) if raw_selector.as_str() == unsupported_selector,
        "unsupported selector must be rejected, got: {handles:?}"
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
        .search_handles(SearchResourceHandlesRequest {
            raw_type_selectors: vec![VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap()],
            exact_names: Some(vec![
                "search-exact-alpha".parse().unwrap(),
                "search-exact-beta".parse().unwrap(),
            ]),
            name_pattern: None,
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(response.total_count, 2);
    assert_eq!(
        sorted_handle_names(response.items),
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
        .search_handles(SearchResourceHandlesRequest {
            raw_type_selectors: vec![VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap()],
            exact_names: Some(vec![
                "search-missing-present".parse().unwrap(),
                "search-missing-absent".parse().unwrap(),
            ]),
            name_pattern: None,
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(response.total_count, 1);
    assert_eq!(
        sorted_handle_names(response.items),
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
        .search_handles(SearchResourceHandlesRequest {
            raw_type_selectors: vec![VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap()],
            exact_names: None,
            name_pattern: Some("search-pattern-%".to_string()),
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(response.total_count, 2);
    assert_eq!(
        sorted_handle_names(response.items),
        vec!["search-pattern-alpha", "search-pattern-beta"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-093
contract_test!(search_multi_type, super::test_search_multi_type);

pub async fn test_search_multi_type(h: &impl FacadeContractHarness) {
    // Create one VariableSet and one SecretSet with a shared name prefix
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("multi-type-vs", None, &[("K", "v")]),
    )
    .await;
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        secret_set_manifest_json("multi-type-ss", None, &[("K", "v")]),
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
        .search_handles(SearchResourceHandlesRequest {
            raw_type_selectors: vec![
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                SECRET_SET_CANONICAL_SELECTOR.parse().unwrap(),
            ],
            exact_names: None,
            name_pattern: Some("multi-type-%".to_string()),
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(
        response.total_count, 2,
        "must find exactly the two multi-type resources"
    );

    let names = sorted_handle_names(response.items.clone());
    assert!(
        names.contains(&"multi-type-vs".to_string()),
        "VariableSet resource must appear in multi-type search"
    );
    assert!(
        names.contains(&"multi-type-ss".to_string()),
        "SecretSet resource must appear in multi-type search"
    );

    // Both schemas must be represented in the result
    let schemas: std::collections::HashSet<_> =
        response.items.iter().map(|i| i.r#type.as_str()).collect();
    assert!(
        schemas.contains(VARIABLE_SET_SCHEMA_STR),
        "result must include VariableSet schema"
    );
    assert!(
        schemas.contains(SECRET_SET_SCHEMA_STR),
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
        .search_handles(SearchResourceHandlesRequest {
            raw_type_selectors: vec![VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap()],
            exact_names: None,
            name_pattern: None,
            account: None,
            label_filter: None,
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
        .search_handles(SearchResourceHandlesRequest {
            raw_type_selectors: vec![VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap()],
            exact_names: None,
            name_pattern: Some("search-page-%".to_string()),
            account: None,
            label_filter: None,
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
        .search_handles(SearchResourceHandlesRequest {
            raw_type_selectors: vec![VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap()],
            exact_names: None,
            name_pattern: Some("search-scope-%".to_string()),
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
    let bob_response = h
        .facade_for(TestAccount::Bob)
        .search_handles(SearchResourceHandlesRequest {
            raw_type_selectors: vec![VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap()],
            exact_names: None,
            name_pattern: Some("search-scope-%".to_string()),
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(
        sorted_handle_names(alice_response.items),
        vec!["search-scope-alice", "search-scope-shared"]
    );
    assert_eq!(
        sorted_handle_names(bob_response.items),
        vec!["search-scope-bob", "search-scope-shared"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// The cases below exercise Phase-7 filter *resolution* at the local facade
// boundary only: repository-level matching lands in Phase 9, and the remote
// (GraphQL) transport for `label_filter` lands in Phase 10 — until then the
// remote client silently drops the filter, so these are local-only.

// RF-097
#[test_log::test(tokio::test)]
async fn list_filter_by_canonical_label_uri_is_accepted() {
    let h = crate::harness::LocalFacadeHarness::new().await;
    create_variable_set_with_labels(
        &h,
        TestAccount::Alice,
        "list-filter-prod",
        &[(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, "prod".into())],
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .list(ListResourcesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: Some(label_filter(&[(
                RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
                "prod",
            )])),
        })
        .await;

    assert_matches!(result, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-098
#[test_log::test(tokio::test)]
async fn list_filter_by_short_label_name_is_accepted() {
    let h = crate::harness::LocalFacadeHarness::new().await;
    create_variable_set_with_labels(
        &h,
        TestAccount::Alice,
        "list-filter-short-prod",
        &[(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, "prod".into())],
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);

    // Short name resolves to the same canonical URI as the fully-qualified key,
    // so it must be accepted just like the URI form.
    let result = facade
        .list(ListResourcesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: Some(label_filter(&[("environment", "prod")])),
        })
        .await;

    assert_matches!(result, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-099
#[test_log::test(tokio::test)]
async fn list_filter_by_free_form_label_is_accepted() {
    let h = crate::harness::LocalFacadeHarness::new().await;
    create_variable_set_with_labels(
        &h,
        TestAccount::Alice,
        "list-filter-freeform-a",
        &[("team", "data".into())],
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);

    // `team` has no registered schema, so it stays free-form and queryable.
    let result = facade
        .list(ListResourcesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: Some(label_filter(&[("team", "data")])),
        })
        .await;

    assert_matches!(result, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-099A
#[test_log::test(tokio::test)]
async fn list_filter_invalid_key_is_rejected() {
    let h = crate::harness::LocalFacadeHarness::new().await;
    let facade = h.facade_for(TestAccount::Alice);

    // `=` is not a valid TypeName/TypeUri character, so this key fails
    // `TypeRef::from_str` before any resolver lookup happens.
    let result = facade
        .list(ListResourcesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: Some(label_filter(&[("not a valid key=", "x")])),
        })
        .await;

    let Err(ListResourcesError::InvalidLabelFilter(err)) = result else {
        panic!("expected InvalidLabelFilter, got {result:?}");
    };
    assert_eq!(err.code, ResourceLabelFilterProblemCode::InvalidKey);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-099B
#[test_log::test(tokio::test)]
async fn list_filter_unknown_uri_is_rejected() {
    let h = crate::harness::LocalFacadeHarness::new().await;
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .list(ListResourcesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: Some(label_filter(&[(
                "https://kamu.dev/schemas/resource/v1alpha1/labels/DoesNotExist",
                "x",
            )])),
        })
        .await;

    let Err(ListResourcesError::InvalidLabelFilter(err)) = result else {
        panic!("expected InvalidLabelFilter, got {result:?}");
    };
    assert_eq!(
        err.code,
        ResourceLabelFilterProblemCode::ResourceExtensionSchema
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-099C
#[test_log::test(tokio::test)]
async fn list_filter_non_string_value_is_rejected() {
    let h = crate::harness::LocalFacadeHarness::new().await;
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .list(ListResourcesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: Some(kamu_resources::ResourceLabelFilterInput {
                entries: std::collections::BTreeMap::from([(
                    "environment".to_string(),
                    serde_json::json!({"not": "a string"}),
                )]),
            }),
        })
        .await;

    let Err(ListResourcesError::InvalidLabelFilter(err)) = result else {
        panic!("expected InvalidLabelFilter, got {result:?}");
    };
    assert_eq!(err.code, ResourceLabelFilterProblemCode::NonStringValue);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-099D
#[test_log::test(tokio::test)]
async fn list_filter_duplicate_after_canonicalization_is_rejected() {
    let h = crate::harness::LocalFacadeHarness::new().await;
    let facade = h.facade_for(TestAccount::Alice);

    // Short name `environment` and its canonical URI both resolve to the same
    // canonical key, so authoring both in one filter is a duplicate.
    let result = facade
        .list(ListResourcesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: Some(label_filter(&[
                ("environment", "prod"),
                (RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, "prod"),
            ])),
        })
        .await;

    let Err(ListResourcesError::InvalidLabelFilter(err)) = result else {
        panic!("expected InvalidLabelFilter, got {result:?}");
    };
    assert_eq!(
        err.code,
        ResourceLabelFilterProblemCode::DuplicateAfterCanonicalization
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-099E
#[test_log::test(tokio::test)]
async fn list_filter_not_operator_is_rejected() {
    let h = crate::harness::LocalFacadeHarness::new().await;
    let facade = h.facade_for(TestAccount::Alice);

    // `$not` is recognized as a reserved combinator key (per the ODF
    // `LabelFilter` schema's examples) but boolean-expression evaluation is
    // not implemented yet, so it must be rejected with a purpose-built code —
    // not misclassified as an invalid key or a non-string value.
    let result = facade
        .list(ListResourcesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: Some(kamu_resources::ResourceLabelFilterInput {
                entries: std::collections::BTreeMap::from([(
                    "$not".to_string(),
                    serde_json::json!({"environment": "prod"}),
                )]),
            }),
        })
        .await;

    let Err(ListResourcesError::InvalidLabelFilter(err)) = result else {
        panic!("expected InvalidLabelFilter, got {result:?}");
    };
    assert_eq!(
        err.code,
        ResourceLabelFilterProblemCode::UnsupportedExpression
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-099F
#[test_log::test(tokio::test)]
async fn list_filter_or_operator_is_rejected() {
    let h = crate::harness::LocalFacadeHarness::new().await;
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .list(ListResourcesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: Some(kamu_resources::ResourceLabelFilterInput {
                entries: std::collections::BTreeMap::from([(
                    "$or".to_string(),
                    serde_json::json!([{"environment": "prod"}, {"environment": "staging"}]),
                )]),
            }),
        })
        .await;

    let Err(ListResourcesError::InvalidLabelFilter(err)) = result else {
        panic!("expected InvalidLabelFilter, got {result:?}");
    };
    assert_eq!(
        err.code,
        ResourceLabelFilterProblemCode::UnsupportedExpression
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-099G
#[test_log::test(tokio::test)]
async fn list_filter_malformed_not_operator_is_rejected() {
    let h = crate::harness::LocalFacadeHarness::new().await;
    let facade = h.facade_for(TestAccount::Alice);

    // A malformed `$not` (wrong value shape) is a parse failure, not a
    // resolver failure, but it shares the same "not supported yet" code as a
    // well-formed `$not` — both mean "this filter syntax isn't usable yet."
    let result = facade
        .list(ListResourcesRequest {
            raw_type_selector: VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: Some(kamu_resources::ResourceLabelFilterInput {
                entries: std::collections::BTreeMap::from([(
                    "$not".to_string(),
                    serde_json::json!("not-an-object"),
                )]),
            }),
        })
        .await;

    let Err(ListResourcesError::InvalidLabelFilter(err)) = result else {
        panic!("expected InvalidLabelFilter, got {result:?}");
    };
    assert_eq!(
        err.code,
        ResourceLabelFilterProblemCode::UnsupportedExpression
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
