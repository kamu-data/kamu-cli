// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_resources::ResourceSelector;
use kamu_resources_facade::{SearchResourceHandlesRequest, SearchResourcesRequest};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_CANONICAL_SELECTOR,
    SECRET_SET_SCHEMA_STR,
    VARIABLE_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_SCHEMA_STR,
    apply_manifest_and_get_id,
    normalize_handles,
    normalize_summary_views,
    secret_set_manifest_json,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn summary_keys(mut items: Vec<kamu_resources::ResourceSummaryView>) -> Vec<(String, String)> {
    normalize_summary_views(&mut items);
    items
        .into_iter()
        .map(|item| (item.schema.to_string(), item.name.to_string()))
        .collect()
}

fn handle_keys(mut items: Vec<kamu_resources::ResourceHandle>) -> Vec<(String, String)> {
    normalize_handles(&mut items);
    items
        .into_iter()
        .map(|item| (item.r#type.to_string(), item.name.to_string()))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-100
contract_test!(
    search_summaries_across_supported_resource_types,
    super::search_summaries_across_supported_resource_types
);

pub async fn search_summaries_across_supported_resource_types(h: &impl FacadeContractHarness) {
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("all-var-alice", None, &[("K", "v")]),
    )
    .await;
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        secret_set_manifest_json("all-secret-alice", None, &[("K", "v")]),
    )
    .await;
    apply_manifest_and_get_id(
        h,
        TestAccount::Bob,
        variable_set_manifest_json("all-var-bob", None, &[("K", "v")]),
    )
    .await;

    let summaries = h
        .facade_for(TestAccount::Alice)
        .search(SearchResourcesRequest {
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
            selectors: vec![ResourceSelector::default()],
        })
        .await
        .unwrap()
        .items;

    assert_eq!(
        summary_keys(summaries),
        vec![
            (
                SECRET_SET_SCHEMA_STR.to_string(),
                "all-secret-alice".to_string()
            ),
            (
                VARIABLE_SET_SCHEMA_STR.to_string(),
                "all-var-alice".to_string(),
            ),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-104
contract_test!(
    search_narrowed_by_selectors,
    super::search_narrowed_by_selectors
);

/// `search` takes selectors: it can span a subset of types, each with its own
/// name pattern. Local and remote must agree, since selectors travel over
/// GraphQL.
pub async fn search_narrowed_by_selectors(h: &impl FacadeContractHarness) {
    for name in ["scoped-app-var", "scoped-db-var"] {
        apply_manifest_and_get_id(
            h,
            TestAccount::Alice,
            variable_set_manifest_json(name, None, &[("K", "v")]),
        )
        .await;
    }
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        secret_set_manifest_json("scoped-app-secret", None, &[("K", "v")]),
    )
    .await;

    let search = async |selectors: Vec<ResourceSelector>| {
        h.facade_for(TestAccount::Alice)
            .search(SearchResourcesRequest {
                account: None,
                label_filter: None,
                pagination: PaginationOpts::from_max_results(1000),
                selectors,
            })
            .await
            .unwrap()
            .items
    };

    // A type-less selector spans every type.
    let any_type = search(vec![ResourceSelector::any_type_name_pattern(
        "scoped-app-%",
    )])
    .await;
    assert_eq!(
        summary_keys(any_type),
        vec![
            (
                SECRET_SET_SCHEMA_STR.to_string(),
                "scoped-app-secret".to_string()
            ),
            (
                VARIABLE_SET_SCHEMA_STR.to_string(),
                "scoped-app-var".to_string()
            ),
        ]
    );

    // Restricting to one type drops the other, even unnarrowed.
    let one_type = search(vec![ResourceSelector::of_type(
        VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
    )])
    .await;
    assert_eq!(
        summary_keys(one_type),
        vec![
            (
                VARIABLE_SET_SCHEMA_STR.to_string(),
                "scoped-app-var".to_string()
            ),
            (
                VARIABLE_SET_SCHEMA_STR.to_string(),
                "scoped-db-var".to_string()
            ),
        ]
    );

    // Each selector carries its own pattern: swapping them must change the
    // result, proving the pairing is per-selector rather than "any pattern,
    // any type".
    let per_type = search(vec![
        ResourceSelector::name_pattern(
            VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            "scoped-db-%",
        ),
        ResourceSelector::name_pattern(
            SECRET_SET_CANONICAL_SELECTOR.parse().unwrap(),
            "scoped-app-%",
        ),
    ])
    .await;
    assert_eq!(
        summary_keys(per_type),
        vec![
            (
                SECRET_SET_SCHEMA_STR.to_string(),
                "scoped-app-secret".to_string()
            ),
            (
                VARIABLE_SET_SCHEMA_STR.to_string(),
                "scoped-db-var".to_string()
            ),
        ]
    );

    // A pattern that matches nothing yields an empty listing, not an error.
    let no_match = search(vec![ResourceSelector::any_type_name_pattern("nomatch-%")]).await;
    assert!(summary_keys(no_match).is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-101
contract_test!(
    search_handles_across_supported_resource_types,
    super::test_search_handles_across_supported_resource_types
);

pub async fn test_search_handles_across_supported_resource_types(h: &impl FacadeContractHarness) {
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("all-id-var", None, &[("K", "v")]),
    )
    .await;
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        secret_set_manifest_json("all-id-secret", None, &[("K", "v")]),
    )
    .await;
    apply_manifest_and_get_id(
        h,
        TestAccount::Bob,
        secret_set_manifest_json("all-id-bob", None, &[("K", "v")]),
    )
    .await;

    let handles = h
        .facade_for(TestAccount::Alice)
        .search_handles(SearchResourceHandlesRequest {
            // Spans every type.
            selectors: vec![ResourceSelector::default()],
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap()
        .items;

    assert_eq!(
        handle_keys(handles),
        vec![
            (
                SECRET_SET_SCHEMA_STR.to_string(),
                "all-id-secret".to_string()
            ),
            (
                VARIABLE_SET_SCHEMA_STR.to_string(),
                "all-id-var".to_string()
            ),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-102
contract_test!(
    search_supports_pagination,
    super::test_search_supports_pagination
);

pub async fn test_search_supports_pagination(h: &impl FacadeContractHarness) {
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("all-page-var-1", None, &[("K", "v")]),
    )
    .await;
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        secret_set_manifest_json("all-page-secret", None, &[("K", "v")]),
    )
    .await;
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("all-page-var-2", None, &[("K", "v")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);
    let first_page = facade
        .search(SearchResourcesRequest {
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_page(0, 2),
            selectors: vec![ResourceSelector::default()],
        })
        .await
        .unwrap()
        .items;
    let second_page = facade
        .search(SearchResourcesRequest {
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_page(1, 2),
            selectors: vec![ResourceSelector::default()],
        })
        .await
        .unwrap()
        .items;
    let handle_second_page = facade
        .search_handles(SearchResourceHandlesRequest {
            // Spans every type.
            selectors: vec![ResourceSelector::default()],
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_page(1, 2),
        })
        .await
        .unwrap()
        .items;

    assert_eq!(first_page.len(), 2);
    assert_eq!(second_page.len(), 1);
    assert_eq!(
        second_page
            .iter()
            .map(|summary| summary.name.clone())
            .collect::<Vec<_>>(),
        handle_second_page
            .iter()
            .map(|handle| handle.name.clone())
            .collect::<Vec<_>>()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-103
contract_test!(
    search_empty_account_returns_empty,
    super::test_search_empty_account_returns_empty
);

pub async fn test_search_empty_account_returns_empty(h: &impl FacadeContractHarness) {
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("all-empty-alice", None, &[("K", "v")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Bob);
    let summaries = facade
        .search(SearchResourcesRequest {
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
            selectors: vec![ResourceSelector::default()],
        })
        .await
        .unwrap()
        .items;
    let handles = facade
        .search_handles(SearchResourceHandlesRequest {
            // Spans every type.
            selectors: vec![ResourceSelector::default()],
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap()
        .items;

    assert!(summaries.is_empty());
    assert!(handles.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
