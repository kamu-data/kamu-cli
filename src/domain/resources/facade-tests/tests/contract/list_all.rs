// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_resources_facade::{ListAllResourceIdentitiesRequest, ListAllResourcesRequest};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_KIND,
    VARIABLE_SET_KIND,
    apply_manifest_and_get_id,
    normalize_identity_views,
    normalize_summary_views,
    secret_set_manifest_json,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn summary_keys(mut items: Vec<kamu_resources::ResourceSummaryView>) -> Vec<(String, String)> {
    normalize_summary_views(&mut items);
    items
        .into_iter()
        .map(|item| (item.kind, item.name))
        .collect()
}

fn identity_keys(mut items: Vec<kamu_resources::ResourceIdentityView>) -> Vec<(String, String)> {
    normalize_identity_views(&mut items);
    items
        .into_iter()
        .map(|item| (item.kind, item.name))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-100
contract_test!(
    list_all_summaries_across_supported_kinds,
    super::test_list_all_summaries_across_supported_kinds
);

pub async fn test_list_all_summaries_across_supported_kinds(h: &impl FacadeContractHarness) {
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
        .list_all(ListAllResourcesRequest {
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(
        summary_keys(summaries),
        vec![
            (SECRET_SET_KIND.to_string(), "all-secret-alice".to_string()),
            (VARIABLE_SET_KIND.to_string(), "all-var-alice".to_string()),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-101
contract_test!(
    list_all_identities_across_supported_kinds,
    super::test_list_all_identities_across_supported_kinds
);

pub async fn test_list_all_identities_across_supported_kinds(h: &impl FacadeContractHarness) {
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

    let identities = h
        .facade_for(TestAccount::Alice)
        .list_all_identities(ListAllResourceIdentitiesRequest {
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(
        identity_keys(identities),
        vec![
            (SECRET_SET_KIND.to_string(), "all-id-secret".to_string()),
            (VARIABLE_SET_KIND.to_string(), "all-id-var".to_string()),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-102
contract_test!(
    list_all_supports_pagination,
    super::test_list_all_supports_pagination
);

pub async fn test_list_all_supports_pagination(h: &impl FacadeContractHarness) {
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
        .list_all(ListAllResourcesRequest {
            account: None,
            pagination: PaginationOpts::from_page(0, 2),
        })
        .await
        .unwrap();
    let second_page = facade
        .list_all(ListAllResourcesRequest {
            account: None,
            pagination: PaginationOpts::from_page(1, 2),
        })
        .await
        .unwrap();
    let identity_second_page = facade
        .list_all_identities(ListAllResourceIdentitiesRequest {
            account: None,
            pagination: PaginationOpts::from_page(1, 2),
        })
        .await
        .unwrap();

    assert_eq!(first_page.len(), 2);
    assert_eq!(second_page.len(), 1);
    assert_eq!(
        second_page
            .iter()
            .map(|summary| summary.name.clone())
            .collect::<Vec<_>>(),
        identity_second_page
            .iter()
            .map(|identity| identity.name.clone())
            .collect::<Vec<_>>()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-103
contract_test!(
    list_all_empty_account_returns_empty,
    super::test_list_all_empty_account_returns_empty
);

pub async fn test_list_all_empty_account_returns_empty(h: &impl FacadeContractHarness) {
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("all-empty-alice", None, &[("K", "v")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Bob);
    let summaries = facade
        .list_all(ListAllResourcesRequest {
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
    let identities = facade
        .list_all_identities(ListAllResourceIdentitiesRequest {
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert!(summaries.is_empty());
    assert!(identities.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
