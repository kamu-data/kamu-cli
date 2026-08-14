// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_resources::{RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, ResourceID, ResourceSelector};
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
    sorted_summary_names,
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

// RF-087
contract_test!(list_narrowed_by_query, super::test_list_narrowed_by_query);

/// `list` narrows by selector while keeping the rich summary view. Local and
/// remote must agree, since selectors travel over GraphQL.
pub async fn test_list_narrowed_by_query(h: &impl FacadeContractHarness) {
    for name in ["query-app-one", "query-app-two", "query-db-one"] {
        create_resource(h, TestAccount::Alice, name).await;
    }
    create_resource(h, TestAccount::Bob, "query-app-bob").await;

    let facade = h.facade_for(TestAccount::Alice);

    let list = async |selectors: Vec<ResourceSelector>| {
        let mut summaries = facade
            .list(ListResourcesRequest {
                selectors,
                account: None,
                pagination: PaginationOpts::from_max_results(1000),
                label_filter: None,
            })
            .await
            .unwrap();
        normalize_summary_views(&mut summaries);
        summaries
            .into_iter()
            .map(|summary| summary.name.to_string())
            .collect::<Vec<_>>()
    };

    let variable_set = || VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap();

    // A name pattern narrows the listing…
    assert_eq!(
        list(vec![ResourceSelector::name_pattern(
            variable_set(),
            "query-app-%"
        )])
        .await,
        vec!["query-app-one", "query-app-two"]
    );

    // …and stays account-scoped: Bob's matching resource is not visible.
    assert_eq!(
        list(vec![ResourceSelector::name_pattern(
            variable_set(),
            "query-app-bob"
        )])
        .await,
        Vec::<String>::new()
    );

    // A wildcard-free pattern is the exact-name case.
    assert_eq!(
        list(vec![ResourceSelector::name_pattern(
            variable_set(),
            "query-db-one"
        )])
        .await,
        vec!["query-db-one"]
    );

    // By id.
    let id = apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("query-by-id", None, &[("K", "v")]),
    )
    .await;
    assert_eq!(
        list(ResourceSelector::ids_of_type(&variable_set(), [id])).await,
        vec!["query-by-id"]
    );

    // Unlike `search`, `list` renders typed columns through one type's
    // dispatcher, so it needs exactly one typed selector. An empty list has no
    // type to render, and is rejected rather than treated as vacuous — the
    // vacuous-empty case belongs to `search` (RF-094).
    //
    // Asserted on the message rather than the variant: remote surfaces this as
    // a transport-level GraphQL error, since the limitation is temporary
    // (`list` folds into a multi-type `search`) and does not warrant a
    // dedicated problem type in the schema.
    let err = facade
        .list(ListResourcesRequest {
            selectors: Vec::new(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: None,
        })
        .await
        .expect_err("an empty selector list has no type to render");
    assert!(
        err.to_string().contains("exactly one typed selector"),
        "unexpected error: {err:?}"
    );

    // An unnarrowed selector still lists the whole type.
    assert!(
        list(vec![ResourceSelector::of_type(variable_set())])
            .await
            .len()
            >= 4
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-088
contract_test!(
    list_handles_honours_selectors,
    super::test_list_handles_honours_selectors
);

/// `list_handles` used to hardcode an unnarrowed scope while its sibling `list`
/// accepted a query. Unifying on selectors removed that asymmetry — this pins
/// the new behaviour so it is not mistaken for an accident and quietly
/// reverted.
pub async fn test_list_handles_honours_selectors(h: &impl FacadeContractHarness) {
    for name in ["handles-app-one", "handles-app-two", "handles-db-one"] {
        create_resource(h, TestAccount::Alice, name).await;
    }

    let facade = h.facade_for(TestAccount::Alice);

    let list_handles = async |selectors: Vec<ResourceSelector>| {
        let mut handles = facade
            .list_handles(ListResourceHandlesRequest {
                selectors,
                account: None,
                label_filter: None,
                pagination: PaginationOpts::from_max_results(1000),
            })
            .await
            .unwrap();
        normalize_handles(&mut handles);
        sorted_handle_names(handles)
    };

    let variable_set = || VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap();

    // A name pattern narrows…
    assert_eq!(
        list_handles(vec![ResourceSelector::name_pattern(
            variable_set(),
            "handles-app-%"
        )])
        .await,
        vec!["handles-app-one", "handles-app-two"]
    );

    // …and an unnarrowed selector still returns the whole type, so the
    // narrowing above is the selector's doing rather than an empty result.
    let all = list_handles(vec![ResourceSelector::of_type(variable_set())]).await;
    assert!(
        all.len() >= 3,
        "an unnarrowed selector must span the whole type, got {all:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-105
contract_test!(
    per_selector_account_is_authorized,
    super::test_per_selector_account_is_authorized
);

/// A selector naming another account is **denied** for a non-admin, and the
/// denial must not reveal whether that account or its resources exist.
///
/// The whole call fails rather than silently dropping the unauthorized
/// selector: a partial result would narrow the caller's request without saying
/// so, which is the same class of bug as ignoring the field entirely.
pub async fn test_per_selector_account_is_authorized(h: &impl FacadeContractHarness) {
    create_resource(h, TestAccount::Bob, "bob-secret").await;

    let facade = h.facade_for(TestAccount::Alice);

    let bobs_selector = ResourceSelector {
        account: Some(kamu_resources::ResourceAccountRef {
            id: None,
            did: None,
            name: Some(h.account_name(TestAccount::Bob)),
        }),
        ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
    };

    let err = facade
        .search_handles(SearchResourceHandlesRequest {
            selectors: vec![bobs_selector.clone()],
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .expect_err("a non-admin must not read another account's resources");
    assert!(
        !err.to_string().contains("bob-secret"),
        "the denial must not leak resource names: {err}"
    );

    // Pairing it with a selector the caller *is* allowed to read must still fail
    // the whole call — no partial results.
    create_resource(h, TestAccount::Alice, "alice-own").await;
    facade
        .search_handles(SearchResourceHandlesRequest {
            selectors: vec![
                ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap()),
                bobs_selector,
            ],
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .expect_err("one denied selector must fail the whole call");

    // The caller's own resources stay readable, so the denial above is about
    // authorization rather than the selector shape.
    let response = facade
        .search_handles(SearchResourceHandlesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .expect("the caller's own resources must remain readable");
    assert_eq!(sorted_handle_names(response.items), vec!["alice-own"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-106
contract_test!(
    any_type_selector_scope_limits,
    super::test_any_type_selector_scope_limits
);

/// The two `AnyType` limits of `ResourceScope`, which carries a single query
/// rather than a per-type list. Both became reachable when listing started
/// taking selectors, and both disappear once every row carries its own type.
///
/// Asserted on the message rather than the variant: remote surfaces these as
/// transport-level GraphQL errors, and they are temporary.
pub async fn test_any_type_selector_scope_limits(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let search = async |selectors: Vec<ResourceSelector>| {
        facade
            .search_handles(SearchResourceHandlesRequest {
                selectors,
                account: None,
                label_filter: None,
                pagination: PaginationOpts::from_max_results(1000),
            })
            .await
    };

    // A type-less selector already spans every type, so pairing it with a typed
    // one cannot be expressed as per-type rows.
    let err = search(vec![
        ResourceSelector::any_type_name_pattern("mixed-%"),
        ResourceSelector::name_pattern(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(), "mixed-%"),
    ])
    .await
    .expect_err("a type-less selector cannot be combined with typed selectors");
    assert!(
        err.to_string().contains("cannot be combined with typed"),
        "unexpected error: {err:?}"
    );

    // Two type-less selectors narrowing by different modes need two queries,
    // but `AnyType` carries only one.
    let err = search(vec![
        ResourceSelector::any_type_name_pattern("modes-%"),
        ResourceSelector::any_type_id(ResourceID::new(uuid::Uuid::new_v4())),
    ])
    .await
    .expect_err("a type-less selector may narrow by only one mode");
    assert!(
        err.to_string().contains("only one of"),
        "unexpected error: {err:?}"
    );

    // `AnyType` carries no per-row account, so a type-less selector naming one
    // is rejected rather than silently scoped to the caller.
    //
    // Named as the *caller's own* account deliberately: authorization runs
    // before coalescing, so naming another account would be denied there and
    // this representability limit would never be reached.
    let err = search(vec![ResourceSelector {
        account: Some(kamu_resources::ResourceAccountRef {
            id: None,
            did: None,
            name: Some(h.account_name(TestAccount::Alice)),
        }),
        ..ResourceSelector::any_type_name_pattern("account-%")
    }])
    .await
    .expect_err("a type-less selector cannot name an account");
    assert!(
        err.to_string().contains("cannot name an account"),
        "unexpected error: {err:?}"
    );

    // A single type-less selector with one mode stays representable.
    search(vec![ResourceSelector::any_type_name_pattern("fine-%")])
        .await
        .expect("one type-less selector with one mode is representable");
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
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
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
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
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
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
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
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_page(0, 2),
            label_filter: None,
        })
        .await
        .unwrap();
    let second_page = facade
        .list(ListResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
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
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_page(1, 2),
            label_filter: None,
        })
        .await
        .unwrap();
    let handles = facade
        .list_handles(ListResourceHandlesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
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
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: None,
        })
        .await
        .unwrap();
    let handles = facade
        .list_handles(ListResourceHandlesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
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
            selectors: vec![ResourceSelector::of_type(
                unsupported_selector.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: None,
        })
        .await;
    let handles = facade
        .list_handles(ListResourceHandlesRequest {
            selectors: vec![ResourceSelector::of_type(
                unsupported_selector.parse().unwrap(),
            )],
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
            selectors: vec![
                ResourceSelector::name_pattern(
                    VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                    "search-exact-alpha",
                ),
                ResourceSelector::name_pattern(
                    VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                    "search-exact-beta",
                ),
            ],
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
            selectors: vec![
                ResourceSelector::name_pattern(
                    VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                    "search-missing-present",
                ),
                ResourceSelector::name_pattern(
                    VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                    "search-missing-absent",
                ),
            ],
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

// RF-091A
contract_test!(search_by_exact_ids, super::test_search_by_exact_ids);

pub async fn test_search_by_exact_ids(h: &impl FacadeContractHarness) {
    let alpha_id = apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("search-id-alpha", None, &[("K", "v")]),
    )
    .await;
    let beta_id = apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("search-id-beta", None, &[("K", "v")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);
    let response = facade
        .search_handles(SearchResourceHandlesRequest {
            selectors: ResourceSelector::ids_of_type(
                &VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                [alpha_id, beta_id],
            ),
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(response.total_count, 2);
    assert_eq!(
        sorted_handle_names(response.items),
        vec!["search-id-alpha", "search-id-beta"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-091B
contract_test!(
    search_exact_ids_ignores_missing,
    super::test_search_exact_ids_ignores_missing
);

pub async fn test_search_exact_ids_ignores_missing(h: &impl FacadeContractHarness) {
    let present_id = apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("search-id-missing-present", None, &[("K", "v")]),
    )
    .await;
    let missing_id = ResourceID::new(uuid::Uuid::new_v4());

    let facade = h.facade_for(TestAccount::Alice);
    let response = facade
        .search_handles(SearchResourceHandlesRequest {
            selectors: ResourceSelector::ids_of_type(
                &VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                [present_id, missing_id],
            ),
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(response.total_count, 1);
    assert_eq!(
        sorted_handle_names(response.items),
        vec!["search-id-missing-present"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-091C
contract_test!(
    search_exact_ids_account_scoping,
    super::test_search_exact_ids_account_scoping
);

pub async fn test_search_exact_ids_account_scoping(h: &impl FacadeContractHarness) {
    let bob_id = apply_manifest_and_get_id(
        h,
        TestAccount::Bob,
        variable_set_manifest_json("search-id-bobs", None, &[("K", "v")]),
    )
    .await;

    // Alice searches by an id that genuinely exists, but is owned by Bob —
    // it must not leak across the account boundary.
    let facade = h.facade_for(TestAccount::Alice);
    let response = facade
        .search_handles(SearchResourceHandlesRequest {
            selectors: ResourceSelector::ids_of_type(
                &VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                [bob_id],
            ),
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(response.items, Vec::new());
    assert_eq!(response.total_count, 0);
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
            selectors: vec![ResourceSelector::name_pattern(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                "search-pattern-%".to_string(),
            )],
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
            selectors: vec![
                ResourceSelector::name_pattern(
                    VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                    "multi-type-%",
                ),
                ResourceSelector::name_pattern(
                    SECRET_SET_CANONICAL_SELECTOR.parse().unwrap(),
                    "multi-type-%",
                ),
            ],
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

// RF-169
contract_test!(search_any_type, super::test_search_any_type);

pub async fn test_search_any_type(h: &impl FacadeContractHarness) {
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("any-type-vs", None, &[("K", "v")]),
    )
    .await;
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        secret_set_manifest_json("any-type-ss", None, &[("K", "v")]),
    )
    .await;
    // A resource under a different account must not leak into the results.
    apply_manifest_and_get_id(
        h,
        TestAccount::Bob,
        variable_set_manifest_json("any-type-vs", None, &[("K", "v")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);
    let response = facade
        .search_handles(SearchResourceHandlesRequest {
            selectors: vec![ResourceSelector::any_type_name_pattern("any-type-%")],
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(
        response.total_count, 2,
        "AnyType must find matches across every schema without a type selector"
    );

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

    for item in &response.items {
        assert_eq!(
            item.account.name,
            h.account_name(TestAccount::Alice),
            "AnyType must still respect account scoping"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-094
contract_test!(
    search_empty_exact_names_returns_no_matches,
    super::test_search_empty_exact_names_returns_no_matches
);

// An empty selector list is valid but vacuous — it matches nothing rather than
// erroring. Note this is *not* "match everything": that requires an explicit
// type-less, unnarrowed selector.
pub async fn test_search_empty_exact_names_returns_no_matches(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .search_handles(SearchResourceHandlesRequest {
            selectors: Vec::new(),
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(response.items, Vec::new());
    assert_eq!(response.total_count, 0);
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
            selectors: vec![ResourceSelector::name_pattern(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                "search-page-%".to_string(),
            )],
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
            selectors: vec![ResourceSelector::name_pattern(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                "search-scope-%".to_string(),
            )],
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
    let bob_response = h
        .facade_for(TestAccount::Bob)
        .search_handles(SearchResourceHandlesRequest {
            selectors: vec![ResourceSelector::name_pattern(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                "search-scope-%".to_string(),
            )],
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

// RF-097
contract_test!(
    list_filter_by_canonical_label_uri,
    super::test_list_filter_by_canonical_label_uri
);

pub async fn test_list_filter_by_canonical_label_uri(h: &impl FacadeContractHarness) {
    create_variable_set_with_labels(
        h,
        TestAccount::Alice,
        "list-filter-prod",
        &[(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, "prod".into())],
    )
    .await;
    create_variable_set_with_labels(
        h,
        TestAccount::Alice,
        "list-filter-staging",
        &[(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, "staging".into())],
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);

    let items = facade
        .list(ListResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: Some(label_filter(&[(
                RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
                "prod",
            )])),
        })
        .await
        .unwrap();

    assert_eq!(
        sorted_summary_names(items),
        vec!["list-filter-prod"],
        "only the matching value must survive the filter"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-098
contract_test!(
    list_filter_by_short_label_name,
    super::test_list_filter_by_short_label_name
);

pub async fn test_list_filter_by_short_label_name(h: &impl FacadeContractHarness) {
    create_variable_set_with_labels(
        h,
        TestAccount::Alice,
        "list-filter-short-prod",
        &[(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, "prod".into())],
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);

    let items = facade
        .list(ListResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: Some(label_filter(&[("environment", "prod")])),
        })
        .await
        .unwrap();

    assert_eq!(sorted_summary_names(items), vec!["list-filter-short-prod"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-099
contract_test!(
    list_filter_by_free_form_label,
    super::test_list_filter_by_free_form_label
);

pub async fn test_list_filter_by_free_form_label(h: &impl FacadeContractHarness) {
    create_variable_set_with_labels(
        h,
        TestAccount::Alice,
        "list-filter-freeform-a",
        &[("team", "data".into())],
    )
    .await;
    create_variable_set_with_labels(
        h,
        TestAccount::Alice,
        "list-filter-freeform-b",
        &[("team", "platform".into())],
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);

    let items = facade
        .list(ListResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: Some(label_filter(&[("team", "data")])),
        })
        .await
        .unwrap();

    assert_eq!(sorted_summary_names(items), vec!["list-filter-freeform-a"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-099A
contract_test!(
    list_filter_invalid_key_is_rejected,
    super::test_list_filter_invalid_key_is_rejected
);

pub async fn test_list_filter_invalid_key_is_rejected(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .list(ListResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
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
contract_test!(
    list_filter_unknown_uri_is_rejected,
    super::test_list_filter_unknown_uri_is_rejected
);

pub async fn test_list_filter_unknown_uri_is_rejected(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .list(ListResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
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
contract_test!(
    list_filter_non_string_value_is_rejected,
    super::test_list_filter_non_string_value_is_rejected
);

pub async fn test_list_filter_non_string_value_is_rejected(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .list(ListResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
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
contract_test!(
    list_filter_duplicate_after_canonicalization_is_rejected,
    super::test_list_filter_duplicate_after_canonicalization_is_rejected
);

pub async fn test_list_filter_duplicate_after_canonicalization_is_rejected(
    h: &impl FacadeContractHarness,
) {
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .list(ListResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
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
contract_test!(
    list_filter_not_operator_is_rejected,
    super::test_list_filter_not_operator_is_rejected
);

pub async fn test_list_filter_not_operator_is_rejected(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .list(ListResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
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
contract_test!(
    list_filter_or_operator_is_rejected,
    super::test_list_filter_or_operator_is_rejected
);

pub async fn test_list_filter_or_operator_is_rejected(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .list(ListResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
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
contract_test!(
    list_filter_malformed_not_operator_is_rejected,
    super::test_list_filter_malformed_not_operator_is_rejected
);

pub async fn test_list_filter_malformed_not_operator_is_rejected(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .list(ListResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
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

// RF-099H
contract_test!(
    search_handles_filter_narrows_candidates,
    super::test_search_handles_filter_narrows_candidates
);

pub async fn test_search_handles_filter_narrows_candidates(h: &impl FacadeContractHarness) {
    create_variable_set_with_labels(
        h,
        TestAccount::Alice,
        "search-filter-prod",
        &[(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, "prod".into())],
    )
    .await;
    create_variable_set_with_labels(
        h,
        TestAccount::Alice,
        "search-filter-staging",
        &[(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, "staging".into())],
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .search_handles(SearchResourceHandlesRequest {
            selectors: vec![ResourceSelector::name_pattern(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                "search-filter-%".to_string(),
            )],
            account: None,
            label_filter: Some(label_filter(&[("environment", "prod")])),
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(
        sorted_handle_names(response.items),
        vec!["search-filter-prod"]
    );
    assert_eq!(response.total_count, 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
