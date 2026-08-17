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
    ListResourcesError,
    ResourceLabelFilterProblemCode,
    SearchResourcesRequest,
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
    summary_column_pairs,
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
contract_test!(
    search_narrowed_by_query,
    super::test_search_narrowed_by_query
);

/// `search` narrows by selector while keeping the rich summary view. Local and
/// remote must agree, since selectors travel over GraphQL.
pub async fn test_search_narrowed_by_query(h: &impl FacadeContractHarness) {
    for name in ["query-app-one", "query-app-two", "query-db-one"] {
        create_resource(h, TestAccount::Alice, name).await;
    }
    create_resource(h, TestAccount::Bob, "query-app-bob").await;

    let facade = h.facade_for(TestAccount::Alice);

    let list = async |selectors: Vec<ResourceSelector>| {
        let mut summaries = facade
            .search(SearchResourcesRequest {
                selectors,
                account: None,
                pagination: PaginationOpts::from_max_results(1000),
            })
            .await
            .unwrap()
            .items;
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

    // An empty selector list matches nothing and is **not** an error.
    //
    // This changed with the listing collapse. The former `list` rendered typed
    // columns through one type's dispatcher, so it required exactly one typed
    // selector and rejected an empty list with `SingleTypeRequired` — a variant
    // that no longer exists. `search` computes columns per result instead, so
    // it has no reason to demand a type, and an explicit "no selectors" now
    // narrows to zero the same way it always did for `search_handles` (RF-094).
    assert_eq!(list(Vec::new()).await, Vec::<String>::new());

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
    search_handles_honours_selectors,
    super::test_search_handles_honours_selectors
);

/// `list_handles` used to hardcode an unnarrowed scope while its sibling `list`
/// accepted a query. Unifying on selectors removed that asymmetry, and the
/// collapse into `search_handles` kept it — this pins the behaviour so it is
/// not mistaken for an accident and quietly reverted.
pub async fn test_search_handles_honours_selectors(h: &impl FacadeContractHarness) {
    for name in ["handles-app-one", "handles-app-two", "handles-db-one"] {
        create_resource(h, TestAccount::Alice, name).await;
    }

    let facade = h.facade_for(TestAccount::Alice);

    let list_handles = async |selectors: Vec<ResourceSelector>| {
        let mut handles = facade
            .search_handles(SearchResourcesRequest {
                selectors,
                account: None,
                pagination: PaginationOpts::from_max_results(1000),
            })
            .await
            .unwrap()
            .items;
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
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                labels: Some(label_filter(&[(
                    RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
                    "prod",
                )])),
                ..bobs_selector.clone()
            }],
            account: None,
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
        .search_handles(SearchResourcesRequest {
            selectors: vec![
                ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap()),
                bobs_selector.clone(),
            ],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .expect_err("one denied selector must fail the whole call");

    // The summary-returning form must honour the field too. Before the listing
    // collapse this went through `list`, which resolved its own account instead
    // of going through the scope resolver and so silently answered against the
    // *caller's* account — returning Alice's resources for a request naming
    // Bob's. That hole closed when `list` folded into `search`.
    let denied = facade
        .search(SearchResourcesRequest {
            selectors: vec![bobs_selector],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;
    assert_matches!(
        denied,
        Err(_),
        "`search` must not ignore a per-selector account"
    );

    // The caller's own resources stay readable, so the denial above is about
    // authorization rather than the selector shape.
    let response = facade
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
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
            .search_handles(SearchResourcesRequest {
                selectors,
                account: None,
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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None, // default = alice
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap()
        .items;

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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_page(0, 2),
        })
        .await
        .unwrap()
        .items;

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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_page(0, 2),
        })
        .await
        .unwrap()
        .items;
    let second_page = facade
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_page(1, 2),
        })
        .await
        .unwrap()
        .items;

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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_page(1, 2),
        })
        .await
        .unwrap()
        .items;
    let handles = facade
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_page(1, 2),
        })
        .await
        .unwrap()
        .items;

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
    let handles = facade
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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                unsupported_selector.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;
    let handles = facade
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                unsupported_selector.parse().unwrap(),
            )],
            account: None,
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
        .search_handles(SearchResourcesRequest {
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
        .search_handles(SearchResourcesRequest {
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
        .search_handles(SearchResourcesRequest {
            selectors: ResourceSelector::ids_of_type(
                &VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                [alpha_id, beta_id],
            ),
            account: None,
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
        .search_handles(SearchResourcesRequest {
            selectors: ResourceSelector::ids_of_type(
                &VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                [present_id, missing_id],
            ),
            account: None,
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
        .search_handles(SearchResourcesRequest {
            selectors: ResourceSelector::ids_of_type(
                &VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                [bob_id],
            ),
            account: None,
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
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector::name_pattern(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                "search-pattern-%".to_string(),
            )],
            account: None,
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
        .search_handles(SearchResourcesRequest {
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
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector::any_type_name_pattern("any-type-%")],
            account: None,
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
        .search_handles(SearchResourcesRequest {
            selectors: Vec::new(),
            account: None,
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
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector::name_pattern(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                "search-page-%".to_string(),
            )],
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
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector::name_pattern(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                "search-scope-%".to_string(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
    let bob_response = h
        .facade_for(TestAccount::Bob)
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector::name_pattern(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                "search-scope-%".to_string(),
            )],
            account: None,
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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                labels: Some(label_filter(&[(
                    RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
                    "prod",
                )])),
                ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
            }],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap()
        .items;

    assert_eq!(
        sorted_summary_names(items),
        vec!["list-filter-prod"],
        "only the matching value must survive the filter"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-175
contract_test!(
    list_filter_differs_per_selector,
    super::test_list_filter_differs_per_selector
);

/// The capability per-selector labels exist for: one call, two selectors, each
/// filtering by a *different* label.
///
/// This is not expressible with a single call-level filter, which is why the
/// label pairs had to move inside the repository's per-row scope. The whole
/// result must be the union of the two independently-filtered selectors, and
/// `total_count` must span both.
pub async fn test_list_filter_differs_per_selector(h: &impl FacadeContractHarness) {
    for (name, environment) in [
        ("per-sel-alpha-prod", "prod"),
        ("per-sel-alpha-staging", "staging"),
        ("per-sel-beta-prod", "prod"),
        ("per-sel-beta-staging", "staging"),
    ] {
        create_variable_set_with_labels(
            h,
            TestAccount::Alice,
            name,
            &[(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, environment.into())],
        )
        .await;
    }

    let facade = h.facade_for(TestAccount::Alice);
    let variable_set = || VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap();

    let response = facade
        .search_handles(SearchResourcesRequest {
            selectors: vec![
                // `per-sel-alpha-%`, but only the prod one…
                ResourceSelector {
                    labels: Some(label_filter(&[("environment", "prod")])),
                    ..ResourceSelector::name_pattern(variable_set(), "per-sel-alpha-%")
                },
                // …and `per-sel-beta-%`, but only the staging one.
                ResourceSelector {
                    labels: Some(label_filter(&[("environment", "staging")])),
                    ..ResourceSelector::name_pattern(variable_set(), "per-sel-beta-%")
                },
            ],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(
        sorted_handle_names(response.items),
        vec!["per-sel-alpha-prod", "per-sel-beta-staging"],
        "each selector must apply only its own label filter"
    );
    assert_eq!(response.total_count, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-176
contract_test!(
    list_filter_one_selector_unfiltered,
    super::test_list_filter_one_selector_unfiltered
);

/// A labelled selector beside an unlabelled one: the filter must not leak onto
/// the neighbour.
///
/// This is the failure mode the coalescer's grouping key guards. Merging the
/// two rows would make the unfiltered selector inherit the other's labels,
/// silently narrowing it.
pub async fn test_list_filter_one_selector_unfiltered(h: &impl FacadeContractHarness) {
    for (name, environment) in [
        ("mixed-alpha-prod", "prod"),
        ("mixed-alpha-staging", "staging"),
        ("mixed-beta-prod", "prod"),
        ("mixed-beta-staging", "staging"),
    ] {
        create_variable_set_with_labels(
            h,
            TestAccount::Alice,
            name,
            &[(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, environment.into())],
        )
        .await;
    }

    let facade = h.facade_for(TestAccount::Alice);
    let variable_set = || VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap();

    let response = facade
        .search_handles(SearchResourcesRequest {
            selectors: vec![
                ResourceSelector {
                    labels: Some(label_filter(&[("environment", "prod")])),
                    ..ResourceSelector::name_pattern(variable_set(), "mixed-alpha-%")
                },
                // No labels: both of these must come back.
                ResourceSelector::name_pattern(variable_set(), "mixed-beta-%"),
            ],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();

    assert_eq!(
        sorted_handle_names(response.items),
        vec!["mixed-alpha-prod", "mixed-beta-prod", "mixed-beta-staging"],
        "the label filter must not leak onto the unfiltered selector"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-177
contract_test!(
    list_filter_non_string_value_fails_whole_call,
    super::test_list_filter_non_string_value_fails_whole_call
);

/// A bad label value on **one** selector fails the whole call, rather than
/// degrading into "that selector matched nothing".
///
/// Only top-level string-valued labels are indexed, so a complex-JSON predicate
/// is unsatisfiable by construction. Silently returning the *other* selector's
/// rows would hide the authoring mistake behind a plausible-looking result.
pub async fn test_list_filter_non_string_value_fails_whole_call(h: &impl FacadeContractHarness) {
    create_variable_set_with_labels(
        h,
        TestAccount::Alice,
        "blast-radius-prod",
        &[(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, "prod".into())],
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);
    let variable_set = || VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap();

    let result = facade
        .search(SearchResourcesRequest {
            selectors: vec![
                // Perfectly valid, and would match on its own.
                ResourceSelector {
                    labels: Some(label_filter(&[("environment", "prod")])),
                    ..ResourceSelector::name_pattern(variable_set(), "blast-radius-%")
                },
                // Only this one is malformed.
                ResourceSelector {
                    labels: Some(kamu_resources::ResourceLabelFilterInput {
                        entries: std::collections::BTreeMap::from([(
                            "environment".to_string(),
                            serde_json::json!({"not": "a string"}),
                        )]),
                    }),
                    ..ResourceSelector::of_type(variable_set())
                },
            ],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;

    let Err(ListResourcesError::InvalidLabelFilter(err)) = result else {
        panic!("expected InvalidLabelFilter, got {result:?}");
    };
    assert_eq!(err.code, ResourceLabelFilterProblemCode::NonStringValue);
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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                labels: Some(label_filter(&[("environment", "prod")])),
                ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
            }],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap()
        .items;

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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                labels: Some(label_filter(&[("team", "data")])),
                ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
            }],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap()
        .items;

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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                labels: Some(label_filter(&[("not a valid key=", "x")])),
                ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
            }],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                labels: Some(label_filter(&[(
                    "https://kamu.dev/schemas/resource/v1alpha1/labels/DoesNotExist",
                    "x",
                )])),
                ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
            }],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                labels: Some(kamu_resources::ResourceLabelFilterInput {
                    entries: std::collections::BTreeMap::from([(
                        "environment".to_string(),
                        serde_json::json!({"not": "a string"}),
                    )]),
                }),
                ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
            }],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                labels: Some(label_filter(&[
                    ("environment", "prod"),
                    (RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI, "prod"),
                ])),
                ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
            }],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                labels: Some(kamu_resources::ResourceLabelFilterInput {
                    entries: std::collections::BTreeMap::from([(
                        "$not".to_string(),
                        serde_json::json!({"environment": "prod"}),
                    )]),
                }),
                ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
            }],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                labels: Some(kamu_resources::ResourceLabelFilterInput {
                    entries: std::collections::BTreeMap::from([(
                        "$or".to_string(),
                        serde_json::json!([{"environment": "prod"}, {"environment": "staging"}]),
                    )]),
                }),
                ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
            }],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
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
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                labels: Some(kamu_resources::ResourceLabelFilterInput {
                    entries: std::collections::BTreeMap::from([(
                        "$not".to_string(),
                        serde_json::json!("not-an-object"),
                    )]),
                }),
                ..ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap())
            }],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
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
        .search_handles(SearchResourcesRequest {
            selectors: vec![ResourceSelector {
                labels: Some(label_filter(&[("environment", "prod")])),
                ..ResourceSelector::name_pattern(
                    VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
                    "search-filter-%".to_string(),
                )
            }],
            account: None,
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

// RF-107
contract_test!(
    search_renders_typed_columns_across_types,
    super::test_search_renders_typed_columns_across_types
);

/// Typed list columns are the schema-specific values the CLI's `list` table
/// shows beyond the generic columns — `variables` for a `VariableSet`,
/// `secrets` for a `SecretSet`.
///
/// Nothing pinned them before this test: no contract test and no E2E test read
/// `list_values`, so the columns `kamu list` prints were free to vanish
/// silently. They are the one thing the listing collapse must not lose, since
/// `search` replaces the dispatcher path that used to produce them.
///
/// Rendering them for a **multi-type** result is new: the retired `list_all`
/// returned an empty `list_values` for every row, so `kamu list` across types
/// showed no typed columns at all.
pub async fn test_search_renders_typed_columns_across_types(h: &impl FacadeContractHarness) {
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("columns-vars", None, &[("A", "1"), ("B", "2")]),
    )
    .await;
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        secret_set_manifest_json("columns-secrets", None, &[("TOKEN", "t")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);

    let mut response = facade
        .search(SearchResourcesRequest {
            selectors: vec![
                ResourceSelector::of_type(VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap()),
                ResourceSelector::of_type(SECRET_SET_CANONICAL_SELECTOR.parse().unwrap()),
            ],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .unwrap();
    normalize_summary_views(&mut response.items);

    let column_pairs = |name: &str| {
        response
            .items
            .iter()
            .find(|summary| summary.name.as_str() == name)
            .map(summary_column_pairs)
            .unwrap_or_else(|| panic!("{name} must be listed"))
    };

    // Each type renders its own columns, and the values are derived from the
    // spec — two variables, one secret — rather than merely being present.
    assert_eq!(column_pairs("columns-vars"), vec!["variables=2"]);
    assert_eq!(column_pairs("columns-secrets"), vec!["secrets=1"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-174
contract_test!(
    selector_field_the_facade_cannot_resolve_is_rejected,
    super::test_selector_field_the_facade_cannot_resolve_is_rejected
);

/// A selector narrowed *only* by a field the facade cannot resolve must fail,
/// not widen.
///
/// Dropping the field leaves a selector that reads as unnarrowed, which matches
/// every resource in the account — the caller asks for a subset and silently
/// receives everything. The trait is public, so the rejection has to live at
/// the facade rather than only in the GraphQL adapter.
pub async fn test_selector_field_the_facade_cannot_resolve_is_rejected(
    h: &impl FacadeContractHarness,
) {
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("unresolvable-field-probe", None, &[("A", "1")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);

    // Narrowed by nothing the facade can act on: were the `did` dropped, this
    // would come back as the whole account.
    let did_only = ResourceSelector {
        did: Some(odf::AccountID::new_seeded_ed25519(b"probe").into()),
        ..ResourceSelector::default()
    };

    assert_matches!(
        facade
            .search_handles(SearchResourcesRequest {
                selectors: vec![did_only.clone()],
                account: None,
                pagination: PaginationOpts::from_max_results(1000),
            })
            .await,
        Err(_),
        "a selector narrowed only by an unresolvable field must be rejected, not widened"
    );

    assert_matches!(
        facade
            .search(SearchResourcesRequest {
                selectors: vec![did_only],
                account: None,
                pagination: PaginationOpts::from_max_results(1000),
            })
            .await,
        Err(_),
        "`search` must reject it too, not only `search_handles`"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
