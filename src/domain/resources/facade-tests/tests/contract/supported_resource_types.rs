// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_configuration::{SecretSetResource, VariableSetResource};
use kamu_resources::{
    ResourceRef,
    ResourceSchemaProvider,
    ResourceSelector,
    TypeName,
    UnsupportedResourceSelectorError,
};
use kamu_resources_facade::{
    DeleteResourceError,
    GetResourceError,
    ListResourcesError,
    SearchResourceHandlesRequest,
    SearchResourcesRequest,
    SpecViewMode,
};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_CANONICAL_SELECTOR,
    apply_manifest_and_get_id,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_unsupported_selector(err: &UnsupportedResourceSelectorError, expected_selector: &str) {
    let UnsupportedResourceSelectorError::NotFound {
        raw_selector: selector,
    } = err;
    assert_eq!(selector.as_str(), expected_selector);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-001
contract_test!(
    lists_supported_resource_types,
    super::test_lists_supported_resource_types
);

pub async fn test_lists_supported_resource_types(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let descriptors = facade.list_supported_resource_types().await.unwrap();

    assert!(
        !descriptors.is_empty(),
        "supported resource types must not be empty"
    );

    for d in &descriptors {
        assert!(
            !d.canonical_selector.as_str().is_empty(),
            "descriptor name must not be empty"
        );
    }

    // Descriptor names are unique
    let names: Vec<&str> = descriptors
        .iter()
        .map(|d| d.canonical_selector.as_str())
        .collect();
    let name_count = names.len();
    let unique_names: std::collections::HashSet<_> = names.into_iter().collect();
    assert_eq!(
        unique_names.len(),
        name_count,
        "descriptor names must be unique"
    );

    // Schemas are unique
    let schemas: Vec<_> = descriptors.iter().map(|d| d.schema.as_str()).collect();
    let schema_count = schemas.len();
    let unique_schemas: std::collections::HashSet<_> = schemas.into_iter().collect();
    assert_eq!(
        unique_schemas.len(),
        schema_count,
        "descriptor schemas must be unique"
    );

    // VariableSet and SecretSet must be present
    let has_variable_set = descriptors
        .iter()
        .any(|d| d.schema == *VariableSetResource::schema());
    let has_secret_set = descriptors
        .iter()
        .any(|d| d.schema == *SecretSetResource::schema());
    assert!(has_variable_set, "VariableSet type must be present");
    assert!(has_secret_set, "SecretSet type must be present");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-002
// Current behavior: the canonical selector (the schema TypeName, e.g.
// "VariableSet") and every registered alias (e.g. "vs", "variablesets")
// resolve case-insensitively to the same type. Only strings that are not
// registered as the canonical name or an alias for any type are rejected
// with UnsupportedSelector — matching is exact-registered-string, not
// inflected (no automatic singular/plural folding).
contract_test!(
    selector_aliases_resolve_consistently,
    super::test_selector_aliases_resolve_consistently
);

pub async fn test_selector_aliases_resolve_consistently(h: &impl FacadeContractHarness) {
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("alias-check", None, &[("K", "v")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);

    // Canonical selector name works for list, list_handles, and get
    let summaries = facade
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: None,
        })
        .await
        .expect("list with canonical selector must succeed");
    for s in &summaries.items {
        assert_eq!(
            s.schema,
            *VariableSetResource::schema(),
            "list schema must be canonical"
        );
    }

    let handles = facade
        .search_handles(SearchResourceHandlesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .expect("list_handles with canonical selector must succeed");
    for i in &handles.items {
        assert_eq!(
            i.r#type,
            *VariableSetResource::schema(),
            "list_handles schema must be canonical"
        );
    }

    facade
        .get(
            ResourceRef {
                account: None,
                r#type: Some(
                    VARIABLE_SET_CANONICAL_SELECTOR
                        .parse::<TypeName>()
                        .unwrap()
                        .into(),
                ),
                id: None,
                did: None,
                name: Some("alias-check".parse().unwrap()),
            },
            SpecViewMode::Encrypted,
        )
        .await
        .expect("get with canonical selector must succeed");

    // Short name "vs" resolves to the canonical VariableSet schema.
    let short_name_summaries = facade
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type("vs".parse().unwrap())],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: None,
        })
        .await
        .expect("short name 'vs' must resolve for list");
    for s in &short_name_summaries.items {
        assert_eq!(
            s.schema,
            *VariableSetResource::schema(),
            "short name list schema must be canonical"
        );
    }

    // The schema TypeName ("VariableSet") is itself the canonical selector today
    // (see VARIABLE_SET_CANONICAL_SELECTOR), so it resolves like any other
    // canonical/alias selector.
    let type_name_summaries = facade
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type("VariableSet".parse().unwrap())],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: None,
        })
        .await
        .expect("schema TypeName 'VariableSet' must resolve for list");
    for s in &type_name_summaries.items {
        assert_eq!(
            s.schema,
            *VariableSetResource::schema(),
            "TypeName selector list schema must be canonical"
        );
    }

    // An unregistered singular-of-plural-only spelling still does not resolve
    // (selectors are exact registered strings, not inflected).
    let bad_result = facade
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                "NoSuchResourceTypeXYZ".parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: None,
        })
        .await;
    match bad_result {
        Err(ListResourcesError::UnsupportedSelector(err)) => {
            assert_unsupported_selector(&err, "NoSuchResourceTypeXYZ");
        }
        other => panic!("unregistered selector must not resolve, got: {other:?}"),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-003
// Unsupported selector rejection behavior by API:
//
// - list / list_handles: UnsupportedSelector (validated before DB query)
// - apply_manifest: UnsupportedDescriptor (validated from manifest)
// - delete (by UID): UnsupportedSelector (validated before UID lookup)
//
// Known gap — get / get_handle by ByName with an unknown resource_type:
//   The facade resolves the UID via a DB name lookup first, passing the raw
//   type string as a filter column.  For an unknown type, nothing matches →
//   LookupProblem(NameNotFound) is returned instead of UnsupportedSelector.
//   This is an implementation detail of the current ByName resolution path.
//   get / get_handle by ById does return UnsupportedSelector because the
// type is validated when the CRUD dispatcher is resolved after the UID is
// known.
contract_test!(
    unsupported_schema_rejected_consistently,
    super::test_unsupported_schema_rejected_consistently
);

pub async fn test_unsupported_schema_rejected_consistently(h: &impl FacadeContractHarness) {
    let id = apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("unsupported-type-base", None, &[("K", "v")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);
    let bad_type = "NoSuchResourceTypeXYZ";
    let bad_schema = "https://example.com/schemas/resources/v1/NoSuchResourceTypeXYZ";

    // get by ByName — unsupported selector is rejected before lookup
    let get_by_name = facade
        .get(
            ResourceRef {
                account: None,
                r#type: Some(bad_type.parse::<TypeName>().unwrap().into()),
                id: None,
                did: None,
                name: Some("unsupported-type-base".parse().unwrap()),
            },
            SpecViewMode::Encrypted,
        )
        .await;
    match get_by_name {
        Err(GetResourceError::UnsupportedSelector(err)) => {
            assert_unsupported_selector(&err, bad_type);
        }
        other => panic!(
            "get by ByName with unknown selector must return UnsupportedSelector, got: {other:?}"
        ),
    }

    // get_handle by ByName — same UnsupportedSelector behavior
    let gi_by_name = facade
        .get_handle(ResourceRef {
            account: None,
            r#type: Some(bad_type.parse::<TypeName>().unwrap().into()),
            id: None,
            did: None,
            name: Some("unsupported-type-base".parse().unwrap()),
        })
        .await;
    match gi_by_name {
        Err(GetResourceError::UnsupportedSelector(err)) => {
            assert_unsupported_selector(&err, bad_type);
        }
        other => panic!(
            "get_handle by ByName with unknown selector must return UnsupportedSelector, got: \
             {other:?}"
        ),
    }

    // list — UnsupportedSelector (type validated before DB query)
    let list_result = facade
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(bad_type.parse().unwrap())],
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
            label_filter: None,
        })
        .await;
    match list_result {
        Err(ListResourcesError::UnsupportedSelector(err)) => {
            assert_unsupported_selector(&err, bad_type);
        }
        other => {
            panic!("list: unsupported selector must return UnsupportedSelector, got: {other:?}")
        }
    }

    // list_handles — UnsupportedSelector
    let li_result = facade
        .search_handles(SearchResourceHandlesRequest {
            selectors: vec![ResourceSelector::of_type(bad_type.parse().unwrap())],
            account: None,
            label_filter: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;
    match li_result {
        Err(ListResourcesError::UnsupportedSelector(err)) => {
            assert_unsupported_selector(&err, bad_type);
        }
        other => panic!(
            "list_handles: unsupported selector must return UnsupportedSelector, got: {other:?}"
        ),
    }

    // apply_manifest — UnsupportedDescriptor
    let bad_manifest =
        format!(r#"{{"$schema":"{bad_schema}","headers":{{"name":"x"}},"spec":{{}}}}"#);
    let apply_result = facade
        .apply_manifest(kamu_resources_facade::ApplyManifestRequest {
            format: kamu_resources_facade::ResourceManifestFormat::Json,
            manifest: bad_manifest,
        })
        .await;
    assert!(
        matches!(
            apply_result,
            Err(kamu_resources_facade::ApplyManifestError::UnsupportedDescriptor(_))
        ),
        "apply_manifest: unsupported type must return UnsupportedDescriptor, got: {apply_result:?}"
    );

    // delete by ById — UnsupportedSelector (type validated after UID is known)
    let delete_result = facade
        .delete(ResourceRef {
            account: None,
            r#type: Some(bad_type.parse::<TypeName>().unwrap().into()),
            id: Some(id),
            did: None,
            name: None,
        })
        .await;
    match delete_result {
        Err(DeleteResourceError::UnsupportedSelector(err)) => {
            assert_unsupported_selector(&err, bad_type);
        }
        other => panic!(
            "delete by ById: unsupported selector must return UnsupportedSelector, got: {other:?}"
        ),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
