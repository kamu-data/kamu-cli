// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::HashSet;

use kamu_configuration::{SecretSetResource, VariableSetResource};
use kamu_resources::{
    ResourceRef,
    ResourceSchemaProvider,
    ResourceSelector,
    ResourceVisibility,
    TypeName,
    UnsupportedResourceSelectorError,
};
use kamu_resources_facade::{
    ApplyManifestError,
    ApplyManifestRequest,
    BatchResourceError,
    ListResourcesError,
    ResourceManifestFormat,
    SpecViewOpts,
};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_CANONICAL_SELECTOR,
    apply_manifest_and_get_id,
    assert_single_batch_success,
    search_request,
    variable_set_manifest_json,
};

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
    let unique_names: HashSet<_> = names.into_iter().collect();
    assert_eq!(
        unique_names.len(),
        name_count,
        "descriptor names must be unique"
    );

    // Schemas are unique
    let schemas: Vec<_> = descriptors.iter().map(|d| d.schema.as_str()).collect();
    let schema_count = schemas.len();
    let unique_schemas: HashSet<_> = schemas.into_iter().collect();
    assert_eq!(
        unique_schemas.len(),
        schema_count,
        "descriptor schemas must be unique"
    );

    // VariableSet and SecretSet must be present
    let has_variable_set = descriptors
        .iter()
        .any(|d| d.schema == *VariableSetResource::type_uri());
    let has_secret_set = descriptors
        .iter()
        .any(|d| d.schema == *SecretSetResource::type_uri());
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

    // Canonical selector name works for search, search_handles, and get
    let summaries = facade
        .search(search_request(vec![ResourceSelector::of_type(
            VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
        )]))
        .await
        .expect("search with canonical selector must succeed");
    for s in &summaries.items {
        assert_eq!(
            s.schema,
            *VariableSetResource::type_uri(),
            "search schema must be canonical"
        );
    }

    let handles = facade
        .search_handles(search_request(vec![ResourceSelector::of_type(
            VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
        )]))
        .await
        .expect("search_handles with canonical selector must succeed");
    for i in &handles.items {
        assert_eq!(
            i.r#type,
            *VariableSetResource::type_uri(),
            "search_handles schema must be canonical"
        );
    }

    assert_single_batch_success(
        facade
            .get(
                vec![ResourceRef {
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
                }],
                SpecViewOpts::ENCRYPTED,
                ResourceVisibility::Active,
            )
            .await
            .expect("get with canonical selector must succeed"),
    );

    // Short name "vs" resolves to the canonical VariableSet schema.
    let short_name_summaries = facade
        .search(search_request(vec![ResourceSelector::of_type(
            "vs".parse().unwrap(),
        )]))
        .await
        .expect("short name 'vs' must resolve for search");
    for s in &short_name_summaries.items {
        assert_eq!(
            s.schema,
            *VariableSetResource::type_uri(),
            "short name search schema must be canonical"
        );
    }

    // The schema TypeName ("VariableSet") is itself the canonical selector today
    // (see VARIABLE_SET_CANONICAL_SELECTOR), so it resolves like any other
    // canonical/alias selector.
    let type_name_summaries = facade
        .search(search_request(vec![ResourceSelector::of_type(
            "VariableSet".parse().unwrap(),
        )]))
        .await
        .expect("schema TypeName 'VariableSet' must resolve for search");
    for s in &type_name_summaries.items {
        assert_eq!(
            s.schema,
            *VariableSetResource::type_uri(),
            "TypeName selector search schema must be canonical"
        );
    }

    // An unregistered singular-of-plural-only spelling still does not resolve
    // (selectors are exact registered strings, not inflected).
    let bad_result = facade
        .search(search_request(vec![ResourceSelector::of_type(
            "NoSuchResourceTypeXYZ".parse().unwrap(),
        )]))
        .await;
    assert_matches!(
        bad_result,
        Err(ListResourcesError::UnsupportedSelector(
            UnsupportedResourceSelectorError::NotFound { raw_selector }
        )) if raw_selector.as_str() == "NoSuchResourceTypeXYZ",
        "unregistered selector must not resolve"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-003
// Unsupported type rejection behaviour, which is uniform across the API except
// for `apply_manifest`:
//
// - get / get_handles by name: UnsupportedSelector (type resolved against the descriptor list
//   before any lookup)
// - search / search_handles: UnsupportedSelector (validated before DB query)
// - delete by id: UnsupportedSelector (type validated when the CRUD dispatcher is resolved, after
//   the id is known)
// - apply_manifest: UnsupportedDescriptor — the odd one out, since the type arrives inside the
//   manifest rather than as a selector
//
// The name paths resolve the type up front rather than via a DB name lookup:
// otherwise an unknown type would match nothing and report the miss as
// LookupProblem(NameNotFound), which is what the first two assertions pin.
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
            vec![ResourceRef {
                account: None,
                r#type: Some(bad_type.parse::<TypeName>().unwrap().into()),
                id: None,
                did: None,
                name: Some("unsupported-type-base".parse().unwrap()),
            }],
            SpecViewOpts::ENCRYPTED,
            ResourceVisibility::Active,
        )
        .await;
    assert_matches!(
        get_by_name,
        Err(BatchResourceError::UnsupportedSelector(
            UnsupportedResourceSelectorError::NotFound { raw_selector }
        )) if raw_selector.as_str() == bad_type,
        "get by ByName with unknown selector must return UnsupportedSelector"
    );

    // get_handle by ByName — same UnsupportedSelector behavior
    let gi_by_name = facade
        .get_handles(
            vec![ResourceRef {
                account: None,
                r#type: Some(bad_type.parse::<TypeName>().unwrap().into()),
                id: None,
                did: None,
                name: Some("unsupported-type-base".parse().unwrap()),
            }],
            ResourceVisibility::Active,
        )
        .await;
    assert_matches!(
        gi_by_name,
        Err(BatchResourceError::UnsupportedSelector(
            UnsupportedResourceSelectorError::NotFound { raw_selector }
        )) if raw_selector.as_str() == bad_type,
        "get_handle by ByName with unknown selector must return UnsupportedSelector"
    );

    // search — UnsupportedSelector (type validated before DB query)
    let list_result = facade
        .search(search_request(vec![ResourceSelector::of_type(
            bad_type.parse().unwrap(),
        )]))
        .await;
    assert_matches!(
        list_result,
        Err(ListResourcesError::UnsupportedSelector(
            UnsupportedResourceSelectorError::NotFound { raw_selector }
        )) if raw_selector.as_str() == bad_type,
        "search: unsupported selector must return UnsupportedSelector"
    );

    // search_handles — UnsupportedSelector
    let li_result = facade
        .search_handles(search_request(vec![ResourceSelector::of_type(
            bad_type.parse().unwrap(),
        )]))
        .await;
    assert_matches!(
        li_result,
        Err(ListResourcesError::UnsupportedSelector(
            UnsupportedResourceSelectorError::NotFound { raw_selector }
        )) if raw_selector.as_str() == bad_type,
        "search_handles: unsupported selector must return UnsupportedSelector"
    );

    // apply_manifest — UnsupportedDescriptor
    let bad_manifest =
        format!(r#"{{"$schema":"{bad_schema}","headers":{{"name":"x"}},"spec":{{}}}}"#);
    let apply_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: bad_manifest,
        })
        .await;
    assert_matches!(
        apply_result,
        Err(ApplyManifestError::UnsupportedDescriptor(_)),
        "apply_manifest: unsupported type must return UnsupportedDescriptor"
    );

    // delete by ById — UnsupportedSelector (type validated after UID is known)
    let delete_result = facade
        .delete(vec![ResourceRef {
            account: None,
            r#type: Some(bad_type.parse::<TypeName>().unwrap().into()),
            id: Some(id),
            did: None,
            name: None,
        }])
        .await;
    assert_matches!(
        delete_result,
        Err(BatchResourceError::UnsupportedSelector(
            UnsupportedResourceSelectorError::NotFound { raw_selector }
        )) if raw_selector.as_str() == bad_type,
        "delete by ById: unsupported selector must return UnsupportedSelector"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
