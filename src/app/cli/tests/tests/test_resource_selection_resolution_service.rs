// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::{Arc, LazyLock, Mutex};

use dill::CatalogBuilder;
use kamu_accounts::DEFAULT_ACCOUNT_HANDLE;
use kamu_cli::services::resources::{
    self,
    ExactResourceRef,
    ResourceSelectionItem,
    ResourceSelectionResolutionOptions,
    ResourceSelectionResolutionService,
    ResourceSelectionSyntax,
};
use kamu_resources::{ResourceHandle, ResourceID, ResourceTypeDescriptor, TypeUri};
use kamu_resources_facade::{
    MockResourceFacade,
    SearchResourceHandlesRequest,
    SearchResourceHandlesResponse,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn variableset_type_uri() -> &'static TypeUri {
    odf::metadata::config::VariableSet::schema()
}
const VARIABLESETS_NAME: &str = "variablesets";
const VARIABLESETS_SHORT_NAME: &str = "vs";

fn secretset_type_uri() -> &'static TypeUri {
    odf::metadata::config::SecretSet::schema()
}
const SECRETSETS_NAME: &str = "secretsets";
const SECRETSETS_SHORT_NAME: &str = "ss";

static STORAGE_TYPE_URI: LazyLock<TypeUri> = LazyLock::new(|| {
    TypeUri::new_unchecked("https://opendatafabric.org/schemas/config/v1alpha1/Storage")
});
const STORAGES_NAME: &str = "storages";
const STORAGES_SHORT_NAME: &str = "st";

const NAME_APP_PATTERN: &str = "app-%";
const NAME_MISSING_PATTERN: &str = "missing-%";
const RESOURCE_DB_CREDS: &str = "db-creds";

/// The single name pattern the selectors carry, for requests that apply one
/// uniformly.
fn selectors_name_pattern(selectors: &[kamu_resources::ResourceSelector]) -> &str {
    let mut patterns = selectors
        .iter()
        .map(|selector| selector.name.as_deref().expect("expected a name pattern"))
        .collect::<Vec<_>>();
    patterns.dedup();
    assert_eq!(patterns.len(), 1, "expected one uniform name pattern");
    patterns[0]
}

/// The name pattern each selector carries, in order.
fn selectors_name_pattern_each(selectors: &[kamu_resources::ResourceSelector]) -> Vec<&str> {
    selectors
        .iter()
        .map(|selector| selector.name.as_deref().expect("expected a name pattern"))
        .collect()
}

/// The type each selector names, in order. Panics on a type-less selector, so a
/// test asserting concrete types cannot silently pass on an any-type request.
fn selector_type_strings(selectors: &[kamu_resources::ResourceSelector]) -> Vec<&str> {
    selectors
        .iter()
        .map(|selector| {
            selector
                .r#type
                .as_ref()
                .expect("expected a concrete type, got a type-less selector")
                .as_ref()
        })
        .collect()
}

/// Asserts every selector spans all types.
fn assert_any_type(selectors: &[kamu_resources::ResourceSelector]) {
    assert!(
        selectors.iter().all(|selector| selector.r#type.is_none()),
        "expected type-less selectors, got {selectors:?}"
    );
}

/// The ids the selectors carry, in order. The wire is scalar, so a batch of ids
/// arrives as one selector each and the facade's coalescer folds them back into
/// a single row.
fn selector_ids(selectors: &[kamu_resources::ResourceSelector]) -> Vec<ResourceID> {
    selectors
        .iter()
        .map(|selector| selector.id.expect("expected an id"))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn resolves_exact_type_name_patterns_via_search() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_handles(
        1,
        vec![ResourceHandle {
            r#type: variableset_type_uri().clone(),
            did: None,
            id: ResourceID::new(uuid::Uuid::new_v4()),
            name: "app-alpha".parse().unwrap(),
            account: DEFAULT_ACCOUNT_HANDLE.clone(),
        }],
        Arc::clone(&search_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::NamePattern {
                    type_descriptor: harness.variableset_type_descriptor(),
                    selector_input: NAME_APP_PATTERN.to_string(),
                    name_pattern: NAME_APP_PATTERN.to_string(),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
                label_filter: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].selector_input, NAME_APP_PATTERN);

    let requests = search_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    // Selectors carry the resolved schema URI, not the CLI's short selector.
    assert_eq!(
        selector_type_strings(&requests[0].selectors),
        vec![variableset_type_uri().as_str()]
    );
    assert_eq!(
        selectors_name_pattern(&requests[0].selectors),
        NAME_APP_PATTERN
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn ignores_unmatched_name_patterns_when_requested() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_search_handles(1, Vec::new(), Arc::new(Mutex::new(Vec::new())));

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::NamePattern {
                    type_descriptor: harness.variableset_type_descriptor(),
                    selector_input: NAME_MISSING_PATTERN.to_string(),
                    name_pattern: NAME_MISSING_PATTERN.to_string(),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: true,
                max_expanded_results: Some(10),
                label_filter: None,
            },
        )
        .await
        .unwrap();

    assert!(result.targets.is_empty());
    assert_eq!(result.ignored_selectors.len(), 1);
    assert_eq!(
        result.ignored_selectors[0].selector_input,
        NAME_MISSING_PATTERN
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn errors_on_unmatched_name_patterns_by_default() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_search_handles(1, Vec::new(), Arc::new(Mutex::new(Vec::new())));

    let error = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::NamePattern {
                    type_descriptor: harness.variableset_type_descriptor(),
                    selector_input: NAME_MISSING_PATTERN.to_string(),
                    name_pattern: NAME_MISSING_PATTERN.to_string(),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
                label_filter: None,
            },
        )
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        format!("Pattern `{NAME_MISSING_PATTERN}` did not match any {VARIABLESETS_NAME}")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn exact_any_type_searches_across_every_supported_type() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let id = ResourceID::new(uuid::Uuid::new_v4());
    harness.expect_list_supported_resource_types(vec![
        harness.variableset_type_descriptor(),
        harness.secretset_type_descriptor(),
        harness.storage_type_descriptor(),
    ]);

    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_handles(
        1,
        vec![ResourceHandle {
            r#type: STORAGE_TYPE_URI.clone(),
            did: None,
            id,
            name: RESOURCE_DB_CREDS.parse().unwrap(),
            account: DEFAULT_ACCOUNT_HANDLE.clone(),
        }],
        Arc::clone(&search_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::ExactAnyType {
                    selector_input: id.to_string(),
                    resource_ref: ExactResourceRef::ById(id),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
                label_filter: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].canonical_selector.as_str(), STORAGES_NAME);
    assert_eq!(result.targets[0].id, id);

    let requests = search_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_any_type(&requests[0].selectors);
    assert_eq!(selector_ids(&requests[0].selectors), vec![id]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn exact_any_type_not_found_errors_by_default() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let id = ResourceID::new(uuid::Uuid::new_v4());
    harness.expect_list_supported_resource_types(vec![harness.variableset_type_descriptor()]);

    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_handles(1, Vec::new(), Arc::clone(&search_requests));

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::ExactAnyType {
                    selector_input: id.to_string(),
                    resource_ref: ExactResourceRef::ById(id),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
                label_filter: None,
            },
        )
        .await;

    assert!(
        result.is_err(),
        "expected a not-found error, got {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn resolves_any_type_exact_ref_across_every_supported_type() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_resource_types(vec![
        harness.secretset_type_descriptor(),
        harness.storage_type_descriptor(),
    ]);

    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_handles(
        1,
        vec![
            ResourceHandle {
                r#type: secretset_type_uri().clone(),
                did: None,
                id: ResourceID::new(uuid::Uuid::new_v4()),
                name: RESOURCE_DB_CREDS.parse().unwrap(),
                account: DEFAULT_ACCOUNT_HANDLE.clone(),
            },
            ResourceHandle {
                r#type: STORAGE_TYPE_URI.clone(),
                did: None,
                id: ResourceID::new(uuid::Uuid::new_v4()),
                name: RESOURCE_DB_CREDS.parse().unwrap(),
                account: DEFAULT_ACCOUNT_HANDLE.clone(),
            },
        ],
        Arc::clone(&search_requests),
    );

    let selector_input = format!("%/{RESOURCE_DB_CREDS}");
    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::AnyTypeExactRef {
                    selector_input: selector_input.clone(),
                    resource_ref: ExactResourceRef::ByName(RESOURCE_DB_CREDS.parse().unwrap()),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
                label_filter: None,
            },
        )
        .await
        .unwrap();

    // Each hit is labelled by its own schema, not by a single requested type.
    assert_eq!(result.targets.len(), 2);
    assert_eq!(
        result.targets[0].canonical_selector.as_str(),
        SECRETSETS_NAME
    );
    assert_eq!(result.targets[1].canonical_selector.as_str(), STORAGES_NAME);
    assert!(
        result
            .targets
            .iter()
            .all(|target| target.selector_input == selector_input)
    );

    let requests = search_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_any_type(&requests[0].selectors);
    // An exact any-type name travels as a wildcard-free `LIKE` pattern: the
    // selector's `name` is a pattern by ODF definition, so the literal is
    // escaped rather than widened.
    assert_eq!(
        selectors_name_pattern(&requests[0].selectors),
        RESOURCE_DB_CREDS
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn resolves_any_type_name_pattern_via_a_single_search() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_resource_types(vec![
        harness.variableset_type_descriptor(),
        harness.secretset_type_descriptor(),
    ]);

    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_handles(
        1,
        vec![
            ResourceHandle {
                r#type: variableset_type_uri().clone(),
                did: None,
                id: ResourceID::new(uuid::Uuid::new_v4()),
                name: "app-alpha".parse().unwrap(),
                account: DEFAULT_ACCOUNT_HANDLE.clone(),
            },
            ResourceHandle {
                r#type: secretset_type_uri().clone(),
                did: None,
                id: ResourceID::new(uuid::Uuid::new_v4()),
                name: "app-beta".parse().unwrap(),
                account: DEFAULT_ACCOUNT_HANDLE.clone(),
            },
        ],
        Arc::clone(&search_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::AnyTypeNamePattern {
                    selector_input: format!("%/{NAME_APP_PATTERN}"),
                    name_pattern: NAME_APP_PATTERN.to_string(),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
                label_filter: Some(environment_label_filter()),
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 2);
    assert_eq!(
        result.targets[0].canonical_selector.as_str(),
        VARIABLESETS_NAME
    );
    assert_eq!(
        result.targets[1].canonical_selector.as_str(),
        SECRETSETS_NAME
    );

    let requests = search_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_any_type(&requests[0].selectors);
    assert_eq!(
        selectors_name_pattern(&requests[0].selectors),
        NAME_APP_PATTERN
    );
    assert_eq!(
        requests[0].label_filter.as_ref(),
        Some(&environment_label_filter()),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn ignores_unmatched_any_type_selectors_when_requested() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_resource_types(vec![harness.variableset_type_descriptor()]);

    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_handles(2, Vec::new(), Arc::clone(&search_requests));

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![
                    ResourceSelectionItem::AnyTypeExactRef {
                        selector_input: format!("%/{RESOURCE_DB_CREDS}"),
                        resource_ref: ExactResourceRef::ByName(RESOURCE_DB_CREDS.parse().unwrap()),
                    },
                    ResourceSelectionItem::AnyTypeNamePattern {
                        selector_input: format!("%/{NAME_MISSING_PATTERN}"),
                        name_pattern: NAME_MISSING_PATTERN.to_string(),
                    },
                ],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: true,
                max_expanded_results: Some(10),
                label_filter: None,
            },
        )
        .await
        .unwrap();

    assert!(result.targets.is_empty());
    assert_eq!(result.ignored_selectors.len(), 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn errors_on_unmatched_any_type_selectors_by_default() {
    for item in [
        ResourceSelectionItem::AnyTypeExactRef {
            selector_input: format!("%/{RESOURCE_DB_CREDS}"),
            resource_ref: ExactResourceRef::ByName(RESOURCE_DB_CREDS.parse().unwrap()),
        },
        ResourceSelectionItem::AnyTypeNamePattern {
            selector_input: format!("%/{NAME_MISSING_PATTERN}"),
            name_pattern: NAME_MISSING_PATTERN.to_string(),
        },
    ] {
        let mut harness = ResourceSelectionResolutionHarness::new();
        harness.expect_list_supported_resource_types(vec![harness.variableset_type_descriptor()]);

        let search_requests = Arc::new(Mutex::new(Vec::new()));
        harness.expect_search_handles(1, Vec::new(), Arc::clone(&search_requests));

        let result = harness
            .service
            .resolve(
                ResourceSelectionSyntax {
                    items: vec![item.clone()],
                    shadowed_selectors: Vec::new(),
                },
                &harness.facade,
                &ResourceSelectionResolutionOptions {
                    ignore_not_found: false,
                    max_expanded_results: Some(10),
                    label_filter: None,
                },
            )
            .await;

        assert_matches!(
            result,
            Err(ref e) if e.to_string().contains("did not match any resource of any type"),
            "expected a not-found error for {item:?}",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn deduplicates_overlapping_name_patterns_before_counting_max_results() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let shared_id = ResourceID::new(uuid::Uuid::new_v4());
    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_handles(
        2,
        vec![ResourceHandle {
            r#type: variableset_type_uri().clone(),
            did: None,
            id: shared_id,
            name: "app-alpha".parse().unwrap(),
            account: DEFAULT_ACCOUNT_HANDLE.clone(),
        }],
        Arc::clone(&search_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![
                    ResourceSelectionItem::NamePattern {
                        type_descriptor: harness.variableset_type_descriptor(),
                        selector_input: NAME_APP_PATTERN.to_string(),
                        name_pattern: NAME_APP_PATTERN.to_string(),
                    },
                    ResourceSelectionItem::NamePattern {
                        type_descriptor: harness.variableset_type_descriptor(),
                        selector_input: "%alpha".to_string(),
                        name_pattern: "%alpha".to_string(),
                    },
                ],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(1),
                label_filter: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].id, shared_id);
    assert_eq!(result.targets[0].selector_input, NAME_APP_PATTERN);

    let requests = search_requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn errors_when_unique_targets_exceed_max_results_after_deduplication() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let shared_id = ResourceID::new(uuid::Uuid::new_v4());
    let second_id = ResourceID::new(uuid::Uuid::new_v4());
    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_handles(
        1,
        vec![
            ResourceHandle {
                r#type: variableset_type_uri().clone(),
                did: None,
                id: shared_id,
                name: "app-alpha".parse().unwrap(),
                account: DEFAULT_ACCOUNT_HANDLE.clone(),
            },
            ResourceHandle {
                r#type: variableset_type_uri().clone(),
                did: None,
                id: second_id,
                name: "app-beta".parse().unwrap(),
                account: DEFAULT_ACCOUNT_HANDLE.clone(),
            },
        ],
        Arc::clone(&search_requests),
    );

    let error = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![
                    ResourceSelectionItem::NamePattern {
                        type_descriptor: harness.variableset_type_descriptor(),
                        selector_input: NAME_APP_PATTERN.to_string(),
                        name_pattern: NAME_APP_PATTERN.to_string(),
                    },
                    ResourceSelectionItem::NamePattern {
                        type_descriptor: harness.variableset_type_descriptor(),
                        selector_input: "%beta".to_string(),
                        name_pattern: "%beta".to_string(),
                    },
                ],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(1),
                label_filter: None,
            },
        )
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Selection matched more than 1 resources; refine selectors, pass --max-results N, or pass \
         --unbounded"
    );

    let requests = search_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn passes_the_label_filter_to_pattern_expansion() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_handles(
        1,
        vec![ResourceHandle {
            r#type: variableset_type_uri().clone(),
            did: None,
            id: ResourceID::new(uuid::Uuid::new_v4()),
            name: "app-alpha".parse().unwrap(),
            account: DEFAULT_ACCOUNT_HANDLE.clone(),
        }],
        Arc::clone(&search_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::NamePattern {
                    type_descriptor: harness.variableset_type_descriptor(),
                    selector_input: NAME_APP_PATTERN.to_string(),
                    name_pattern: NAME_APP_PATTERN.to_string(),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
                label_filter: Some(environment_label_filter()),
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 1);

    let requests = search_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].label_filter.as_ref(),
        Some(&environment_label_filter())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn passes_the_label_filter_to_the_all_selector() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_resource_types(vec![harness.variableset_type_descriptor()]);

    let list_all_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_list_all_handles(
        vec![ResourceHandle {
            r#type: variableset_type_uri().clone(),
            did: None,
            id: ResourceID::new(uuid::Uuid::new_v4()),
            name: "app-alpha".parse().unwrap(),
            account: DEFAULT_ACCOUNT_HANDLE.clone(),
        }],
        Arc::clone(&list_all_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::All],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
                label_filter: Some(environment_label_filter()),
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 1);

    let requests = list_all_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].label_filter.as_ref(),
        Some(&environment_label_filter())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn narrows_exact_selectors_by_the_label_filter() {
    let matching_id = ResourceID::new(uuid::Uuid::new_v4());

    let matching = ResourceHandle {
        r#type: variableset_type_uri().clone(),
        did: None,
        id: matching_id,
        name: "vars-a".parse().unwrap(),
        account: DEFAULT_ACCOUNT_HANDLE.clone(),
    };

    let mut harness = ResourceSelectionResolutionHarness::new();
    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_handles(1, vec![matching], Arc::clone(&search_requests));

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![
                    harness.exact_selection_item("vars-a"),
                    harness.exact_selection_item("vars-b"),
                ],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: true,
                max_expanded_results: Some(10),
                label_filter: Some(environment_label_filter()),
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].id, matching_id);
    assert_eq!(result.ignored_selectors.len(), 1);

    // A label filter must reach the facade. The ref API carries none, so exact
    // names fall back to escaped, wildcard-free patterns here — otherwise the
    // filter would be silently dropped and non-matching resources returned.
    let requests = search_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].label_filter.as_ref(),
        Some(&environment_label_filter())
    );
    assert_eq!(
        selectors_name_pattern_each(&requests[0].selectors),
        vec!["vars-a", "vars-b"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn leaves_exact_selectors_untouched_without_a_label_filter() {
    let handle = ResourceHandle {
        r#type: variableset_type_uri().clone(),
        did: None,
        id: ResourceID::new(uuid::Uuid::new_v4()),
        name: "vars-a".parse().unwrap(),
        account: DEFAULT_ACCOUNT_HANDLE.clone(),
    };

    let mut harness = ResourceSelectionResolutionHarness::new();
    let get_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_get_handles(1, vec![handle], Arc::clone(&get_requests));

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![harness.exact_selection_item("vars-a")],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            &ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
                label_filter: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 1);

    // Without a label filter an exact name resolves through the ref API, which
    // keeps the whole batch in one request rather than one `ILIKE` row per
    // name.
    let requests = get_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]
            .iter()
            .map(|resource_ref| resource_ref.name.as_ref().unwrap().as_str())
            .collect::<Vec<_>>(),
        vec!["vars-a"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn environment_label_filter() -> kamu_resources::ResourceLabelFilterInput {
    kamu_resources::ResourceLabelFilterInput {
        entries: std::collections::BTreeMap::from([(
            "environment".to_string(),
            serde_json::Value::String("production".to_string()),
        )]),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ResourceSelectionResolutionHarness {
    service: Arc<dyn ResourceSelectionResolutionService>,
    facade: MockResourceFacade,
}

impl ResourceSelectionResolutionHarness {
    fn new() -> Self {
        Self {
            service: Self::build_service(),
            facade: MockResourceFacade::new(),
        }
    }

    fn build_service() -> Arc<dyn ResourceSelectionResolutionService> {
        let catalog = Self::build_catalog();

        catalog
            .get_one::<dyn ResourceSelectionResolutionService>()
            .unwrap()
    }

    fn build_catalog() -> dill::Catalog {
        let mut catalog_builder = CatalogBuilder::new();
        resources::register_dependencies(&mut catalog_builder);
        catalog_builder.build()
    }

    fn expect_list_supported_resource_types(
        &mut self,
        supported_resource_types: Vec<ResourceTypeDescriptor>,
    ) {
        self.facade
            .expect_list_supported_resource_types()
            .times(1)
            .returning(move || Ok(supported_resource_types.clone()));
    }

    fn exact_selection_item(&self, name: &str) -> ResourceSelectionItem {
        ResourceSelectionItem::Exact(resources::ResourceExactSelector {
            type_descriptor: self.variableset_type_descriptor(),
            selector_input: format!("{VARIABLESETS_SHORT_NAME}/{name}"),
            resource_ref: ExactResourceRef::ByName(name.parse().unwrap()),
        })
    }

    fn expect_list_all_handles(
        &mut self,
        handles: Vec<ResourceHandle>,
        requests: Arc<Mutex<Vec<kamu_resources_facade::ListAllResourceHandlesRequest>>>,
    ) {
        self.facade
            .expect_list_all_handles()
            .times(1)
            .returning(move |request| {
                requests.lock().unwrap().push(request);
                Ok(handles.clone())
            });
    }

    /// Exact names without a label filter resolve through the ref API, which
    /// keeps an N-name batch as a single request.
    fn expect_get_handles(
        &mut self,
        times: usize,
        handles: Vec<ResourceHandle>,
        requests: Arc<Mutex<Vec<Vec<kamu_resources::ResourceRef>>>>,
    ) {
        self.facade
            .expect_get_handles()
            .times(times)
            .returning(move |resource_refs| {
                requests.lock().unwrap().push(resource_refs);
                Ok(kamu_resources_facade::BatchResourceResponse {
                    successes: handles
                        .iter()
                        .enumerate()
                        .map(|(request_index, handle)| {
                            kamu_resources_facade::BatchResourceSuccess {
                                request_index,
                                item: handle.clone(),
                            }
                        })
                        .collect(),
                    problems: Vec::new(),
                })
            });
    }

    fn expect_search_handles(
        &mut self,
        times: usize,
        search_results: Vec<ResourceHandle>,
        search_requests: Arc<Mutex<Vec<SearchResourceHandlesRequest>>>,
    ) {
        self.facade
            .expect_search_handles()
            .times(times)
            .returning(move |request| {
                search_requests.lock().unwrap().push(request);
                Ok(SearchResourceHandlesResponse {
                    total_count: search_results.len(),
                    items: search_results.clone(),
                })
            });
    }

    fn variableset_type_descriptor(&self) -> ResourceTypeDescriptor {
        ResourceTypeDescriptor {
            canonical_selector: kamu_resources::ResourceSelectorName::new(VARIABLESETS_NAME)
                .unwrap(),
            selector_aliases: vec![
                kamu_resources::ResourceSelectorName::new(VARIABLESETS_SHORT_NAME).unwrap(),
            ],
            schema: variableset_type_uri().clone(),
            list_columns: Vec::new(),
        }
    }

    fn secretset_type_descriptor(&self) -> ResourceTypeDescriptor {
        ResourceTypeDescriptor {
            canonical_selector: kamu_resources::ResourceSelectorName::new(SECRETSETS_NAME).unwrap(),
            selector_aliases: vec![
                kamu_resources::ResourceSelectorName::new(SECRETSETS_SHORT_NAME).unwrap(),
            ],
            schema: secretset_type_uri().clone(),
            list_columns: Vec::new(),
        }
    }

    fn storage_type_descriptor(&self) -> ResourceTypeDescriptor {
        ResourceTypeDescriptor {
            canonical_selector: kamu_resources::ResourceSelectorName::new(STORAGES_NAME).unwrap(),
            selector_aliases: vec![
                kamu_resources::ResourceSelectorName::new(STORAGES_SHORT_NAME).unwrap(),
            ],
            schema: STORAGE_TYPE_URI.clone(),
            list_columns: Vec::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
