// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use dill::CatalogBuilder;
use kamu_accounts::DEFAULT_ACCOUNT_HANDLE;
use kamu_cli::services::resources::{
    self,
    ResourceSelectionItem,
    ResourceSelectionResolutionOptions,
    ResourceSelectionResolutionService,
    ResourceSelectionSyntax,
};
use kamu_resources::{
    ResourceHandle,
    ResourceID,
    ResourceNameNotFoundError,
    ResourceTypeDescriptor,
    TypeUri,
};
use kamu_resources_facade::{
    MockResourceFacade,
    ResourceLookupProblem,
    ResourceRef,
    ResourceSelector,
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
const TYPE_PATTERN_S: &str = "S%";
const RESOURCE_DB_CREDS: &str = "db-creds";

fn raw_selector_strings(selectors: &[kamu_resources::ResourceTypeSelectorRaw]) -> Vec<&str> {
    selectors
        .iter()
        .map(kamu_resources::ResourceTypeSelectorRaw::as_str)
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
            ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].selector_input, NAME_APP_PATTERN);

    let requests = search_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        raw_selector_strings(&requests[0].raw_type_selectors),
        vec![VARIABLESETS_NAME]
    );
    assert_eq!(requests[0].name_pattern.as_deref(), Some(NAME_APP_PATTERN));
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
            ResourceSelectionResolutionOptions {
                ignore_not_found: true,
                max_expanded_results: Some(10),
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
            ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
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
async fn resolves_type_patterns_with_exact_names_in_supported_type_order() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_resource_types(vec![
        harness.secretset_type_descriptor(),
        harness.storage_type_descriptor(),
    ]);

    let get_handle_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_get_handle(
        2,
        HashMap::from([
            (
                SECRETSETS_NAME.to_string(),
                Some(ResourceHandle {
                    r#type: secretset_type_uri().clone(),
                    did: None,
                    id: ResourceID::new(uuid::Uuid::new_v4()),
                    name: RESOURCE_DB_CREDS.parse().unwrap(),
                    account: DEFAULT_ACCOUNT_HANDLE.clone(),
                }),
            ),
            (
                STORAGES_NAME.to_string(),
                Some(ResourceHandle {
                    r#type: STORAGE_TYPE_URI.clone(),
                    did: None,
                    id: ResourceID::new(uuid::Uuid::new_v4()),
                    name: RESOURCE_DB_CREDS.parse().unwrap(),
                    account: DEFAULT_ACCOUNT_HANDLE.clone(),
                }),
            ),
        ]),
        Arc::clone(&get_handle_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::TypePatternExactName {
                    type_pattern: TYPE_PATTERN_S.to_string(),
                    selector_input: format!("{TYPE_PATTERN_S}/{RESOURCE_DB_CREDS}"),
                    resource_ref: kamu_resources_facade::ResourceRef::ByName(
                        RESOURCE_DB_CREDS.parse().unwrap(),
                    ),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
            },
        )
        .await
        .unwrap();

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
            .all(|target| target.selector_input == format!("{TYPE_PATTERN_S}/{RESOURCE_DB_CREDS}"))
    );

    let requests = get_handle_requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].resource_type.as_str(), SECRETSETS_NAME);
    assert_eq!(requests[1].resource_type.as_str(), STORAGES_NAME);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn resolves_type_pattern_all_via_search_across_matched_types() {
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
                name: "db-creds".parse().unwrap(),
                account: DEFAULT_ACCOUNT_HANDLE.clone(),
            },
            ResourceHandle {
                r#type: STORAGE_TYPE_URI.clone(),
                did: None,
                id: ResourceID::new(uuid::Uuid::new_v4()),
                name: "warehouse".parse().unwrap(),
                account: DEFAULT_ACCOUNT_HANDLE.clone(),
            },
        ],
        Arc::clone(&search_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::TypePatternAll {
                    type_pattern: TYPE_PATTERN_S.to_string(),
                    selector_input: "%".to_string(),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 2);
    assert_eq!(
        result.targets[0].canonical_selector.as_str(),
        SECRETSETS_NAME
    );
    assert_eq!(result.targets[1].canonical_selector.as_str(), STORAGES_NAME);

    let requests = search_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        raw_selector_strings(&requests[0].raw_type_selectors),
        vec![SECRETSETS_NAME, STORAGES_NAME]
    );
    assert_eq!(requests[0].exact_names, None);
    assert_eq!(requests[0].name_pattern, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn resolves_type_pattern_name_patterns_via_single_search_across_matched_types() {
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
                name: "db-creds".parse().unwrap(),
                account: DEFAULT_ACCOUNT_HANDLE.clone(),
            },
            ResourceHandle {
                r#type: STORAGE_TYPE_URI.clone(),
                did: None,
                id: ResourceID::new(uuid::Uuid::new_v4()),
                name: "db-warehouse".parse().unwrap(),
                account: DEFAULT_ACCOUNT_HANDLE.clone(),
            },
        ],
        Arc::clone(&search_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::TypePatternNamePattern {
                    type_pattern: TYPE_PATTERN_S.to_string(),
                    selector_input: format!("{TYPE_PATTERN_S}/db-%"),
                    name_pattern: "db-%".to_string(),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
            },
        )
        .await
        .unwrap();

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
            .all(|target| target.selector_input == format!("{TYPE_PATTERN_S}/db-%"))
    );

    let requests = search_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        raw_selector_strings(&requests[0].raw_type_selectors),
        vec![SECRETSETS_NAME, STORAGES_NAME]
    );
    assert_eq!(requests[0].exact_names, None);
    assert_eq!(requests[0].name_pattern.as_deref(), Some("db-%"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn type_pattern_exact_id_tries_every_matched_type() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let id = ResourceID::new(uuid::Uuid::new_v4());
    harness.expect_list_supported_resource_types(vec![
        harness.secretset_type_descriptor(),
        harness.storage_type_descriptor(),
    ]);

    let get_handle_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_get_handle(
        2,
        HashMap::from([(
            STORAGES_NAME.to_string(),
            Some(ResourceHandle {
                r#type: STORAGE_TYPE_URI.clone(),
                did: None,
                id,
                name: RESOURCE_DB_CREDS.parse().unwrap(),
                account: DEFAULT_ACCOUNT_HANDLE.clone(),
            }),
        )]),
        Arc::clone(&get_handle_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::TypePatternExactName {
                    type_pattern: TYPE_PATTERN_S.to_string(),
                    selector_input: format!("{TYPE_PATTERN_S}/{id}"),
                    resource_ref: kamu_resources_facade::ResourceRef::ById(id),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].canonical_selector.as_str(), STORAGES_NAME);

    let requests = get_handle_requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].resource_type.as_str(), SECRETSETS_NAME);
    assert_eq!(requests[1].resource_type.as_str(), STORAGES_NAME);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn ignores_unmatched_type_pattern_exact_selectors_when_requested() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_resource_types(vec![harness.secretset_type_descriptor()]);
    harness.expect_get_handle(1, HashMap::new(), Arc::new(Mutex::new(Vec::new())));

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::TypePatternExactName {
                    type_pattern: TYPE_PATTERN_S.to_string(),
                    selector_input: format!("{TYPE_PATTERN_S}/missing"),
                    resource_ref: kamu_resources_facade::ResourceRef::ByName(
                        "missing".parse().unwrap(),
                    ),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            ResourceSelectionResolutionOptions {
                ignore_not_found: true,
                max_expanded_results: Some(10),
            },
        )
        .await
        .unwrap();

    assert!(result.targets.is_empty());
    assert_eq!(result.ignored_selectors.len(), 1);
    assert_eq!(
        result.ignored_selectors[0].selector_input,
        format!("{TYPE_PATTERN_S}/missing")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn errors_on_unmatched_type_pattern_exact_selectors_by_default() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_resource_types(vec![harness.secretset_type_descriptor()]);
    harness.expect_get_handle(1, HashMap::new(), Arc::new(Mutex::new(Vec::new())));

    let error = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::TypePatternExactName {
                    type_pattern: TYPE_PATTERN_S.to_string(),
                    selector_input: format!("{TYPE_PATTERN_S}/missing"),
                    resource_ref: kamu_resources_facade::ResourceRef::ByName(
                        "missing".parse().unwrap(),
                    ),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
            },
        )
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        format!("Selector `missing` did not match any resource type matched by `{TYPE_PATTERN_S}`")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn ignores_unmatched_type_pattern_name_patterns_when_requested() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_resource_types(vec![harness.secretset_type_descriptor()]);
    harness.expect_search_handles(1, Vec::new(), Arc::new(Mutex::new(Vec::new())));

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::TypePatternNamePattern {
                    type_pattern: TYPE_PATTERN_S.to_string(),
                    selector_input: format!("{TYPE_PATTERN_S}/{NAME_MISSING_PATTERN}"),
                    name_pattern: NAME_MISSING_PATTERN.to_string(),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            ResourceSelectionResolutionOptions {
                ignore_not_found: true,
                max_expanded_results: Some(10),
            },
        )
        .await
        .unwrap();

    assert!(result.targets.is_empty());
    assert_eq!(result.ignored_selectors.len(), 1);
    assert_eq!(
        result.ignored_selectors[0].selector_input,
        format!("{TYPE_PATTERN_S}/{NAME_MISSING_PATTERN}")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn errors_on_unmatched_type_pattern_name_patterns_by_default() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_resource_types(vec![harness.secretset_type_descriptor()]);
    harness.expect_search_handles(1, Vec::new(), Arc::new(Mutex::new(Vec::new())));

    let error = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::TypePatternNamePattern {
                    type_pattern: TYPE_PATTERN_S.to_string(),
                    selector_input: format!("{TYPE_PATTERN_S}/{NAME_MISSING_PATTERN}"),
                    name_pattern: NAME_MISSING_PATTERN.to_string(),
                }],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(10),
            },
        )
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        format!(
            "Pattern `{NAME_MISSING_PATTERN}` did not match any resource type matched by \
             `{TYPE_PATTERN_S}`"
        )
    );
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
            ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(1),
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
async fn deduplicates_repeated_type_pattern_exact_name_matches() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let shared_id = ResourceID::new(uuid::Uuid::new_v4());
    harness.expect_list_supported_resource_types(vec![harness.secretset_type_descriptor()]);

    let get_handle_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_get_handle(
        2,
        HashMap::from([(
            SECRETSETS_NAME.to_string(),
            Some(ResourceHandle {
                r#type: secretset_type_uri().clone(),
                did: None,
                id: shared_id,
                name: RESOURCE_DB_CREDS.parse().unwrap(),
                account: DEFAULT_ACCOUNT_HANDLE.clone(),
            }),
        )]),
        Arc::clone(&get_handle_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![
                    ResourceSelectionItem::TypePatternExactName {
                        type_pattern: TYPE_PATTERN_S.to_string(),
                        selector_input: format!("{TYPE_PATTERN_S}/{RESOURCE_DB_CREDS}"),
                        resource_ref: kamu_resources_facade::ResourceRef::ByName(
                            RESOURCE_DB_CREDS.parse().unwrap(),
                        ),
                    },
                    ResourceSelectionItem::TypePatternExactName {
                        type_pattern: TYPE_PATTERN_S.to_string(),
                        selector_input: format!("{TYPE_PATTERN_S}/{RESOURCE_DB_CREDS}"),
                        resource_ref: kamu_resources_facade::ResourceRef::ByName(
                            RESOURCE_DB_CREDS.parse().unwrap(),
                        ),
                    },
                ],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(1),
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].id, shared_id);

    let requests = get_handle_requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn deduplicates_type_pattern_all_before_counting_max_results() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let shared_id = ResourceID::new(uuid::Uuid::new_v4());
    harness.expect_list_supported_resource_types(vec![harness.secretset_type_descriptor()]);

    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_handles(
        1,
        vec![ResourceHandle {
            r#type: secretset_type_uri().clone(),
            did: None,
            id: shared_id,
            name: RESOURCE_DB_CREDS.parse().unwrap(),
            account: DEFAULT_ACCOUNT_HANDLE.clone(),
        }],
        Arc::clone(&search_requests),
    );

    let get_handle_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_get_handle(
        1,
        HashMap::from([(
            SECRETSETS_NAME.to_string(),
            Some(ResourceHandle {
                r#type: secretset_type_uri().clone(),
                did: None,
                id: shared_id,
                name: RESOURCE_DB_CREDS.parse().unwrap(),
                account: DEFAULT_ACCOUNT_HANDLE.clone(),
            }),
        )]),
        Arc::clone(&get_handle_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![
                    ResourceSelectionItem::TypePatternAll {
                        type_pattern: TYPE_PATTERN_S.to_string(),
                        selector_input: "%".to_string(),
                    },
                    ResourceSelectionItem::TypePatternExactName {
                        type_pattern: TYPE_PATTERN_S.to_string(),
                        selector_input: format!("{TYPE_PATTERN_S}/{RESOURCE_DB_CREDS}"),
                        resource_ref: kamu_resources_facade::ResourceRef::ByName(
                            RESOURCE_DB_CREDS.parse().unwrap(),
                        ),
                    },
                ],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(1),
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].id, shared_id);

    let search_requests = search_requests.lock().unwrap();
    assert_eq!(search_requests.len(), 1);

    let get_handle_requests = get_handle_requests.lock().unwrap();
    assert_eq!(get_handle_requests.len(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn deduplicates_type_pattern_name_patterns_before_counting_max_results() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let shared_id = ResourceID::new(uuid::Uuid::new_v4());
    harness.expect_list_supported_resource_types(vec![harness.secretset_type_descriptor()]);

    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_handles(
        2,
        vec![ResourceHandle {
            r#type: secretset_type_uri().clone(),
            did: None,
            id: shared_id,
            name: RESOURCE_DB_CREDS.parse().unwrap(),
            account: DEFAULT_ACCOUNT_HANDLE.clone(),
        }],
        Arc::clone(&search_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![
                    ResourceSelectionItem::TypePatternNamePattern {
                        type_pattern: TYPE_PATTERN_S.to_string(),
                        selector_input: format!("{TYPE_PATTERN_S}/db-%"),
                        name_pattern: "db-%".to_string(),
                    },
                    ResourceSelectionItem::TypePatternNamePattern {
                        type_pattern: TYPE_PATTERN_S.to_string(),
                        selector_input: format!("{TYPE_PATTERN_S}/%creds"),
                        name_pattern: "%creds".to_string(),
                    },
                ],
                shadowed_selectors: Vec::new(),
            },
            &harness.facade,
            ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(1),
            },
        )
        .await
        .unwrap();

    assert_eq!(result.targets.len(), 1);
    assert_eq!(result.targets[0].id, shared_id);
    assert_eq!(
        result.targets[0].selector_input,
        format!("{TYPE_PATTERN_S}/db-%")
    );

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
            ResourceSelectionResolutionOptions {
                ignore_not_found: false,
                max_expanded_results: Some(1),
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

    fn expect_get_handle(
        &mut self,
        times: usize,
        get_handle_results: HashMap<String, Option<ResourceHandle>>,
        get_handle_requests: Arc<Mutex<Vec<ResourceSelector>>>,
    ) {
        self.facade
            .expect_get_handle()
            .times(times)
            .returning(move |selector| {
                get_handle_requests.lock().unwrap().push(selector.clone());

                if let Some(handle) = get_handle_results
                    .get(selector.resource_type.as_str())
                    .cloned()
                    .flatten()
                {
                    return Ok(handle);
                }

                // The resolution service always calls `get_handle` with the
                // already-resolved canonical selector name, never a raw alias.
                let type_name =
                    kamu_resources::TypeName::new_unchecked(selector.resource_type.as_str());

                match selector.resource_ref {
                    ResourceRef::ById(id) => Err(ResourceLookupProblem::IDNotFound(
                        kamu_resources::ResourceIDNotFoundError(id),
                    )
                    .into()),
                    ResourceRef::ByName(name) => Err(ResourceLookupProblem::NameNotFound(
                        ResourceNameNotFoundError { type_name, name },
                    )
                    .into()),
                }
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
