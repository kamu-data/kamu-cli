// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dill::CatalogBuilder;
use kamu_cli::services::resources::{
    self,
    ResourceSelectionItem,
    ResourceSelectionResolutionOptions,
    ResourceSelectionResolutionService,
    ResourceSelectionSyntax,
};
use kamu_resources::{
    ResourceID,
    ResourceIdentityView,
    ResourceKindDescriptor,
    ResourceNameNotFoundError,
};
use kamu_resources_facade::{
    MockResourceFacade,
    ResourceLookupProblem,
    ResourceRef,
    ResourceSelector,
    SearchResourceIdentitiesRequest,
    SearchResourceIdentitiesResponse,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const API_VERSION_V1: &str = "v1";
const VARIABLESET_KIND: &str = "kamu.dev/variableset";
const VARIABLESETS_NAME: &str = "variablesets";
const VARIABLESETS_SHORT_NAME: &str = "vs";
const SECRETSET_KIND: &str = "kamu.dev/secretset";
const SECRETSETS_NAME: &str = "secretsets";
const SECRETSETS_SHORT_NAME: &str = "ss";
const STORAGE_KIND: &str = "kamu.dev/storage";
const STORAGES_NAME: &str = "storages";
const STORAGES_SHORT_NAME: &str = "st";

const NAME_APP_PATTERN: &str = "app-%";
const NAME_MISSING_PATTERN: &str = "missing-%";
const KIND_PATTERN_S: &str = "S%";
const RESOURCE_DB_CREDS: &str = "db-creds";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn resolves_exact_kind_name_patterns_via_search() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_identities(
        1,
        vec![ResourceIdentityView {
            kind: VARIABLESET_KIND.to_string(),
            api_version: API_VERSION_V1.to_string(),
            canonical_kind_name: VARIABLESETS_NAME.to_string(),
            id: ResourceID::new(uuid::Uuid::new_v4()),
            name: "app-alpha".to_string(),
        }],
        Arc::clone(&search_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::NamePattern {
                    kind_descriptor: harness.variableset_kind_descriptor(),
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
    assert_eq!(requests[0].kinds, vec![VARIABLESET_KIND]);
    assert_eq!(requests[0].name_pattern.as_deref(), Some(NAME_APP_PATTERN));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn ignores_unmatched_name_patterns_when_requested() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_search_identities(1, Vec::new(), Arc::new(Mutex::new(Vec::new())));

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::NamePattern {
                    kind_descriptor: harness.variableset_kind_descriptor(),
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
    harness.expect_search_identities(1, Vec::new(), Arc::new(Mutex::new(Vec::new())));

    let error = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::NamePattern {
                    kind_descriptor: harness.variableset_kind_descriptor(),
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
async fn resolves_kind_patterns_with_exact_names_in_supported_kind_order() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_kinds(vec![
        harness.secretset_kind_descriptor(),
        harness.storage_kind_descriptor(),
    ]);

    let get_identity_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_get_identity(
        2,
        HashMap::from([
            (
                SECRETSET_KIND.to_string(),
                Some(ResourceIdentityView {
                    kind: SECRETSET_KIND.to_string(),
                    api_version: API_VERSION_V1.to_string(),
                    canonical_kind_name: SECRETSETS_NAME.to_string(),
                    id: ResourceID::new(uuid::Uuid::new_v4()),
                    name: RESOURCE_DB_CREDS.to_string(),
                }),
            ),
            (
                STORAGE_KIND.to_string(),
                Some(ResourceIdentityView {
                    kind: STORAGE_KIND.to_string(),
                    api_version: API_VERSION_V1.to_string(),
                    canonical_kind_name: STORAGES_NAME.to_string(),
                    id: ResourceID::new(uuid::Uuid::new_v4()),
                    name: RESOURCE_DB_CREDS.to_string(),
                }),
            ),
        ]),
        Arc::clone(&get_identity_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::KindPatternExactName {
                    kind_pattern: KIND_PATTERN_S.to_string(),
                    selector_input: format!("{KIND_PATTERN_S}/{RESOURCE_DB_CREDS}"),
                    resource_ref: kamu_resources_facade::ResourceRef::ByName(
                        RESOURCE_DB_CREDS.to_string(),
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
    assert_eq!(result.targets[0].canonical_kind_name, SECRETSETS_NAME);
    assert_eq!(result.targets[1].canonical_kind_name, STORAGES_NAME);
    assert!(
        result
            .targets
            .iter()
            .all(|target| target.selector_input == format!("{KIND_PATTERN_S}/{RESOURCE_DB_CREDS}"))
    );

    let requests = get_identity_requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].kind, SECRETSET_KIND);
    assert_eq!(requests[1].kind, STORAGE_KIND);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn resolves_kind_pattern_all_via_search_across_matched_kinds() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_kinds(vec![
        harness.secretset_kind_descriptor(),
        harness.storage_kind_descriptor(),
    ]);

    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_identities(
        1,
        vec![
            ResourceIdentityView {
                kind: SECRETSET_KIND.to_string(),
                api_version: API_VERSION_V1.to_string(),
                canonical_kind_name: SECRETSETS_NAME.to_string(),
                id: ResourceID::new(uuid::Uuid::new_v4()),
                name: "db-creds".to_string(),
            },
            ResourceIdentityView {
                kind: STORAGE_KIND.to_string(),
                api_version: API_VERSION_V1.to_string(),
                canonical_kind_name: STORAGES_NAME.to_string(),
                id: ResourceID::new(uuid::Uuid::new_v4()),
                name: "warehouse".to_string(),
            },
        ],
        Arc::clone(&search_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::KindPatternAll {
                    kind_pattern: KIND_PATTERN_S.to_string(),
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
    assert_eq!(result.targets[0].canonical_kind_name, SECRETSETS_NAME);
    assert_eq!(result.targets[1].canonical_kind_name, STORAGES_NAME);

    let requests = search_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].kinds,
        vec![SECRETSET_KIND.to_string(), STORAGE_KIND.to_string()]
    );
    assert_eq!(requests[0].exact_names, None);
    assert_eq!(requests[0].name_pattern, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn resolves_kind_pattern_name_patterns_via_single_search_across_matched_kinds() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_kinds(vec![
        harness.secretset_kind_descriptor(),
        harness.storage_kind_descriptor(),
    ]);

    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_identities(
        1,
        vec![
            ResourceIdentityView {
                kind: SECRETSET_KIND.to_string(),
                api_version: API_VERSION_V1.to_string(),
                canonical_kind_name: SECRETSETS_NAME.to_string(),
                id: ResourceID::new(uuid::Uuid::new_v4()),
                name: "db-creds".to_string(),
            },
            ResourceIdentityView {
                kind: STORAGE_KIND.to_string(),
                api_version: API_VERSION_V1.to_string(),
                canonical_kind_name: STORAGES_NAME.to_string(),
                id: ResourceID::new(uuid::Uuid::new_v4()),
                name: "db-warehouse".to_string(),
            },
        ],
        Arc::clone(&search_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::KindPatternNamePattern {
                    kind_pattern: KIND_PATTERN_S.to_string(),
                    selector_input: format!("{KIND_PATTERN_S}/db-%"),
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
    assert_eq!(result.targets[0].canonical_kind_name, SECRETSETS_NAME);
    assert_eq!(result.targets[1].canonical_kind_name, STORAGES_NAME);
    assert!(
        result
            .targets
            .iter()
            .all(|target| target.selector_input == format!("{KIND_PATTERN_S}/db-%"))
    );

    let requests = search_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].kinds,
        vec![SECRETSET_KIND.to_string(), STORAGE_KIND.to_string()]
    );
    assert_eq!(requests[0].exact_names, None);
    assert_eq!(requests[0].name_pattern.as_deref(), Some("db-%"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn kind_pattern_exact_id_tries_every_matched_kind() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let id = ResourceID::new(uuid::Uuid::new_v4());
    harness.expect_list_supported_kinds(vec![
        harness.secretset_kind_descriptor(),
        harness.storage_kind_descriptor(),
    ]);

    let get_identity_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_get_identity(
        2,
        HashMap::from([(
            STORAGE_KIND.to_string(),
            Some(ResourceIdentityView {
                kind: STORAGE_KIND.to_string(),
                api_version: API_VERSION_V1.to_string(),
                canonical_kind_name: STORAGES_NAME.to_string(),
                id,
                name: RESOURCE_DB_CREDS.to_string(),
            }),
        )]),
        Arc::clone(&get_identity_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::KindPatternExactName {
                    kind_pattern: KIND_PATTERN_S.to_string(),
                    selector_input: format!("{KIND_PATTERN_S}/{id}"),
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
    assert_eq!(result.targets[0].canonical_kind_name, STORAGES_NAME);

    let requests = get_identity_requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].kind, SECRETSET_KIND);
    assert_eq!(requests[1].kind, STORAGE_KIND);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn ignores_unmatched_kind_pattern_exact_selectors_when_requested() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_kinds(vec![harness.secretset_kind_descriptor()]);
    harness.expect_get_identity(1, HashMap::new(), Arc::new(Mutex::new(Vec::new())));

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::KindPatternExactName {
                    kind_pattern: KIND_PATTERN_S.to_string(),
                    selector_input: format!("{KIND_PATTERN_S}/missing"),
                    resource_ref: kamu_resources_facade::ResourceRef::ByName("missing".to_string()),
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
        format!("{KIND_PATTERN_S}/missing")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn errors_on_unmatched_kind_pattern_exact_selectors_by_default() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_kinds(vec![harness.secretset_kind_descriptor()]);
    harness.expect_get_identity(1, HashMap::new(), Arc::new(Mutex::new(Vec::new())));

    let error = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::KindPatternExactName {
                    kind_pattern: KIND_PATTERN_S.to_string(),
                    selector_input: format!("{KIND_PATTERN_S}/missing"),
                    resource_ref: kamu_resources_facade::ResourceRef::ByName("missing".to_string()),
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
        format!("Selector `missing` did not match any resource kind matched by `{KIND_PATTERN_S}`")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn ignores_unmatched_kind_pattern_name_patterns_when_requested() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_kinds(vec![harness.secretset_kind_descriptor()]);
    harness.expect_search_identities(1, Vec::new(), Arc::new(Mutex::new(Vec::new())));

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::KindPatternNamePattern {
                    kind_pattern: KIND_PATTERN_S.to_string(),
                    selector_input: format!("{KIND_PATTERN_S}/{NAME_MISSING_PATTERN}"),
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
        format!("{KIND_PATTERN_S}/{NAME_MISSING_PATTERN}")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn errors_on_unmatched_kind_pattern_name_patterns_by_default() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    harness.expect_list_supported_kinds(vec![harness.secretset_kind_descriptor()]);
    harness.expect_search_identities(1, Vec::new(), Arc::new(Mutex::new(Vec::new())));

    let error = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![ResourceSelectionItem::KindPatternNamePattern {
                    kind_pattern: KIND_PATTERN_S.to_string(),
                    selector_input: format!("{KIND_PATTERN_S}/{NAME_MISSING_PATTERN}"),
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
            "Pattern `{NAME_MISSING_PATTERN}` did not match any resource kind matched by \
             `{KIND_PATTERN_S}`"
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn deduplicates_overlapping_name_patterns_before_counting_max_results() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let shared_id = ResourceID::new(uuid::Uuid::new_v4());
    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_identities(
        2,
        vec![ResourceIdentityView {
            kind: VARIABLESET_KIND.to_string(),
            api_version: API_VERSION_V1.to_string(),
            canonical_kind_name: VARIABLESETS_NAME.to_string(),
            id: shared_id,
            name: "app-alpha".to_string(),
        }],
        Arc::clone(&search_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![
                    ResourceSelectionItem::NamePattern {
                        kind_descriptor: harness.variableset_kind_descriptor(),
                        selector_input: NAME_APP_PATTERN.to_string(),
                        name_pattern: NAME_APP_PATTERN.to_string(),
                    },
                    ResourceSelectionItem::NamePattern {
                        kind_descriptor: harness.variableset_kind_descriptor(),
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
async fn deduplicates_repeated_kind_pattern_exact_name_matches() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let shared_id = ResourceID::new(uuid::Uuid::new_v4());
    harness.expect_list_supported_kinds(vec![harness.secretset_kind_descriptor()]);

    let get_identity_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_get_identity(
        2,
        HashMap::from([(
            SECRETSET_KIND.to_string(),
            Some(ResourceIdentityView {
                kind: SECRETSET_KIND.to_string(),
                api_version: API_VERSION_V1.to_string(),
                canonical_kind_name: SECRETSETS_NAME.to_string(),
                id: shared_id,
                name: RESOURCE_DB_CREDS.to_string(),
            }),
        )]),
        Arc::clone(&get_identity_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![
                    ResourceSelectionItem::KindPatternExactName {
                        kind_pattern: KIND_PATTERN_S.to_string(),
                        selector_input: format!("{KIND_PATTERN_S}/{RESOURCE_DB_CREDS}"),
                        resource_ref: kamu_resources_facade::ResourceRef::ByName(
                            RESOURCE_DB_CREDS.to_string(),
                        ),
                    },
                    ResourceSelectionItem::KindPatternExactName {
                        kind_pattern: KIND_PATTERN_S.to_string(),
                        selector_input: format!("{KIND_PATTERN_S}/{RESOURCE_DB_CREDS}"),
                        resource_ref: kamu_resources_facade::ResourceRef::ByName(
                            RESOURCE_DB_CREDS.to_string(),
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

    let requests = get_identity_requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn deduplicates_kind_pattern_all_before_counting_max_results() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let shared_id = ResourceID::new(uuid::Uuid::new_v4());
    harness.expect_list_supported_kinds(vec![harness.secretset_kind_descriptor()]);

    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_identities(
        1,
        vec![ResourceIdentityView {
            kind: SECRETSET_KIND.to_string(),
            api_version: API_VERSION_V1.to_string(),
            canonical_kind_name: SECRETSETS_NAME.to_string(),
            id: shared_id,
            name: RESOURCE_DB_CREDS.to_string(),
        }],
        Arc::clone(&search_requests),
    );

    let get_identity_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_get_identity(
        1,
        HashMap::from([(
            SECRETSET_KIND.to_string(),
            Some(ResourceIdentityView {
                kind: SECRETSET_KIND.to_string(),
                api_version: API_VERSION_V1.to_string(),
                canonical_kind_name: SECRETSETS_NAME.to_string(),
                id: shared_id,
                name: RESOURCE_DB_CREDS.to_string(),
            }),
        )]),
        Arc::clone(&get_identity_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![
                    ResourceSelectionItem::KindPatternAll {
                        kind_pattern: KIND_PATTERN_S.to_string(),
                        selector_input: "%".to_string(),
                    },
                    ResourceSelectionItem::KindPatternExactName {
                        kind_pattern: KIND_PATTERN_S.to_string(),
                        selector_input: format!("{KIND_PATTERN_S}/{RESOURCE_DB_CREDS}"),
                        resource_ref: kamu_resources_facade::ResourceRef::ByName(
                            RESOURCE_DB_CREDS.to_string(),
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

    let get_identity_requests = get_identity_requests.lock().unwrap();
    assert_eq!(get_identity_requests.len(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn deduplicates_kind_pattern_name_patterns_before_counting_max_results() {
    let mut harness = ResourceSelectionResolutionHarness::new();
    let shared_id = ResourceID::new(uuid::Uuid::new_v4());
    harness.expect_list_supported_kinds(vec![harness.secretset_kind_descriptor()]);

    let search_requests = Arc::new(Mutex::new(Vec::new()));
    harness.expect_search_identities(
        2,
        vec![ResourceIdentityView {
            kind: SECRETSET_KIND.to_string(),
            api_version: API_VERSION_V1.to_string(),
            canonical_kind_name: SECRETSETS_NAME.to_string(),
            id: shared_id,
            name: RESOURCE_DB_CREDS.to_string(),
        }],
        Arc::clone(&search_requests),
    );

    let result = harness
        .service
        .resolve(
            ResourceSelectionSyntax {
                items: vec![
                    ResourceSelectionItem::KindPatternNamePattern {
                        kind_pattern: KIND_PATTERN_S.to_string(),
                        selector_input: format!("{KIND_PATTERN_S}/db-%"),
                        name_pattern: "db-%".to_string(),
                    },
                    ResourceSelectionItem::KindPatternNamePattern {
                        kind_pattern: KIND_PATTERN_S.to_string(),
                        selector_input: format!("{KIND_PATTERN_S}/%creds"),
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
        format!("{KIND_PATTERN_S}/db-%")
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
    harness.expect_search_identities(
        1,
        vec![
            ResourceIdentityView {
                kind: VARIABLESET_KIND.to_string(),
                api_version: API_VERSION_V1.to_string(),
                canonical_kind_name: VARIABLESETS_NAME.to_string(),
                id: shared_id,
                name: "app-alpha".to_string(),
            },
            ResourceIdentityView {
                kind: VARIABLESET_KIND.to_string(),
                api_version: API_VERSION_V1.to_string(),
                canonical_kind_name: VARIABLESETS_NAME.to_string(),
                id: second_id,
                name: "app-beta".to_string(),
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
                        kind_descriptor: harness.variableset_kind_descriptor(),
                        selector_input: NAME_APP_PATTERN.to_string(),
                        name_pattern: NAME_APP_PATTERN.to_string(),
                    },
                    ResourceSelectionItem::NamePattern {
                        kind_descriptor: harness.variableset_kind_descriptor(),
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

    fn expect_list_supported_kinds(&mut self, supported_kinds: Vec<ResourceKindDescriptor>) {
        self.facade
            .expect_list_supported_kinds()
            .times(1)
            .returning(move || Ok(supported_kinds.clone()));
    }

    fn expect_get_identity(
        &mut self,
        times: usize,
        get_identity_results: HashMap<String, Option<ResourceIdentityView>>,
        get_identity_requests: Arc<Mutex<Vec<ResourceSelector>>>,
    ) {
        self.facade
            .expect_get_identity()
            .times(times)
            .returning(move |selector| {
                get_identity_requests.lock().unwrap().push(selector.clone());

                if let Some(identity) = get_identity_results.get(&selector.kind).cloned().flatten()
                {
                    return Ok(identity);
                }

                match selector.resource_ref {
                    ResourceRef::ById(id) => Err(ResourceLookupProblem::IDNotFound(
                        kamu_resources::ResourceIDNotFoundError(id),
                    )
                    .into()),
                    ResourceRef::ByName(name) => Err(ResourceLookupProblem::NameNotFound(
                        ResourceNameNotFoundError {
                            kind: selector.kind,
                            name,
                        },
                    )
                    .into()),
                }
            });
    }

    fn expect_search_identities(
        &mut self,
        times: usize,
        search_results: Vec<ResourceIdentityView>,
        search_requests: Arc<Mutex<Vec<SearchResourceIdentitiesRequest>>>,
    ) {
        self.facade
            .expect_search_identities()
            .times(times)
            .returning(move |request| {
                search_requests.lock().unwrap().push(request);
                Ok(SearchResourceIdentitiesResponse {
                    total_count: search_results.len(),
                    items: search_results.clone(),
                })
            });
    }

    fn variableset_kind_descriptor(&self) -> ResourceKindDescriptor {
        ResourceKindDescriptor {
            name: VARIABLESETS_NAME.to_string(),
            short_names: vec![VARIABLESETS_SHORT_NAME.to_string()],
            kind: VARIABLESET_KIND.to_string(),
            api_version: API_VERSION_V1.to_string(),
            list_columns: Vec::new(),
        }
    }

    fn secretset_kind_descriptor(&self) -> ResourceKindDescriptor {
        ResourceKindDescriptor {
            name: SECRETSETS_NAME.to_string(),
            short_names: vec![SECRETSETS_SHORT_NAME.to_string()],
            kind: SECRETSET_KIND.to_string(),
            api_version: API_VERSION_V1.to_string(),
            list_columns: Vec::new(),
        }
    }

    fn storage_kind_descriptor(&self) -> ResourceKindDescriptor {
        ResourceKindDescriptor {
            name: STORAGES_NAME.to_string(),
            short_names: vec![STORAGES_SHORT_NAME.to_string()],
            kind: STORAGE_KIND.to_string(),
            api_version: API_VERSION_V1.to_string(),
            list_columns: Vec::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
