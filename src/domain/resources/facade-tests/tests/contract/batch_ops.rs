// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::VariableSetResource;
use kamu_resources::{ResourceRef, ResourceSchemaProvider, TypeName};
use kamu_resources_facade::{
    BatchResourceError,
    ResourceLookupProblem,
    ResourceManifestFormat,
    SpecViewOpts,
};
use pretty_assertions::{assert_eq, assert_matches};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_SCHEMA_STR,
    apply_manifest_and_get_id,
    assert_batch_indexes,
    assert_resource_view_fields,
    assert_single_batch_problem,
    create_variable_set,
    secret_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-050
contract_test!(get_all_successes, super::test_get_all_successes);

pub async fn test_get_all_successes(h: &impl FacadeContractHarness) {
    let id_a = create_variable_set(h, TestAccount::Alice, "batch-a").await;
    let id_b = create_variable_set(h, TestAccount::Alice, "batch-b").await;
    let facade = h.facade_for(TestAccount::Alice);

    let selector = vec![
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
            name: Some("batch-a".parse().unwrap()),
        },
        ResourceRef {
            account: None,
            r#type: Some(
                VARIABLE_SET_CANONICAL_SELECTOR
                    .parse::<TypeName>()
                    .unwrap()
                    .into(),
            ),
            id: Some(id_b),
            did: None,
            name: None,
        },
    ];

    let response = facade.get(selector, SpecViewOpts::ENCRYPTED).await.unwrap();

    assert_batch_indexes(&response, &[0, 1], &[]);
    assert_eq!(response.successes.len(), 2);

    let by_index: std::collections::HashMap<usize, &kamu_resources::Resource> = response
        .successes
        .iter()
        .map(|s| (s.request_index, &s.item))
        .collect();

    let view_a = by_index[&0];
    let view_b = by_index[&1];

    assert_resource_view_fields(view_a, VariableSetResource::schema(), "batch-a");
    assert_eq!(view_a.headers.id, id_a);

    assert_resource_view_fields(view_b, VariableSetResource::schema(), "batch-b");
    assert_eq!(view_b.headers.id, id_b);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-051
contract_test!(
    get_mixed_successes_problems,
    super::test_get_mixed_successes_problems
);

pub async fn test_get_mixed_successes_problems(h: &impl FacadeContractHarness) {
    let id_existing = create_variable_set(h, TestAccount::Alice, "mixed-a").await;
    let absent_id = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .get(
            vec![
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
                    name: Some("mixed-a".parse().unwrap()),
                }, // idx 0 — exists
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
                    name: Some("no-such-name".parse().unwrap()),
                }, // idx 1 — missing name
                ResourceRef {
                    account: None,
                    r#type: Some(
                        VARIABLE_SET_CANONICAL_SELECTOR
                            .parse::<TypeName>()
                            .unwrap()
                            .into(),
                    ),
                    id: Some(id_existing),
                    did: None,
                    name: None,
                }, // idx 2 — exists by id
                ResourceRef {
                    account: None,
                    r#type: Some(
                        VARIABLE_SET_CANONICAL_SELECTOR
                            .parse::<TypeName>()
                            .unwrap()
                            .into(),
                    ),
                    id: Some(absent_id),
                    did: None,
                    name: None,
                }, // idx 3 — missing id
            ],
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();

    assert_batch_indexes(&response, &[0, 2], &[1, 3]);

    let problem_by_index: std::collections::HashMap<
        usize,
        &kamu_resources_facade::ResourceLookupProblem,
    > = response
        .problems
        .iter()
        .map(|p| (p.request_index, &p.error))
        .collect();

    assert_matches!(
        problem_by_index[&1],
        ResourceLookupProblem::NameNotFound(_),
        "idx 1 must be NameNotFound"
    );
    assert_matches!(
        problem_by_index[&3],
        ResourceLookupProblem::IDNotFound(_),
        "idx 3 must be IDNotFound"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-052
contract_test!(get_duplicate_refs, super::test_get_duplicate_refs);

pub async fn test_get_duplicate_refs(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "dup-ref").await;
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .get(
            vec![
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
                    name: Some("dup-ref".parse().unwrap()),
                }, // idx 0
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
                    name: Some("dup-ref".parse().unwrap()),
                }, // idx 1 — same ref
            ],
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();

    // Both occurrences succeed and both refer to the same resource.
    assert_batch_indexes(&response, &[0, 1], &[]);
    for s in &response.successes {
        assert_eq!(s.item.headers.id, id, "all dup refs resolve to same id");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-053
contract_test!(get_empty_refs, super::test_get_empty_refs);

/// An empty batch names nothing, so every batch operation answers with an
/// empty result rather than an error — batch calls stay idempotent.
///
/// There is nothing else to assert about an empty batch: the type and account
/// live on each ref, so a batch with no refs carries neither.
pub async fn test_get_empty_refs(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade.get(vec![], SpecViewOpts::ENCRYPTED).await.unwrap();
    assert!(response.successes.is_empty(), "get successes must be empty");
    assert!(response.problems.is_empty(), "get problems must be empty");

    let response = facade.get_handles(vec![]).await.unwrap();
    assert!(
        response.successes.is_empty(),
        "get_handles successes must be empty"
    );
    assert!(
        response.problems.is_empty(),
        "get_handles problems must be empty"
    );

    let response = facade
        .render_manifests(
            vec![],
            ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();
    assert!(
        response.successes.is_empty(),
        "render_manifests successes must be empty"
    );
    assert!(
        response.problems.is_empty(),
        "render_manifests problems must be empty"
    );

    let response = facade.delete(vec![]).await.unwrap();
    assert!(
        response.successes.is_empty(),
        "delete successes must be empty"
    );
    assert!(
        response.problems.is_empty(),
        "delete problems must be empty"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-054
contract_test!(get_wrong_schema, super::test_get_wrong_schema);

pub async fn test_get_wrong_schema(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "api-ver-batch").await;
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .get(
            vec![
                ResourceRef {
                    account: None,
                    r#type: Some(
                        SECRET_SET_CANONICAL_SELECTOR
                            .parse::<TypeName>()
                            .unwrap()
                            .into(),
                    ),
                    id: Some(id),
                    did: None,
                    name: None,
                }, // idx 0 — exists but wrong schema
            ],
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();

    assert_batch_indexes(&response, &[], &[0]);
    assert_matches!(
        &response.problems[0].error,
        ResourceLookupProblem::SchemaMismatch(_),
        "expected SchemaMismatch problem, got: {:?}",
        response.problems[0].error
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-055
contract_test!(get_handles_mirrors_get, super::test_get_handles_mirrors_get);

pub async fn test_get_handles_mirrors_get(h: &impl FacadeContractHarness) {
    let uid_a = create_variable_set(h, TestAccount::Alice, "idents-a").await;
    let absent_uid = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .get_handles(vec![
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
                name: Some("idents-a".parse().unwrap()),
            }, // idx 0 — exists
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
                name: Some("no-such-ident".parse().unwrap()),
            }, // idx 1 — missing
            ResourceRef {
                account: None,
                r#type: Some(
                    VARIABLE_SET_CANONICAL_SELECTOR
                        .parse::<TypeName>()
                        .unwrap()
                        .into(),
                ),
                id: Some(absent_uid),
                did: None,
                name: None,
            }, // idx 2 — missing id
        ])
        .await
        .unwrap();

    assert_batch_indexes(&response, &[0], &[1, 2]);
    assert_eq!(response.successes[0].item.id, uid_a);

    let problem_by_index: std::collections::HashMap<
        usize,
        &kamu_resources_facade::ResourceLookupProblem,
    > = response
        .problems
        .iter()
        .map(|p| (p.request_index, &p.error))
        .collect();

    assert_matches!(problem_by_index[&1], ResourceLookupProblem::NameNotFound(_));
    assert_matches!(problem_by_index[&2], ResourceLookupProblem::IDNotFound(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-056
contract_test!(
    render_manifests_all_successes,
    super::test_render_manifests_all_successes
);

pub async fn test_render_manifests_all_successes(h: &impl FacadeContractHarness) {
    let id_a = create_variable_set(h, TestAccount::Alice, "render-a").await;
    let id_b = create_variable_set(h, TestAccount::Alice, "render-b").await;
    let facade = h.facade_for(TestAccount::Alice);

    for format in [
        kamu_resources_facade::ResourceManifestFormat::Json,
        kamu_resources_facade::ResourceManifestFormat::Yaml,
    ] {
        let response = facade
            .render_manifests(
                vec![
                    ResourceRef {
                        account: None,
                        r#type: Some(
                            VARIABLE_SET_CANONICAL_SELECTOR
                                .parse::<TypeName>()
                                .unwrap()
                                .into(),
                        ),
                        id: Some(id_a),
                        did: None,
                        name: None,
                    }, // idx 0
                    ResourceRef {
                        account: None,
                        r#type: Some(
                            VARIABLE_SET_CANONICAL_SELECTOR
                                .parse::<TypeName>()
                                .unwrap()
                                .into(),
                        ),
                        id: Some(id_b),
                        did: None,
                        name: None,
                    }, // idx 1
                ],
                format,
                SpecViewOpts::ENCRYPTED,
            )
            .await
            .unwrap();

        assert_batch_indexes(&response, &[0, 1], &[]);

        for s in &response.successes {
            assert_eq!(s.item.format, format, "rendered format must match request");
            assert!(
                !s.item.manifest.is_empty(),
                "rendered manifest must not be empty"
            );

            // Parse and check schema are present
            let parsed: serde_json::Value = match format {
                kamu_resources_facade::ResourceManifestFormat::Json => {
                    serde_json::from_str(&s.item.manifest).expect("must be valid JSON")
                }
                kamu_resources_facade::ResourceManifestFormat::Yaml => {
                    let y: serde_yaml::Value =
                        serde_yaml::from_str(&s.item.manifest).expect("must be valid YAML");
                    serde_json::to_value(y).unwrap()
                }
            };
            assert_eq!(
                parsed["$schema"], VARIABLE_SET_SCHEMA_STR,
                "schema mismatch"
            );
            assert_eq!(parsed["$schema"], VARIABLE_SET_SCHEMA_STR);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-057
contract_test!(
    render_manifests_mixed_successes_problems,
    super::test_render_manifests_mixed_successes_problems
);

pub async fn test_render_manifests_mixed_successes_problems(h: &impl FacadeContractHarness) {
    let uid_existing = create_variable_set(h, TestAccount::Alice, "render-mix").await;
    let absent_uid = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .render_manifests(
            vec![
                ResourceRef {
                    account: None,
                    r#type: Some(
                        VARIABLE_SET_CANONICAL_SELECTOR
                            .parse::<TypeName>()
                            .unwrap()
                            .into(),
                    ),
                    id: Some(uid_existing),
                    did: None,
                    name: None,
                }, // idx 0 — exists
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
                    name: Some("render-missing".parse().unwrap()),
                }, // idx 1 — missing
                ResourceRef {
                    account: None,
                    r#type: Some(
                        VARIABLE_SET_CANONICAL_SELECTOR
                            .parse::<TypeName>()
                            .unwrap()
                            .into(),
                    ),
                    id: Some(absent_uid),
                    did: None,
                    name: None,
                }, // idx 2 — missing id
            ],
            kamu_resources_facade::ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();

    assert_batch_indexes(&response, &[0], &[1, 2]);
    assert!(!response.successes[0].item.manifest.is_empty());

    let problem_by_index: std::collections::HashMap<
        usize,
        &kamu_resources_facade::ResourceLookupProblem,
    > = response
        .problems
        .iter()
        .map(|p| (p.request_index, &p.error))
        .collect();

    assert_matches!(problem_by_index[&1], ResourceLookupProblem::NameNotFound(_));
    assert_matches!(problem_by_index[&2], ResourceLookupProblem::IDNotFound(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-058
contract_test!(delete_all_successes, super::test_delete_all_successes);

pub async fn test_delete_all_successes(h: &impl FacadeContractHarness) {
    let id_a = create_variable_set(h, TestAccount::Alice, "del-many-a").await;
    let id_b = create_variable_set(h, TestAccount::Alice, "del-many-b").await;
    let id_c = create_variable_set(h, TestAccount::Alice, "del-many-c").await;
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .delete(vec![
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
                name: Some("del-many-a".parse().unwrap()),
            }, // idx 0
            ResourceRef {
                account: None,
                r#type: Some(
                    VARIABLE_SET_CANONICAL_SELECTOR
                        .parse::<TypeName>()
                        .unwrap()
                        .into(),
                ),
                id: Some(id_b),
                did: None,
                name: None,
            }, // idx 1
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
                name: Some("del-many-c".parse().unwrap()),
            }, // idx 2
        ])
        .await
        .unwrap();

    assert_batch_indexes(&response, &[0, 1, 2], &[]);

    let deleted_by_index: std::collections::HashMap<usize, kamu_resources::ResourceID> = response
        .successes
        .into_iter()
        .map(|s| (s.request_index, s.item))
        .collect();

    assert_eq!(deleted_by_index[&0], id_a, "idx 0 must return id_a");
    assert_eq!(deleted_by_index[&1], id_b, "idx 1 must return id_b");
    assert_eq!(deleted_by_index[&2], id_c, "idx 2 must return id_c");

    // All three resources must be gone
    for (name, id) in [
        ("del-many-a", id_a),
        ("del-many-b", id_b),
        ("del-many-c", id_c),
    ] {
        let get = facade
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
                    name: Some(name.parse().unwrap()),
                }],
                SpecViewOpts::ENCRYPTED,
            )
            .await
            .unwrap();
        assert_matches!(
            assert_single_batch_problem(get),
            ResourceLookupProblem::NameNotFound(_),
            "deleted resource '{name}' (id={id}) must not be found after delete"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-059
contract_test!(
    delete_mixed_successes_problems,
    super::test_delete_mixed_successes_problems
);

pub async fn test_delete_mixed_successes_problems(h: &impl FacadeContractHarness) {
    let uid_existing = create_variable_set(h, TestAccount::Alice, "del-mix-exists").await;
    let absent_uid = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .delete(vec![
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
                name: Some("del-mix-exists".parse().unwrap()),
            }, // idx 0 — exists
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
                name: Some("del-mix-missing".parse().unwrap()),
            }, /* idx 1 — missing
                * name */
            ResourceRef {
                account: None,
                r#type: Some(
                    VARIABLE_SET_CANONICAL_SELECTOR
                        .parse::<TypeName>()
                        .unwrap()
                        .into(),
                ),
                id: Some(absent_uid),
                did: None,
                name: None,
            }, // idx 2 — missing id
        ])
        .await
        .unwrap();

    assert_batch_indexes(&response, &[0], &[1, 2]);
    assert_eq!(
        response.successes[0].item, uid_existing,
        "success must return the deleted id"
    );

    let problem_by_index: std::collections::HashMap<
        usize,
        &kamu_resources_facade::ResourceLookupProblem,
    > = response
        .problems
        .iter()
        .map(|p| (p.request_index, &p.error))
        .collect();

    assert_matches!(
        problem_by_index[&1],
        ResourceLookupProblem::NameNotFound(_),
        "idx 1 must be NameNotFound"
    );
    assert_matches!(
        problem_by_index[&2],
        ResourceLookupProblem::IDNotFound(_),
        "idx 2 must be IDNotFound"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-060
// delete with duplicate refs: document the current behavior.
// The contract is that both occurrences succeed if the resource is resolved
// before deletion (pre-resolution deduplication), OR the first succeeds and
// the second returns NameNotFound.  We assert whichever branch fires and verify
// it is identical for local and remote.
contract_test!(
    delete_duplicate_refs_is_deterministic,
    super::test_delete_duplicate_refs_is_deterministic
);

pub async fn test_delete_duplicate_refs_is_deterministic(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "del-dup-ref").await;
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .delete(vec![
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
                name: Some("del-dup-ref".parse().unwrap()),
            }, // idx 0
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
                name: Some("del-dup-ref".parse().unwrap()),
            }, // idx 1 — duplicate
        ])
        .await
        .unwrap();

    // Acceptable contracts:
    // A) Both succeed (pre-resolution, same UID returned twice)
    // B) First succeeds, second is NameNotFound
    // Either way: no request index is lost.
    let total = response.successes.len() + response.problems.len();
    assert_eq!(total, 2, "all request indexes must be accounted for");

    if response.successes.len() == 2 {
        // Contract A: both succeed, same UID
        for s in &response.successes {
            assert_eq!(s.item, id, "duplicate delete success must refer to same id");
        }
    } else {
        // Contract B: first succeeds, second fails
        assert_eq!(response.successes.len(), 1);
        assert_eq!(response.problems.len(), 1);
        assert_eq!(response.successes[0].item, id);
        assert_matches!(
            &response.problems[0].error,
            ResourceLookupProblem::NameNotFound(_),
            "second duplicate must be NameNotFound, got: {:?}",
            response.problems[0].error
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-061
// Unsupported type must produce a batch-level
// Err(BatchResourceError::UnsupportedSelector), not Ok with per-item
// problems.  The CRUD dispatcher registry rejects the type before any refs are
// processed, for all four batch APIs.
contract_test!(
    batch_apis_reject_unsupported_type,
    super::test_batch_apis_reject_unsupported_type
);

pub async fn test_batch_apis_reject_unsupported_type(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "batch-bad-type-base").await;
    let facade = h.facade_for(TestAccount::Alice);
    let bad_type = "NoSuchResourceTypeXYZ";

    let selector = vec![ResourceRef {
        account: None,
        r#type: Some(bad_type.parse::<TypeName>().unwrap().into()),
        id: Some(id),
        did: None,
        name: None,
    }];

    let gm = facade.get(selector.clone(), SpecViewOpts::ENCRYPTED).await;
    assert_matches!(
        gm,
        Err(BatchResourceError::UnsupportedSelector(_)),
        "get: unsupported type must be a batch-level UnsupportedSelector, got: {gm:?}"
    );

    let gi = facade.get_handles(selector.clone()).await;
    assert_matches!(
        gi,
        Err(BatchResourceError::UnsupportedSelector(_)),
        "get_handles: unsupported type must be a batch-level UnsupportedSelector, got: {gi:?}"
    );

    let rm = facade
        .render_manifests(
            selector.clone(),
            ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
        )
        .await;
    assert_matches!(
        rm,
        Err(BatchResourceError::UnsupportedSelector(_)),
        "render_manifests: unsupported type must be a batch-level UnsupportedSelector, got: {rm:?}"
    );

    let dm = facade.delete(selector.clone()).await;
    assert_matches!(
        dm,
        Err(BatchResourceError::UnsupportedSelector(_)),
        "delete: unsupported type must be a batch-level UnsupportedSelector, got: {dm:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-108
contract_test!(
    batch_spans_types_and_accounts,
    super::test_batch_spans_types_and_accounts
);

/// One batch can name resources of **different types** belonging to
/// **different accounts** — the capability the ODF-shaped `ResourceRef` was
/// adopted for, since each ref carries its own `account` and `type`.
///
/// Pinned because the wire format permits a mixed batch structurally, so a
/// pipeline that quietly required a uniform target would still typecheck.
///
/// Alice is used for both halves: a *cross-account* batch is separately
/// governed by authorization (RF-105 covers the denial), so naming Bob here
/// would test permissions rather than fan-out.
pub async fn test_batch_spans_types_and_accounts(h: &impl FacadeContractHarness) {
    create_variable_set(h, TestAccount::Alice, "span-vars").await;
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        secret_set_manifest_json("span-secrets", None, &[("TOKEN", "t")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);

    let mixed_types = || {
        vec![
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
                name: Some("span-vars".parse().unwrap()),
            },
            ResourceRef {
                account: None,
                r#type: Some(
                    SECRET_SET_CANONICAL_SELECTOR
                        .parse::<TypeName>()
                        .unwrap()
                        .into(),
                ),
                id: None,
                did: None,
                name: Some("span-secrets".parse().unwrap()),
            },
        ]
    };

    // Every read path resolves both entries, and the positional indexes survive
    // the grouping — a fan-out that lost order would still "succeed" here, so
    // the indexes are the real assertion.
    let response = facade
        .get(mixed_types(), SpecViewOpts::ENCRYPTED)
        .await
        .expect("a batch spanning two types must resolve");
    assert_batch_indexes(&response, &[0, 1], &[]);

    let handles = facade
        .get_handles(mixed_types())
        .await
        .expect("get_handles must span two types");
    assert_batch_indexes(&handles, &[0, 1], &[]);

    let manifests = facade
        .render_manifests(
            mixed_types(),
            ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .expect("render_manifests must span two types");
    assert_batch_indexes(&manifests, &[0, 1], &[]);

    // Naming the account explicitly must behave the same as leaving it unset,
    // since both resolve to the caller. Grouping compares accounts *after*
    // resolution for exactly this reason: a structural comparison would split
    // one account spelled two ways into two groups.
    let spelled_out = vec![
        ResourceRef {
            account: Some(kamu_resources::ResourceAccountRef {
                id: None,
                did: None,
                name: Some(h.account_name(TestAccount::Alice)),
            }),
            r#type: Some(
                VARIABLE_SET_CANONICAL_SELECTOR
                    .parse::<TypeName>()
                    .unwrap()
                    .into(),
            ),
            id: None,
            did: None,
            name: Some("span-vars".parse().unwrap()),
        },
        ResourceRef {
            account: None,
            r#type: Some(
                SECRET_SET_CANONICAL_SELECTOR
                    .parse::<TypeName>()
                    .unwrap()
                    .into(),
            ),
            id: None,
            did: None,
            name: Some("span-secrets".parse().unwrap()),
        },
    ];
    let handles = facade
        .get_handles(spelled_out)
        .await
        .expect("one account spelled two ways must not split the batch");
    assert_batch_indexes(&handles, &[0, 1], &[]);

    // The write path last, since it consumes the fixtures.
    let deleted = facade
        .delete(mixed_types())
        .await
        .expect("delete must span two types");
    assert_batch_indexes(&deleted, &[0, 1], &[]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-170
contract_test!(
    ref_id_and_name_must_agree,
    super::test_ref_id_and_name_must_agree
);

/// A ref supplying **both** an `id` and a `name` asserts they describe the same
/// resource, and the mismatch must fail the entry.
///
/// ODF allows the pair as a consistency assertion, and the id is the
/// authoritative half the lookup uses — so if the name is not checked, the
/// assertion is decorative. Pairing one resource's id with another's name would
/// then read, render, or **delete** the resource the id names while the caller
/// believes they addressed the one they spelled out.
///
/// The surviving entry is asserted alongside the failing one: this must be a
/// per-item problem, not a whole-batch rejection.
pub async fn test_ref_id_and_name_must_agree(h: &impl FacadeContractHarness) {
    let alpha_id = create_variable_set(h, TestAccount::Alice, "agree-alpha").await;
    create_variable_set(h, TestAccount::Alice, "agree-beta").await;

    let facade = h.facade_for(TestAccount::Alice);

    // Index 0 pairs alpha's id with beta's name; index 1 is a plain, valid ref.
    let mismatched = || {
        vec![
            ResourceRef {
                account: None,
                r#type: Some(
                    VARIABLE_SET_CANONICAL_SELECTOR
                        .parse::<TypeName>()
                        .unwrap()
                        .into(),
                ),
                id: Some(alpha_id),
                did: None,
                name: Some("agree-beta".parse().unwrap()),
            },
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
                name: Some("agree-beta".parse().unwrap()),
            },
        ]
    };

    let response = facade
        .get(mismatched(), SpecViewOpts::ENCRYPTED)
        .await
        .expect("a mismatched ref is a per-item problem, not a batch failure");
    assert_batch_indexes(&response, &[1], &[0]);

    let handles = facade
        .get_handles(mismatched())
        .await
        .expect("get_handles must report the mismatch per item");
    assert_batch_indexes(&handles, &[1], &[0]);

    // The one that would silently destroy the wrong resource.
    let deleted = facade
        .delete(mismatched())
        .await
        .expect("delete must report the mismatch per item");
    assert_batch_indexes(&deleted, &[1], &[0]);

    // Alpha must still be there: its id was named, but under the wrong name.
    let survivors = facade
        .get_handles(vec![ResourceRef {
            account: None,
            r#type: Some(
                VARIABLE_SET_CANONICAL_SELECTOR
                    .parse::<TypeName>()
                    .unwrap()
                    .into(),
            ),
            id: Some(alpha_id),
            did: None,
            name: None,
        }])
        .await
        .expect("alpha must survive a delete that named it by a mismatched pair");
    assert_batch_indexes(&survivors, &[0], &[]);

    // A case-only variant is the *same* name: `ResourceName` equality is
    // deliberately case-insensitive and the repository resolves names that way,
    // so this pair agrees and must not be reported as a mismatch. Asserted on
    // every path, because a raw-string comparison in one of them would make the
    // paths disagree about the same ref.
    let case_variant = || {
        vec![ResourceRef {
            account: None,
            r#type: Some(
                VARIABLE_SET_CANONICAL_SELECTOR
                    .parse::<TypeName>()
                    .unwrap()
                    .into(),
            ),
            id: Some(alpha_id),
            did: None,
            name: Some("AGREE-ALPHA".parse().unwrap()),
        }]
    };

    let response = facade
        .get(case_variant(), SpecViewOpts::ENCRYPTED)
        .await
        .expect("a case-only name variant must resolve");
    assert_batch_indexes(&response, &[0], &[]);

    let handles = facade
        .get_handles(case_variant())
        .await
        .expect("get_handles must accept a case-only name variant");
    assert_batch_indexes(&handles, &[0], &[]);

    let manifests = facade
        .render_manifests(
            case_variant(),
            ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .expect("render_manifests must accept a case-only name variant");
    assert_batch_indexes(&manifests, &[0], &[]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-171
contract_test!(
    type_less_ref_resolves_across_types,
    super::test_type_less_ref_resolves_across_types
);

/// A ref that names no type resolves by searching every registered type.
///
/// ODF made `type` optional on `ResourceRef`, so a caller may address a
/// resource by name alone. Resolution happens in the batch pipelines' shared
/// front half, which is why all three paths are asserted: a fix applied to one
/// of them and not the others would leave the paths disagreeing about an
/// identical ref.
///
/// A miss is asserted alongside the hit, since a type-less ref that resolves
/// nothing must be a per-item problem rather than a whole-batch failure.
pub async fn test_type_less_ref_resolves_across_types(h: &impl FacadeContractHarness) {
    create_variable_set(h, TestAccount::Alice, "typeless-vars").await;

    let facade = h.facade_for(TestAccount::Alice);

    // Index 0 names a resource that exists in exactly one type; index 1 names
    // nothing at all.
    let refs = || {
        vec![
            ResourceRef {
                account: None,
                r#type: None,
                id: None,
                did: None,
                name: Some("typeless-vars".parse().unwrap()),
            },
            ResourceRef {
                account: None,
                r#type: None,
                id: None,
                did: None,
                name: Some("typeless-absent".parse().unwrap()),
            },
        ]
    };

    let response = facade
        .get(refs(), SpecViewOpts::ENCRYPTED)
        .await
        .expect("a type-less ref must resolve without naming a type");
    assert_batch_indexes(&response, &[0], &[1]);

    let handles = facade
        .get_handles(refs())
        .await
        .expect("get_handles must resolve a type-less ref");
    assert_batch_indexes(&handles, &[0], &[1]);

    let manifests = facade
        .render_manifests(
            refs(),
            ResourceManifestFormat::Json,
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .expect("render_manifests must resolve a type-less ref");
    assert_batch_indexes(&manifests, &[0], &[1]);

    // The write path last, since it consumes the fixture.
    let deleted = facade
        .delete(refs())
        .await
        .expect("delete must resolve a type-less ref");
    assert_batch_indexes(&deleted, &[0], &[1]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-172
contract_test!(
    type_less_ref_matching_several_types_is_ambiguous,
    super::test_type_less_ref_matching_several_types_is_ambiguous
);

/// A type-less ref whose name exists in several types is an addressing error.
///
/// A `ResourceRef` names *exactly one* resource, so matching in two types is
/// not a multi-match to be returned — it is a question the caller has to
/// answer. Picking a winner would make `kamu get <name>` silently resolve to
/// whichever type happened to sort first.
///
/// Contrast a type-less `ResourceSelector`, for which several matches are the
/// expected outcome; that asymmetry is the whole ref/selector distinction.
pub async fn test_type_less_ref_matching_several_types_is_ambiguous(
    h: &impl FacadeContractHarness,
) {
    // The same name in two different types.
    create_variable_set(h, TestAccount::Alice, "ambiguous-name").await;
    apply_manifest_and_get_id(
        h,
        TestAccount::Alice,
        secret_set_manifest_json("ambiguous-name", None, &[("TOKEN", "t")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);

    let refs = || {
        vec![ResourceRef {
            account: None,
            r#type: None,
            id: None,
            did: None,
            name: Some("ambiguous-name".parse().unwrap()),
        }]
    };

    let response = facade
        .get(refs(), SpecViewOpts::ENCRYPTED)
        .await
        .expect("an ambiguous ref is a per-item problem, not a batch failure");
    assert_batch_indexes(&response, &[], &[0]);

    let handles = facade
        .get_handles(refs())
        .await
        .expect("get_handles must report the ambiguity per item");
    assert_batch_indexes(&handles, &[], &[0]);

    // The one that would otherwise delete an arbitrary one of the two.
    let deleted = facade
        .delete(refs())
        .await
        .expect("delete must report the ambiguity per item");
    assert_batch_indexes(&deleted, &[], &[0]);

    // Naming the type disambiguates, and both resources must still exist —
    // the ambiguous delete above must not have removed either.
    let disambiguated = facade
        .get_handles(vec![ResourceRef {
            account: None,
            r#type: Some(
                VARIABLE_SET_CANONICAL_SELECTOR
                    .parse::<TypeName>()
                    .unwrap()
                    .into(),
            ),
            id: None,
            did: None,
            name: Some("ambiguous-name".parse().unwrap()),
        }])
        .await
        .expect("naming the type must resolve what the type-less ref could not");
    assert_batch_indexes(&disambiguated, &[0], &[]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
