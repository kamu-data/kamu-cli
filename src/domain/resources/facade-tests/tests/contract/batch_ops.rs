// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ApplyResourceOutcome, ResourceManifestAccount};
use kamu_resources_facade::{
    ApplyManifestRequest,
    BatchResourceError,
    GetResourceError,
    ResourceBatchSelector,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourceRef,
    ResourceSelector,
    SpecViewMode,
};
use pretty_assertions::{assert_eq, assert_matches};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_API_VERSION,
    VARIABLE_SET_KIND,
    assert_applied_outcome,
    assert_batch_indexes,
    assert_resource_view_fields,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_resource(h: &impl FacadeContractHarness, name: &str) -> kamu_resources::ResourceID {
    let facade = h.facade_for(TestAccount::Alice);
    let manifest = variable_set_manifest_json(name, None, &[("K", "v")]);
    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await
        .unwrap();
    let result = assert_applied_outcome(&decision, ApplyResourceOutcome::Created);
    result.headers.id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-050
contract_test!(get_many_all_successes, super::test_get_many_all_successes);

pub async fn test_get_many_all_successes(h: &impl FacadeContractHarness) {
    let id_a = create_resource(h, "batch-a").await;
    let id_b = create_resource(h, "batch-b").await;
    let facade = h.facade_for(TestAccount::Alice);

    let selector = ResourceBatchSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_refs: vec![
            ResourceRef::ByName("batch-a".to_string()),
            ResourceRef::ById(id_b),
        ],
    };

    let response = facade
        .get_many(selector, SpecViewMode::Encrypted)
        .await
        .unwrap();

    assert_batch_indexes(&response, &[0, 1], &[]);
    assert_eq!(response.successes.len(), 2);

    let by_index: std::collections::HashMap<usize, &kamu_resources::ResourceView> = response
        .successes
        .iter()
        .map(|s| (s.request_index, &s.item))
        .collect();

    let view_a = by_index[&0];
    let view_b = by_index[&1];

    assert_resource_view_fields(
        view_a,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "batch-a",
    );
    assert_eq!(view_a.headers.id, id_a);

    assert_resource_view_fields(
        view_b,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "batch-b",
    );
    assert_eq!(view_b.headers.id, id_b);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-051
contract_test!(
    get_many_mixed_successes_problems,
    super::test_get_many_mixed_successes_problems
);

pub async fn test_get_many_mixed_successes_problems(h: &impl FacadeContractHarness) {
    let id_existing = create_resource(h, "mixed-a").await;
    let absent_id = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .get_many(
            ResourceBatchSelector {
                account: None,
                kind: VARIABLE_SET_KIND.to_string(),
                api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
                resource_refs: vec![
                    ResourceRef::ByName("mixed-a".to_string()), // idx 0 — exists
                    ResourceRef::ByName("no-such-name".to_string()), // idx 1 — missing name
                    ResourceRef::ById(id_existing),             // idx 2 — exists by id
                    ResourceRef::ById(absent_id),               // idx 3 — missing id
                ],
            },
            SpecViewMode::Encrypted,
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

    assert!(
        matches!(problem_by_index[&1], ResourceLookupProblem::NameNotFound(_)),
        "idx 1 must be NameNotFound"
    );
    assert!(
        matches!(problem_by_index[&3], ResourceLookupProblem::IDNotFound(_)),
        "idx 3 must be UIDNotFound"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-052
contract_test!(get_many_duplicate_refs, super::test_get_many_duplicate_refs);

pub async fn test_get_many_duplicate_refs(h: &impl FacadeContractHarness) {
    let id = create_resource(h, "dup-ref").await;
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .get_many(
            ResourceBatchSelector {
                account: None,
                kind: VARIABLE_SET_KIND.to_string(),
                api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
                resource_refs: vec![
                    ResourceRef::ByName("dup-ref".to_string()), // idx 0
                    ResourceRef::ByName("dup-ref".to_string()), // idx 1 — same ref
                ],
            },
            SpecViewMode::Encrypted,
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
contract_test!(get_many_empty_refs, super::test_get_many_empty_refs);

pub async fn test_get_many_empty_refs(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .get_many(
            ResourceBatchSelector {
                account: None,
                kind: VARIABLE_SET_KIND.to_string(),
                api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
                resource_refs: vec![],
            },
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();

    assert!(response.successes.is_empty(), "successes must be empty");
    assert!(response.problems.is_empty(), "problems must be empty");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-053A
contract_test!(
    get_many_empty_refs_validates_unsupported_kind,
    super::test_get_many_empty_refs_validates_unsupported_kind
);

pub async fn test_get_many_empty_refs_validates_unsupported_kind(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .get_many(
            ResourceBatchSelector {
                account: None,
                kind: "NoSuchResourceKindXYZ".to_string(),
                api_version: None,
                resource_refs: vec![],
            },
            SpecViewMode::Encrypted,
        )
        .await;

    assert_matches!(
        result,
        Err(BatchResourceError::UnsupportedDescriptor(_)),
        "empty batch with unsupported kind must still be rejected"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-053B
contract_test!(
    get_many_empty_refs_validates_bad_account,
    super::test_get_many_empty_refs_validates_bad_account
);

pub async fn test_get_many_empty_refs_validates_bad_account(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade
        .get_many(
            ResourceBatchSelector {
                account: Some(ResourceManifestAccount {
                    name: Some("unknown-resource-contract-account".to_string()),
                    id: None,
                }),
                kind: VARIABLE_SET_KIND.to_string(),
                api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
                resource_refs: vec![],
            },
            SpecViewMode::Encrypted,
        )
        .await;

    assert_matches!(
        result,
        Err(BatchResourceError::BadAccount(_)),
        "empty batch with bad account must still be rejected"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-053C
contract_test!(
    batch_empty_refs_validation_is_consistent,
    super::test_batch_empty_refs_validation_is_consistent
);

pub async fn test_batch_empty_refs_validation_is_consistent(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let bad_kind = "NoSuchResourceKindXYZ";
    let bad_account = Some(ResourceManifestAccount {
        name: Some("unknown-resource-contract-account".to_string()),
        id: None,
    });

    // get_identities: unsupported kind
    let gi_kind = facade
        .get_identities(ResourceBatchSelector {
            account: None,
            kind: bad_kind.to_string(),
            api_version: None,
            resource_refs: vec![],
        })
        .await;
    assert_matches!(
        gi_kind,
        Err(BatchResourceError::UnsupportedDescriptor(_)),
        "get_identities empty+bad kind must be rejected"
    );

    // get_identities: bad account
    let gi_acct = facade
        .get_identities(ResourceBatchSelector {
            account: bad_account.clone(),
            kind: VARIABLE_SET_KIND.to_string(),
            api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
            resource_refs: vec![],
        })
        .await;
    assert_matches!(
        gi_acct,
        Err(BatchResourceError::BadAccount(_)),
        "get_identities empty+bad account must be rejected"
    );

    // render_manifests: unsupported kind
    let rm_kind = facade
        .render_manifests(
            ResourceBatchSelector {
                account: None,
                kind: bad_kind.to_string(),
                api_version: None,
                resource_refs: vec![],
            },
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert_matches!(
        rm_kind,
        Err(BatchResourceError::UnsupportedDescriptor(_)),
        "render_manifests empty+bad kind must be rejected"
    );

    // render_manifests: bad account
    let rm_acct = facade
        .render_manifests(
            ResourceBatchSelector {
                account: bad_account.clone(),
                kind: VARIABLE_SET_KIND.to_string(),
                api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
                resource_refs: vec![],
            },
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert_matches!(
        rm_acct,
        Err(BatchResourceError::BadAccount(_)),
        "render_manifests empty+bad account must be rejected"
    );

    // delete_many: unsupported kind
    let dm_kind = facade
        .delete_many(ResourceBatchSelector {
            account: None,
            kind: bad_kind.to_string(),
            api_version: None,
            resource_refs: vec![],
        })
        .await;
    assert_matches!(
        dm_kind,
        Err(BatchResourceError::UnsupportedDescriptor(_)),
        "delete_many empty+bad kind must be rejected"
    );

    // delete_many: bad account
    let dm_acct = facade
        .delete_many(ResourceBatchSelector {
            account: bad_account.clone(),
            kind: VARIABLE_SET_KIND.to_string(),
            api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
            resource_refs: vec![],
        })
        .await;
    assert_matches!(
        dm_acct,
        Err(BatchResourceError::BadAccount(_)),
        "delete_many empty+bad account must be rejected"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-054
contract_test!(
    get_many_wrong_api_version,
    super::test_get_many_wrong_api_version
);

pub async fn test_get_many_wrong_api_version(h: &impl FacadeContractHarness) {
    let id = create_resource(h, "api-ver-batch").await;
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .get_many(
            ResourceBatchSelector {
                account: None,
                kind: VARIABLE_SET_KIND.to_string(),
                api_version: Some("v0.never.existed".to_string()),
                resource_refs: vec![
                    ResourceRef::ById(id), // idx 0 — exists but wrong api_version
                ],
            },
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();

    assert_batch_indexes(&response, &[], &[0]);
    assert!(
        matches!(
            &response.problems[0].error,
            ResourceLookupProblem::ApiVersionMismatch(_)
        ),
        "expected ApiVersionMismatch problem, got: {:?}",
        response.problems[0].error
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-055
contract_test!(
    get_identities_mirrors_get_many,
    super::test_get_identities_mirrors_get_many
);

pub async fn test_get_identities_mirrors_get_many(h: &impl FacadeContractHarness) {
    let uid_a = create_resource(h, "idents-a").await;
    let absent_uid = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .get_identities(ResourceBatchSelector {
            account: None,
            kind: VARIABLE_SET_KIND.to_string(),
            api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
            resource_refs: vec![
                ResourceRef::ByName("idents-a".to_string()), // idx 0 — exists
                ResourceRef::ByName("no-such-ident".to_string()), // idx 1 — missing
                ResourceRef::ById(absent_uid),               // idx 2 — missing id
            ],
        })
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
    let id_a = create_resource(h, "render-a").await;
    let id_b = create_resource(h, "render-b").await;
    let facade = h.facade_for(TestAccount::Alice);

    for format in [
        kamu_resources_facade::ResourceManifestFormat::Json,
        kamu_resources_facade::ResourceManifestFormat::Yaml,
    ] {
        let response = facade
            .render_manifests(
                ResourceBatchSelector {
                    account: None,
                    kind: VARIABLE_SET_KIND.to_string(),
                    api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
                    resource_refs: vec![
                        ResourceRef::ById(id_a), // idx 0
                        ResourceRef::ById(id_b), // idx 1
                    ],
                },
                format,
                SpecViewMode::Encrypted,
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

            // Parse and check kind/apiVersion are present
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
            assert_eq!(parsed["kind"], VARIABLE_SET_KIND);
            assert_eq!(parsed["apiVersion"], VARIABLE_SET_API_VERSION);
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
    let uid_existing = create_resource(h, "render-mix").await;
    let absent_uid = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .render_manifests(
            ResourceBatchSelector {
                account: None,
                kind: VARIABLE_SET_KIND.to_string(),
                api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
                resource_refs: vec![
                    ResourceRef::ById(uid_existing),                   // idx 0 — exists
                    ResourceRef::ByName("render-missing".to_string()), // idx 1 — missing
                    ResourceRef::ById(absent_uid),                     // idx 2 — missing id
                ],
            },
            kamu_resources_facade::ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
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
contract_test!(
    delete_many_all_successes,
    super::test_delete_many_all_successes
);

pub async fn test_delete_many_all_successes(h: &impl FacadeContractHarness) {
    let id_a = create_resource(h, "del-many-a").await;
    let id_b = create_resource(h, "del-many-b").await;
    let id_c = create_resource(h, "del-many-c").await;
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .delete_many(ResourceBatchSelector {
            account: None,
            kind: VARIABLE_SET_KIND.to_string(),
            api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
            resource_refs: vec![
                ResourceRef::ByName("del-many-a".to_string()), // idx 0
                ResourceRef::ById(id_b),                       // idx 1
                ResourceRef::ByName("del-many-c".to_string()), // idx 2
            ],
        })
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
                ResourceSelector {
                    account: None,
                    kind: VARIABLE_SET_KIND.to_string(),
                    api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
                    resource_ref: ResourceRef::ByName(name.to_string()),
                },
                SpecViewMode::Encrypted,
            )
            .await;
        assert!(
            matches!(get, Err(GetResourceError::LookupProblem(_))),
            "deleted resource '{name}' (id={id}) must not be found after delete_many"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-059
contract_test!(
    delete_many_mixed_successes_problems,
    super::test_delete_many_mixed_successes_problems
);

pub async fn test_delete_many_mixed_successes_problems(h: &impl FacadeContractHarness) {
    let uid_existing = create_resource(h, "del-mix-exists").await;
    let absent_uid = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .delete_many(ResourceBatchSelector {
            account: None,
            kind: VARIABLE_SET_KIND.to_string(),
            api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
            resource_refs: vec![
                ResourceRef::ByName("del-mix-exists".to_string()), // idx 0 — exists
                ResourceRef::ByName("del-mix-missing".to_string()), // idx 1 — missing name
                ResourceRef::ById(absent_uid),                     // idx 2 — missing id
            ],
        })
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

    assert!(
        matches!(problem_by_index[&1], ResourceLookupProblem::NameNotFound(_)),
        "idx 1 must be NameNotFound"
    );
    assert!(
        matches!(problem_by_index[&2], ResourceLookupProblem::IDNotFound(_)),
        "idx 2 must be UIDNotFound"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-060
// delete_many with duplicate refs: document the current behavior.
// The contract is that both occurrences succeed if the resource is resolved
// before deletion (pre-resolution deduplication), OR the first succeeds and
// the second returns NameNotFound.  We assert whichever branch fires and verify
// it is identical for local and remote.
contract_test!(
    delete_many_duplicate_refs_is_deterministic,
    super::test_delete_many_duplicate_refs_is_deterministic
);

pub async fn test_delete_many_duplicate_refs_is_deterministic(h: &impl FacadeContractHarness) {
    let id = create_resource(h, "del-dup-ref").await;
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .delete_many(ResourceBatchSelector {
            account: None,
            kind: VARIABLE_SET_KIND.to_string(),
            api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
            resource_refs: vec![
                ResourceRef::ByName("del-dup-ref".to_string()), // idx 0
                ResourceRef::ByName("del-dup-ref".to_string()), // idx 1 — duplicate
            ],
        })
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
            assert_eq!(
                s.item, id,
                "duplicate delete_many success must refer to same id"
            );
        }
    } else {
        // Contract B: first succeeds, second fails
        assert_eq!(response.successes.len(), 1);
        assert_eq!(response.problems.len(), 1);
        assert_eq!(response.successes[0].item, id);
        assert!(
            matches!(
                &response.problems[0].error,
                ResourceLookupProblem::NameNotFound(_)
            ),
            "second duplicate must be NameNotFound, got: {:?}",
            response.problems[0].error
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-061
// Unsupported kind must produce a batch-level
// Err(BatchResourceError::UnsupportedDescriptor), not Ok with per-item
// problems.  The CRUD dispatcher registry rejects the kind before any refs are
// processed, for all four batch APIs.
contract_test!(
    batch_apis_reject_unsupported_kind,
    super::test_batch_apis_reject_unsupported_kind
);

pub async fn test_batch_apis_reject_unsupported_kind(h: &impl FacadeContractHarness) {
    let id = create_resource(h, "batch-bad-kind-base").await;
    let facade = h.facade_for(TestAccount::Alice);
    let bad_kind = "NoSuchResourceKindXYZ";

    let selector = ResourceBatchSelector {
        account: None,
        kind: bad_kind.to_string(),
        api_version: None,
        resource_refs: vec![ResourceRef::ById(id)],
    };

    let gm = facade
        .get_many(selector.clone(), SpecViewMode::Encrypted)
        .await;
    assert!(
        matches!(gm, Err(BatchResourceError::UnsupportedDescriptor(_))),
        "get_many: unsupported kind must be a batch-level UnsupportedDescriptor, got: {gm:?}"
    );

    let gi = facade.get_identities(selector.clone()).await;
    assert!(
        matches!(gi, Err(BatchResourceError::UnsupportedDescriptor(_))),
        "get_identities: unsupported kind must be a batch-level UnsupportedDescriptor, got: {gi:?}"
    );

    let rm = facade
        .render_manifests(
            selector.clone(),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(rm, Err(BatchResourceError::UnsupportedDescriptor(_))),
        "render_manifests: unsupported kind must be a batch-level UnsupportedDescriptor, got: \
         {rm:?}"
    );

    let dm = facade.delete_many(selector.clone()).await;
    assert!(
        matches!(dm, Err(BatchResourceError::UnsupportedDescriptor(_))),
        "delete_many: unsupported kind must be a batch-level UnsupportedDescriptor, got: {dm:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
