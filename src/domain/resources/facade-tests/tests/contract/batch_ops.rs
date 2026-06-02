// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::ApplyResourceOutcome;
use kamu_resources_facade::{
    ApplyManifestRequest,
    ResourceBatchSelector,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourceRef,
    SpecViewMode,
};
use pretty_assertions::assert_eq;

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

async fn create_resource(
    h: &impl FacadeContractHarness,
    name: &str,
) -> kamu_resources::ResourceUID {
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
    result.metadata.uid
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-050
contract_test!(get_many_all_successes, super::test_get_many_all_successes);

pub async fn test_get_many_all_successes(h: &impl FacadeContractHarness) {
    let uid_a = create_resource(h, "batch-a").await;
    let uid_b = create_resource(h, "batch-b").await;
    let facade = h.facade_for(TestAccount::Alice);

    let selector = ResourceBatchSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_refs: vec![
            ResourceRef::ByName("batch-a".to_string()),
            ResourceRef::ById(uid_b),
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
    assert_eq!(view_a.metadata.uid, uid_a);

    assert_resource_view_fields(
        view_b,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "batch-b",
    );
    assert_eq!(view_b.metadata.uid, uid_b);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-051
contract_test!(
    get_many_mixed_successes_problems,
    super::test_get_many_mixed_successes_problems
);

pub async fn test_get_many_mixed_successes_problems(h: &impl FacadeContractHarness) {
    let uid_existing = create_resource(h, "mixed-a").await;
    let absent_uid = kamu_resources::ResourceUID::new(uuid::Uuid::new_v4());
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
                    ResourceRef::ById(uid_existing),            // idx 2 — exists by uid
                    ResourceRef::ById(absent_uid),              // idx 3 — missing uid
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
        matches!(problem_by_index[&3], ResourceLookupProblem::UIDNotFound(_)),
        "idx 3 must be UIDNotFound"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-052
contract_test!(get_many_duplicate_refs, super::test_get_many_duplicate_refs);

pub async fn test_get_many_duplicate_refs(h: &impl FacadeContractHarness) {
    let uid = create_resource(h, "dup-ref").await;
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
        assert_eq!(s.item.metadata.uid, uid, "all dup refs resolve to same uid");
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

// RF-054
contract_test!(
    get_many_wrong_api_version,
    super::test_get_many_wrong_api_version
);

pub async fn test_get_many_wrong_api_version(h: &impl FacadeContractHarness) {
    let uid = create_resource(h, "api-ver-batch").await;
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .get_many(
            ResourceBatchSelector {
                account: None,
                kind: VARIABLE_SET_KIND.to_string(),
                api_version: Some("v0.never.existed".to_string()),
                resource_refs: vec![
                    ResourceRef::ById(uid), // idx 0 — exists but wrong api_version
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
    let absent_uid = kamu_resources::ResourceUID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    let response = facade
        .get_identities(ResourceBatchSelector {
            account: None,
            kind: VARIABLE_SET_KIND.to_string(),
            api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
            resource_refs: vec![
                ResourceRef::ByName("idents-a".to_string()), // idx 0 — exists
                ResourceRef::ByName("no-such-ident".to_string()), // idx 1 — missing
                ResourceRef::ById(absent_uid),               // idx 2 — missing uid
            ],
        })
        .await
        .unwrap();

    assert_batch_indexes(&response, &[0], &[1, 2]);
    assert_eq!(response.successes[0].item.uid, uid_a);

    let problem_by_index: std::collections::HashMap<
        usize,
        &kamu_resources_facade::ResourceLookupProblem,
    > = response
        .problems
        .iter()
        .map(|p| (p.request_index, &p.error))
        .collect();

    assert!(matches!(
        problem_by_index[&1],
        ResourceLookupProblem::NameNotFound(_)
    ));
    assert!(matches!(
        problem_by_index[&2],
        ResourceLookupProblem::UIDNotFound(_)
    ));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-056
contract_test!(
    render_manifests_all_successes,
    super::test_render_manifests_all_successes
);

pub async fn test_render_manifests_all_successes(h: &impl FacadeContractHarness) {
    let uid_a = create_resource(h, "render-a").await;
    let uid_b = create_resource(h, "render-b").await;
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
                        ResourceRef::ById(uid_a), // idx 0
                        ResourceRef::ById(uid_b), // idx 1
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
    let absent_uid = kamu_resources::ResourceUID::new(uuid::Uuid::new_v4());
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
                    ResourceRef::ById(absent_uid),                     // idx 2 — missing uid
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

    assert!(matches!(
        problem_by_index[&1],
        ResourceLookupProblem::NameNotFound(_)
    ));
    assert!(matches!(
        problem_by_index[&2],
        ResourceLookupProblem::UIDNotFound(_)
    ));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
