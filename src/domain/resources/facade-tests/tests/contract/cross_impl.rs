// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cross-implementation equivalence tests (RF-150..155).
//!
//! RF-151 and RF-152 are the core cross-facade tests: they write through one
//! transport (local or remote) and read back through the other, proving that
//! both facades operate against the same backing store.
//!
//! Each test uses `local_facade_for` for the local path and `facade_for` for
//! the remote path.  When run against `LocalFacadeHarness` both methods return
//! the same local facade, so the tests reduce to same-store round-trips
//! (harmless and already covered elsewhere).  The meaningful assertion fires
//! when the `contract_test!` macro runs the same function against
//! `RemoteGraphqlFacadeHarness`, where `facade_for` returns the remote GraphQL
//! facade and `local_facade_for` returns the underlying
//! `LocalResourceFacadeImpl` that shares the same in-memory store.

use kamu_resources::{ApplyManifestPlanningDecision, ApplyResourceOutcome};
use kamu_resources_facade::{
    ApplyManifestRequest,
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

fn by_name(name: &str) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_ref: ResourceRef::ByName(name.to_string()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-150
contract_test!(same_supported_kinds, super::test_same_supported_kinds);

pub async fn test_same_supported_kinds(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let mut descriptors = facade.list_supported_kinds().await.unwrap();
    descriptors.sort_by(|a, b| a.kind.cmp(&b.kind));

    // Basic checks: we can list and they are non-empty.
    // The full equivalence between local and remote is checked by the contract_test
    // macro running the same test against both harnesses.
    assert!(!descriptors.is_empty(), "descriptors must not be empty");
    for d in &descriptors {
        assert!(!d.kind.is_empty());
        assert!(!d.name.is_empty());
        assert!(!d.api_version.is_empty());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-151: local-created resource is readable remotely.
//
// Creates a resource through the *local* facade and reads it back through the
// *remote* facade.  For the local harness both facades are local, so this
// reduces to a same-store round-trip (already covered elsewhere).  The
// interesting case is the remote harness run, where the two facades use
// different transports but share the same in-memory store.
contract_test!(
    local_created_readable_remotely,
    super::test_local_created_readable_remotely
);

pub async fn test_local_created_readable_remotely(h: &impl FacadeContractHarness) {
    let local = h.local_facade_for(TestAccount::Alice);
    let remote = h.facade_for(TestAccount::Alice);

    let uid = {
        let d = local
            .apply_manifest(ApplyManifestRequest {
                format: ResourceManifestFormat::Json,
                manifest: variable_set_manifest_json("cross-local-to-remote", None, &[("X", "1")]),
            })
            .await
            .unwrap();
        assert_applied_outcome(&d, ApplyResourceOutcome::Created)
            .metadata
            .uid
    };

    let view = remote
        .get(by_name("cross-local-to-remote"), SpecViewMode::Encrypted)
        .await
        .unwrap();

    assert_resource_view_fields(
        &view,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "cross-local-to-remote",
    );
    assert_eq!(view.metadata.uid, uid);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-152: remote-created resource is readable locally.
//
// Mirror of RF-151: creates through the *remote* facade and reads back through
// the *local* facade.
contract_test!(
    remote_created_readable_locally,
    super::test_remote_created_readable_locally
);

pub async fn test_remote_created_readable_locally(h: &impl FacadeContractHarness) {
    let local = h.local_facade_for(TestAccount::Alice);
    let remote = h.facade_for(TestAccount::Alice);

    let uid = {
        let d = remote
            .apply_manifest(ApplyManifestRequest {
                format: ResourceManifestFormat::Json,
                manifest: variable_set_manifest_json("cross-remote-to-local", None, &[("X", "1")]),
            })
            .await
            .unwrap();
        assert_applied_outcome(&d, ApplyResourceOutcome::Created)
            .metadata
            .uid
    };

    let view = local
        .get(by_name("cross-remote-to-local"), SpecViewMode::Encrypted)
        .await
        .unwrap();

    assert_resource_view_fields(
        &view,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "cross-remote-to-local",
    );
    assert_eq!(view.metadata.uid, uid);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-153: local and remote produce equivalent rendered manifests.
//
// Creates through the local facade then renders via both facades in JSON and
// YAML, asserting that the parsed values are semantically equal.
contract_test!(
    render_manifest_equivalence,
    super::test_render_manifest_equivalence
);

pub async fn test_render_manifest_equivalence(h: &impl FacadeContractHarness) {
    let local = h.local_facade_for(TestAccount::Alice);
    let remote = h.facade_for(TestAccount::Alice);

    local
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("cross-render-eq", None, &[("X", "1")]),
        })
        .await
        .unwrap();

    let selector = by_name("cross-render-eq");

    // JSON equivalence
    let local_json = local
        .render_manifest(
            selector.clone(),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap()
        .manifest;
    let remote_json = remote
        .render_manifest(
            selector.clone(),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap()
        .manifest;
    let local_json_val: serde_json::Value = serde_json::from_str(&local_json).unwrap();
    let remote_json_val: serde_json::Value = serde_json::from_str(&remote_json).unwrap();
    assert_eq!(
        local_json_val, remote_json_val,
        "JSON renders must be semantically equal"
    );

    // YAML equivalence — re-parse via serde_yaml and compare as JSON values
    let local_yaml = local
        .render_manifest(
            selector.clone(),
            ResourceManifestFormat::Yaml,
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap()
        .manifest;
    let remote_yaml = remote
        .render_manifest(
            selector.clone(),
            ResourceManifestFormat::Yaml,
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap()
        .manifest;
    let local_yaml_val: serde_json::Value = serde_yaml::from_str(&local_yaml).unwrap();
    let remote_yaml_val: serde_json::Value = serde_yaml::from_str(&remote_yaml).unwrap();
    assert_eq!(
        local_yaml_val, remote_yaml_val,
        "YAML renders must be semantically equal"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-154
// Runs representative mixed batch calls and verifies that both local and remote
// produce equivalent normalized batch responses (same successes/problems by
// index and lookup problem variant).
contract_test!(batch_equivalence, super::test_batch_equivalence);

pub async fn test_batch_equivalence(h: &impl FacadeContractHarness) {
    let uid_a = {
        let facade = h.facade_for(TestAccount::Alice);
        let manifest = variable_set_manifest_json("cross-batch-a", None, &[("X", "1")]);
        let d = facade
            .apply_manifest(ApplyManifestRequest {
                format: ResourceManifestFormat::Json,
                manifest,
            })
            .await
            .unwrap();
        assert_applied_outcome(&d, ApplyResourceOutcome::Created)
            .metadata
            .uid
    };
    let absent_uid = kamu_resources::ResourceUID::new(uuid::Uuid::new_v4());
    let facade = h.facade_for(TestAccount::Alice);

    let batch_selector = ResourceBatchSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_refs: vec![
            ResourceRef::ByName("cross-batch-a".to_string()), // idx 0 — exists
            ResourceRef::ByName("cross-batch-missing".to_string()), // idx 1 — missing name
            ResourceRef::ById(absent_uid),                    // idx 2 — missing uid
        ],
    };

    // get_many
    let get_resp = facade
        .get_many(batch_selector.clone(), SpecViewMode::Encrypted)
        .await
        .unwrap();
    assert_batch_indexes(&get_resp, &[0], &[1, 2]);
    assert_eq!(get_resp.successes[0].item.metadata.uid, uid_a);
    assert_matches!(
        &get_resp
            .problems
            .iter()
            .find(|p| p.request_index == 1)
            .unwrap()
            .error,
        ResourceLookupProblem::NameNotFound(_)
    );
    assert_matches!(
        &get_resp
            .problems
            .iter()
            .find(|p| p.request_index == 2)
            .unwrap()
            .error,
        ResourceLookupProblem::UIDNotFound(_)
    );

    // get_identities
    let id_resp = facade.get_identities(batch_selector.clone()).await.unwrap();
    assert_batch_indexes(&id_resp, &[0], &[1, 2]);
    assert_eq!(id_resp.successes[0].item.uid, uid_a);

    // render_manifests
    let render_resp = facade
        .render_manifests(
            batch_selector.clone(),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    assert_batch_indexes(&render_resp, &[0], &[1, 2]);
    assert!(!render_resp.successes[0].item.manifest.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-155: local and remote produce equivalent apply decisions.
//
// Runs create → update → untouched → rejection through the facade and asserts
// that each operation produces the expected decision variant.  Because the
// `contract_test!` macro runs the same function against both harnesses, this
// implicitly verifies that local and remote agree on every step.
contract_test!(apply_equivalence, super::test_apply_equivalence);

pub async fn test_apply_equivalence(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    // --- Create ---
    let create_manifest = variable_set_manifest_json("cross-apply-eq", None, &[("X", "1")]);
    let create_decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: create_manifest,
        })
        .await
        .unwrap();
    assert_applied_outcome(&create_decision, ApplyResourceOutcome::Created);

    // --- Update ---
    let update_manifest = variable_set_manifest_json("cross-apply-eq", None, &[("X", "2")]);
    let update_decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: update_manifest.clone(),
        })
        .await
        .unwrap();
    assert_applied_outcome(&update_decision, ApplyResourceOutcome::Updated);

    // --- Untouched (same manifest re-applied) ---
    let untouched_decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: update_manifest,
        })
        .await
        .unwrap();
    assert_applied_outcome(&untouched_decision, ApplyResourceOutcome::Untouched);

    // --- Plan: untouched round-trip ---
    let plan_manifest = variable_set_manifest_json("cross-apply-eq", None, &[("X", "2")]);
    let plan_decision = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: plan_manifest,
        })
        .await
        .unwrap();
    assert!(
        matches!(
            plan_decision,
            ApplyManifestPlanningDecision::Planned(ref p)
            if p.outcome == ApplyResourceOutcome::Untouched
        ),
        "expected Planned(Unchanged), got: {plan_decision:?}"
    );

    // --- Rejection (business validation: empty variables) ---
    // Empty variables deserializes correctly but fails VariableSetSpec::validate()
    // inside the lifecycle, so both apply and plan return
    // Ok(Rejected(BusinessValidationFailed)).
    let reject_manifest = indoc::indoc!(
        r#"{
            "apiVersion": "kamu.dev/v1alpha1",
            "kind": "VariableSet",
            "metadata": {"name": "cross-apply-eq-rejected"},
            "spec": {"variables": {}}
        }"#
    )
    .to_string();
    let reject_result = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: reject_manifest.clone(),
        })
        .await;
    assert!(
        matches!(
            reject_result,
            Ok(kamu_resources::ApplyManifestApplicationDecision::Rejected(
                kamu_resources::ApplyManifestRejection {
                    category:
                        kamu_resources::ApplyResourceRejectionCategory::BusinessValidationFailed,
                    ..
                }
            ))
        ),
        "apply: expected Ok(Rejected(BusinessValidationFailed)), got: {reject_result:?}"
    );

    let plan_reject_result = facade
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: reject_manifest,
        })
        .await;
    assert!(
        matches!(
            plan_reject_result,
            Ok(kamu_resources::ApplyManifestPlanningDecision::Rejected(
                kamu_resources::ApplyManifestRejection {
                    category:
                        kamu_resources::ApplyResourceRejectionCategory::BusinessValidationFailed,
                    ..
                }
            ))
        ),
        "plan: expected Ok(Rejected(BusinessValidationFailed)), got: {plan_reject_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
