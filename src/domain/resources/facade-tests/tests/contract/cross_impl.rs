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
    VARIABLE_SET_KIND,
    VARIABLE_SET_SCHEMA,
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
        resource_ref: ResourceRef::ByName(name.parse().unwrap()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-150
contract_test!(same_supported_kinds, super::test_same_supported_kinds);

pub async fn test_same_supported_kinds(h: &impl FacadeContractHarness) {
    let local = h.local_facade_for(TestAccount::Alice);
    let remote = h.facade_for(TestAccount::Alice);

    let mut local_kinds = local.list_supported_kinds().await.unwrap();
    let mut remote_kinds = remote.list_supported_kinds().await.unwrap();

    local_kinds.sort_by(|a, b| a.schema.cmp(&b.schema));
    remote_kinds.sort_by(|a, b| a.schema.cmp(&b.schema));

    assert!(!local_kinds.is_empty(), "descriptors must not be empty");
    assert_eq!(
        local_kinds, remote_kinds,
        "local and remote must report identical supported kinds"
    );
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

    let id = {
        let d = local
            .apply_manifest(ApplyManifestRequest {
                format: ResourceManifestFormat::Json,
                manifest: variable_set_manifest_json("cross-local-to-remote", None, &[("X", "1")]),
            })
            .await
            .unwrap();
        assert_applied_outcome(&d, ApplyResourceOutcome::Created)
            .headers
            .id
    };

    let view = remote
        .get(by_name("cross-local-to-remote"), SpecViewMode::Encrypted)
        .await
        .unwrap();

    assert_resource_view_fields(
        &view,
        VARIABLE_SET_KIND,
        VARIABLE_SET_SCHEMA,
        "cross-local-to-remote",
    );
    assert_eq!(view.headers.id, id);
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

    let id = {
        let d = remote
            .apply_manifest(ApplyManifestRequest {
                format: ResourceManifestFormat::Json,
                manifest: variable_set_manifest_json("cross-remote-to-local", None, &[("X", "1")]),
            })
            .await
            .unwrap();
        assert_applied_outcome(&d, ApplyResourceOutcome::Created)
            .headers
            .id
    };

    let view = local
        .get(by_name("cross-remote-to-local"), SpecViewMode::Encrypted)
        .await
        .unwrap();

    assert_resource_view_fields(
        &view,
        VARIABLE_SET_KIND,
        VARIABLE_SET_SCHEMA,
        "cross-remote-to-local",
    );
    assert_eq!(view.headers.id, id);
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
    let local = h.local_facade_for(TestAccount::Alice);
    let remote = h.facade_for(TestAccount::Alice);

    let uid_a = {
        let manifest = variable_set_manifest_json("cross-batch-a", None, &[("X", "1")]);
        let d = local
            .apply_manifest(ApplyManifestRequest {
                format: ResourceManifestFormat::Json,
                manifest,
            })
            .await
            .unwrap();
        assert_applied_outcome(&d, ApplyResourceOutcome::Created)
            .headers
            .id
    };
    let absent_uid = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());

    let batch_selector = ResourceBatchSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        resource_refs: vec![
            ResourceRef::ByName("cross-batch-a".parse().unwrap()), // idx 0 — exists
            ResourceRef::ByName("cross-batch-missing".parse().unwrap()), // idx 1 — missing name
            ResourceRef::ById(absent_uid),                         // idx 2 — missing id
        ],
    };

    // get_many: both facades must return the same structure
    let local_get = local
        .get_many(batch_selector.clone(), SpecViewMode::Encrypted)
        .await
        .unwrap();
    let remote_get = remote
        .get_many(batch_selector.clone(), SpecViewMode::Encrypted)
        .await
        .unwrap();
    assert_batch_indexes(&local_get, &[0], &[1, 2]);
    assert_batch_indexes(&remote_get, &[0], &[1, 2]);
    assert_eq!(local_get.successes[0].item.headers.id, uid_a);
    assert_eq!(remote_get.successes[0].item.headers.id, uid_a);
    assert_matches!(
        &local_get
            .problems
            .iter()
            .find(|p| p.request_index == 1)
            .unwrap()
            .error,
        ResourceLookupProblem::NameNotFound(_)
    );
    assert_matches!(
        &remote_get
            .problems
            .iter()
            .find(|p| p.request_index == 1)
            .unwrap()
            .error,
        ResourceLookupProblem::NameNotFound(_)
    );
    assert_matches!(
        &local_get
            .problems
            .iter()
            .find(|p| p.request_index == 2)
            .unwrap()
            .error,
        ResourceLookupProblem::IDNotFound(_)
    );
    assert_matches!(
        &remote_get
            .problems
            .iter()
            .find(|p| p.request_index == 2)
            .unwrap()
            .error,
        ResourceLookupProblem::IDNotFound(_)
    );

    // get_identities: both facades must agree on success id and problem indexes
    let local_id = local.get_identities(batch_selector.clone()).await.unwrap();
    let remote_id = remote.get_identities(batch_selector.clone()).await.unwrap();
    assert_batch_indexes(&local_id, &[0], &[1, 2]);
    assert_batch_indexes(&remote_id, &[0], &[1, 2]);
    assert_eq!(local_id.successes[0].item.id, uid_a);
    assert_eq!(remote_id.successes[0].item.id, uid_a);

    // render_manifests: both facades must return a non-empty manifest for the found
    // resource
    let local_render = local
        .render_manifests(
            batch_selector.clone(),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    let remote_render = remote
        .render_manifests(
            batch_selector.clone(),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    assert_batch_indexes(&local_render, &[0], &[1, 2]);
    assert_batch_indexes(&remote_render, &[0], &[1, 2]);
    let local_manifest: serde_json::Value =
        serde_json::from_str(&local_render.successes[0].item.manifest).unwrap();
    let remote_manifest: serde_json::Value =
        serde_json::from_str(&remote_render.successes[0].item.manifest).unwrap();
    assert_eq!(
        local_manifest, remote_manifest,
        "local and remote must render identical JSON manifests"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-155: local and remote produce equivalent apply decisions.
//
// Runs create → update → untouched → plan → rejection through *both* the local
// and remote facades and directly compares decision variants at each step.
// Separate resource names are used for the local and remote sequences to avoid
// name collisions in the shared backing store (both facades write to the same
// in-memory store in the remote harness).
contract_test!(apply_equivalence, super::test_apply_equivalence);

pub async fn test_apply_equivalence(h: &impl FacadeContractHarness) {
    let local = h.local_facade_for(TestAccount::Alice);
    let remote = h.facade_for(TestAccount::Alice);

    // --- Create ---
    let local_create = local
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("cross-apply-eq-local", None, &[("X", "1")]),
        })
        .await
        .unwrap();
    let remote_create = remote
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("cross-apply-eq-remote", None, &[("X", "1")]),
        })
        .await
        .unwrap();
    assert_applied_outcome(&local_create, ApplyResourceOutcome::Created);
    assert_applied_outcome(&remote_create, ApplyResourceOutcome::Created);

    // --- Update ---
    let local_update = local
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("cross-apply-eq-local", None, &[("X", "2")]),
        })
        .await
        .unwrap();
    let remote_update = remote
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("cross-apply-eq-remote", None, &[("X", "2")]),
        })
        .await
        .unwrap();
    assert_applied_outcome(&local_update, ApplyResourceOutcome::Updated);
    assert_applied_outcome(&remote_update, ApplyResourceOutcome::Updated);

    // --- Untouched (same manifest re-applied) ---
    let local_untouched = local
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("cross-apply-eq-local", None, &[("X", "2")]),
        })
        .await
        .unwrap();
    let remote_untouched = remote
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("cross-apply-eq-remote", None, &[("X", "2")]),
        })
        .await
        .unwrap();
    assert_applied_outcome(&local_untouched, ApplyResourceOutcome::Untouched);
    assert_applied_outcome(&remote_untouched, ApplyResourceOutcome::Untouched);

    // --- Plan: untouched round-trip ---
    let local_plan = local
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("cross-apply-eq-local", None, &[("X", "2")]),
        })
        .await
        .unwrap();
    let remote_plan = remote
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("cross-apply-eq-remote", None, &[("X", "2")]),
        })
        .await
        .unwrap();
    assert!(
        matches!(
            local_plan,
            ApplyManifestPlanningDecision::Planned(ref p)
            if p.outcome == ApplyResourceOutcome::Untouched
        ),
        "local: expected Planned(Untouched), got: {local_plan:?}"
    );
    assert!(
        matches!(
            remote_plan,
            ApplyManifestPlanningDecision::Planned(ref p)
            if p.outcome == ApplyResourceOutcome::Untouched
        ),
        "remote: expected Planned(Untouched), got: {remote_plan:?}"
    );

    // --- Rejection (business validation: empty variables) ---
    // Empty variables deserializes correctly but fails VariableSetSpec::validate()
    // inside the lifecycle, so both apply and plan return
    // Ok(Rejected(BusinessValidationFailed)).
    let reject_manifest = serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA,
        "headers": {"name": "cross-apply-eq-rejected"},
        "spec": {"variables": {}}
    })
    .to_string();
    let local_reject = local
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: reject_manifest.clone(),
        })
        .await;
    let remote_reject = remote
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: reject_manifest.clone(),
        })
        .await;
    assert!(
        matches!(
            local_reject,
            Ok(kamu_resources::ApplyManifestApplicationDecision::Rejected(
                kamu_resources::ApplyManifestRejection {
                    category:
                        kamu_resources::ApplyResourceRejectionCategory::BusinessValidationFailed,
                    ..
                }
            ))
        ),
        "local apply: expected Ok(Rejected(BusinessValidationFailed)), got: {local_reject:?}"
    );
    assert!(
        matches!(
            remote_reject,
            Ok(kamu_resources::ApplyManifestApplicationDecision::Rejected(
                kamu_resources::ApplyManifestRejection {
                    category:
                        kamu_resources::ApplyResourceRejectionCategory::BusinessValidationFailed,
                    ..
                }
            ))
        ),
        "remote apply: expected Ok(Rejected(BusinessValidationFailed)), got: {remote_reject:?}"
    );

    let local_plan_reject = local
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: reject_manifest.clone(),
        })
        .await;
    let remote_plan_reject = remote
        .plan_apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: reject_manifest,
        })
        .await;
    assert!(
        matches!(
            local_plan_reject,
            Ok(kamu_resources::ApplyManifestPlanningDecision::Rejected(
                kamu_resources::ApplyManifestRejection {
                    category:
                        kamu_resources::ApplyResourceRejectionCategory::BusinessValidationFailed,
                    ..
                }
            ))
        ),
        "local plan: expected Ok(Rejected(BusinessValidationFailed)), got: {local_plan_reject:?}"
    );
    assert!(
        matches!(
            remote_plan_reject,
            Ok(kamu_resources::ApplyManifestPlanningDecision::Rejected(
                kamu_resources::ApplyManifestRejection {
                    category:
                        kamu_resources::ApplyResourceRejectionCategory::BusinessValidationFailed,
                    ..
                }
            ))
        ),
        "remote plan: expected Ok(Rejected(BusinessValidationFailed)), got: {remote_plan_reject:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
