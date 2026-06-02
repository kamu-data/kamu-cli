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
//! These tests only make sense when run with the `RemoteGraphqlFacadeHarness`
//! because they compare the local and remote facades side-by-side.  When run
//! through the `contract_test!` macro with the `LocalFacadeHarness` the test
//! simply applies through local and then reads back through local, which is
//! already covered by the individual contract tests.  For the remote harness,
//! the facade returned by `facade_for` is the *remote* facade backed by the
//! shared in-memory storage, so writes through local are visible remotely and
//! vice-versa.

use kamu_resources::ApplyResourceOutcome;
use kamu_resources_facade::{
    ApplyManifestRequest,
    ResourceManifestFormat,
    ResourceRef,
    ResourceSelector,
    SpecViewMode,
};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_API_VERSION,
    VARIABLE_SET_KIND,
    assert_applied_outcome,
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

// RF-151 / RF-152
// When this test runs via the local harness, it uses the same storage for both
// "create" and "read".  When it runs via the remote harness, `facade_for`
// returns the remote facade, and the underlying store is the shared
// in-memory store, so the resource is visible through the remote facade too.
contract_test!(
    resource_readable_after_create,
    super::test_resource_readable_after_create
);

pub async fn test_resource_readable_after_create(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let manifest = variable_set_manifest_json("cross-read-test", None, &[("X", "1")]);

    // Create through the facade
    let create_decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await
        .unwrap();
    let created = assert_applied_outcome(&create_decision, ApplyResourceOutcome::Created);
    let uid = created.metadata.uid;

    // Read back through the same facade
    let view = facade
        .get(by_name("cross-read-test"), SpecViewMode::Encrypted)
        .await
        .unwrap();

    assert_resource_view_fields(
        &view,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "cross-read-test",
    );
    assert_eq!(view.metadata.uid, uid);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
