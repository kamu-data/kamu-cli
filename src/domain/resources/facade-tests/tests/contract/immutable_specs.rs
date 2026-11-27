// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ApplyResourceOutcome, ApplyResourceRejectionCategory};
use kamu_resources_facade::{ApplyManifestRequest, ResourceFacade, ResourceManifestFormat};
use kamu_resources_services::testing::{ImmutableTestResource, TestTargets};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, LocalFacadeHarnessOpts, TestAccount};
use crate::helpers::assert_plan_and_apply_rejected;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-024 / RF-143
contract_test!(
    changed_immutable_spec_is_rejected,
    super::test_changed_immutable_spec_is_rejected,
    opts = super::LocalFacadeHarnessOpts::with_test_resources()
);

pub async fn test_changed_immutable_spec_is_rejected(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let unchanged = || manifest(TestTargets::NAME, "auto");
    assert_eq!(
        apply(facade.as_ref(), unchanged()).await,
        ApplyResourceOutcome::Created
    );
    assert_eq!(
        apply(facade.as_ref(), unchanged()).await,
        ApplyResourceOutcome::Untouched
    );

    let message = assert_plan_and_apply_rejected(
        facade.as_ref(),
        manifest(TestTargets::NAME, "manual"),
        ApplyResourceRejectionCategory::ImmutableFieldChanged,
    )
    .await;
    assert!(message.contains("is immutable"), "{message}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-143
contract_test!(
    missing_referenced_object_is_rejected,
    super::test_missing_referenced_object_is_rejected,
    opts = super::LocalFacadeHarnessOpts::with_test_resources()
);

pub async fn test_missing_referenced_object_is_rejected(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let message = assert_plan_and_apply_rejected(
        facade.as_ref(),
        manifest("no-such-target", "auto"),
        ApplyResourceRejectionCategory::ReferencedObjectMissing,
    )
    .await;
    assert!(message.contains("no-such-target"), "{message}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn manifest(target_name: &str, mode: &str) -> ApplyManifestRequest {
    ApplyManifestRequest {
        format: ResourceManifestFormat::Json,
        manifest: serde_json::json!({
            "$schema": ImmutableTestResource::SCHEMA_STR,
            "headers": { "name": "immutable" },
            "spec": { "target": { "name": target_name }, "mode": mode },
        })
        .to_string(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn apply(facade: &dyn ResourceFacade, request: ApplyManifestRequest) -> ApplyResourceOutcome {
    facade
        .apply_manifest(request)
        .await
        .unwrap()
        .expect_applied()
        .outcome
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
