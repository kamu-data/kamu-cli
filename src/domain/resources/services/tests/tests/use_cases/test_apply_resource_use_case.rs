// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    ApplyResourceAction,
    ApplyResourceApplicationDecision,
    ApplyResourceOutcome,
    ApplyResourceParams,
    ApplyResourcePlanningDecision,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

use crate::tests::use_cases::resource_use_case_base_harness::{
    ResourceUseCaseBaseHarness,
    ResourceUseCaseBaseHarnessOpts,
};
use crate::tests::utils::{TestResourceSpec, make_account_id};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// The planner and executor are each tested in isolation in their own test
// modules. Here we only test behaviour that is exclusive to the use-case layer:
// a basic end-to-end wiring smoke test, sanitizer integration (all code-paths),
// and the boundary where a sanitizer produces an invalid spec.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_end_to_end_create_and_retrieve() {
    // Smoke test: verifies planner → executor → persistence wiring is correct.
    // No sanitizer; just confirms the resource is created and retrievable.
    let harness = ResourceUseCaseBaseHarness::new();
    let account_id = make_account_id();

    let decision = harness
        .apply_test_uc()
        .apply(make_params(account_id, "res-a", "res-a"))
        .await
        .unwrap();

    let result = decision.expect_applied();
    assert_eq!(result.outcome, ApplyResourceOutcome::Created);

    let snapshot = harness.get_snapshot_by_uid(&result.uid).await.unwrap();
    pretty_assertions::assert_eq!(snapshot.spec, serde_json::json!({ "value": "res-a" }));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sanitizer_modifies_spec_on_create() {
    let harness = ResourceUseCaseBaseHarness::new_with_opts(ResourceUseCaseBaseHarnessOpts {
        with_sanitizer: true,
        ..Default::default()
    });
    let account_id = make_account_id();

    let decision = harness
        .apply_test_uc()
        .apply(make_params(account_id, "res-a", "original"))
        .await
        .unwrap();

    let result = decision.expect_applied();
    assert_eq!(result.outcome, ApplyResourceOutcome::Created);

    // The sanitizer appends "-sanitized"; the persisted spec must reflect that.
    let snapshot = harness.get_snapshot_by_uid(&result.uid).await.unwrap();
    pretty_assertions::assert_eq!(
        snapshot.spec,
        serde_json::json!({ "value": "original-sanitized" })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sanitizer_receives_current_spec_on_update() {
    let harness = ResourceUseCaseBaseHarness::new_with_opts(ResourceUseCaseBaseHarnessOpts {
        with_sanitizer: true,
        ..Default::default()
    });
    let account_id = make_account_id();

    let (uid, initial_snapshot) = harness
        .apply_and_assert_snapshot(account_id.clone(), "res-a")
        .await;
    // Sanitizer appends "-sanitized" even during initial apply
    pretty_assertions::assert_eq!(
        initial_snapshot.spec,
        serde_json::json!({ "value": "res-a-sanitized" })
    );

    let params = ApplyResourceParams {
        uid: Some(uid),
        ..make_params(account_id, "res-a", "updated")
    };

    let decision = harness.apply_test_uc().apply(params).await.unwrap();

    assert_eq!(
        decision.expect_applied().outcome,
        ApplyResourceOutcome::Updated
    );

    let snapshot = harness.get_snapshot_by_uid(&uid).await.unwrap();
    pretty_assertions::assert_eq!(
        snapshot.spec,
        serde_json::json!({ "value": "updated-sanitized" })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sanitizer_modifies_spec_in_plan() {
    // plan() must also run sanitize_params() — the returned plan's state must
    // reflect the sanitized spec, not the raw input.
    let harness = ResourceUseCaseBaseHarness::new_with_opts(ResourceUseCaseBaseHarnessOpts {
        with_sanitizer: true,
        ..Default::default()
    });
    let account_id = make_account_id();

    let decision = harness
        .apply_test_uc()
        .plan(make_params(account_id, "res-a", "original"))
        .await
        .unwrap();

    pretty_assertions::assert_matches!(
        decision,
        ApplyResourcePlanningDecision::Planned(ref plan)
            if plan.action == ApplyResourceAction::Create
            && plan.state.spec.value == "original-sanitized"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_no_sanitizer_passes_spec_unchanged() {
    let harness = ResourceUseCaseBaseHarness::new();
    let account_id = make_account_id();

    let decision = harness
        .apply_test_uc()
        .apply(make_params(account_id, "res-a", "original"))
        .await
        .unwrap();

    let result = decision.expect_applied();

    let snapshot = harness.get_snapshot_by_uid(&result.uid).await.unwrap();
    pretty_assertions::assert_eq!(snapshot.spec, serde_json::json!({ "value": "original" }));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_rejected_when_sanitized_spec_fails_validation() {
    // A sanitizer that clears the value produces an empty string, which fails
    // TestResourceSpec validation → the use case must return Rejected, not panic.
    use std::sync::Arc;

    use crate::tests::utils::TestResourceSpecClearingSanitizer;

    let harness = ResourceUseCaseBaseHarness::new_with_opts(ResourceUseCaseBaseHarnessOpts {
        with_sanitizer: false, // we'll wire the clearing sanitizer manually below
        ..Default::default()
    });

    // Build a catalog on top of the harness's catalog with the clearing sanitizer.
    let mut b = dill::CatalogBuilder::new_chained(harness.catalog());
    b.add_value(TestResourceSpecClearingSanitizer)
        .bind::<dyn kamu_resources::ResourceSpecSanitizer<crate::tests::utils::TestResource>, TestResourceSpecClearingSanitizer>();
    let catalog = b.build();

    let apply_svc: Arc<
        dyn kamu_resources::ApplyResourceUseCase<crate::tests::utils::TestResource>,
    > = catalog.get_one().unwrap();

    let account_id = make_account_id();
    let decision = apply_svc
        .apply(make_params(account_id, "res-a", "non-empty"))
        .await
        .unwrap();

    pretty_assertions::assert_matches!(decision, ApplyResourceApplicationDecision::Rejected(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_params(
    account_id: odf::AccountID,
    name: &str,
    value: &str,
) -> ApplyResourceParams<crate::tests::utils::TestResource> {
    ApplyResourceParams {
        uid: None,
        metadata: BaseResourceServiceHarness::make_metadata_input(account_id, name),
        spec: TestResourceSpec {
            value: value.to_string(),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
