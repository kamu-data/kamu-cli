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
    ResourceConditionStatus,
    ResourceConditionType,
    ResourcePhase,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
};
use messaging_outbox::OutboxProvider;

use crate::tests::use_cases::resource_use_case_base_harness::{
    ResourceUseCaseBaseHarness,
    ResourceUseCaseBaseHarnessOpts,
    SanitizerKind,
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

    let snapshot = harness.get_snapshot_by_id(&result.id).await.unwrap();
    pretty_assertions::assert_eq!(snapshot.spec, serde_json::json!({ "value": "res-a" }));

    // Default harness uses Immediate outbox — auto-reconcile runs synchronously
    // after apply, so the resource must already be Ready at this point.
    let status = snapshot.basic_status().unwrap();
    assert_eq!(status.phase, ResourcePhase::Ready);
    assert_eq!(status.observed_generation, 1);
    assert!(
        snapshot.last_reconciled_at.is_some(),
        "last_reconciled_at must be populated after auto-reconcile"
    );
    BaseResourceServiceHarness::assert_condition(
        &status,
        ResourceConditionType::Ready,
        ResourceConditionStatus::True,
        Some("Reconciled"),
        None,
    );
    BaseResourceServiceHarness::assert_condition(
        &status,
        ResourceConditionType::Accepted,
        ResourceConditionStatus::True,
        None,
        None,
    );
    BaseResourceServiceHarness::assert_condition(
        &status,
        ResourceConditionType::Reconciling,
        ResourceConditionStatus::False,
        Some("Idle"),
        None,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sanitizer_modifies_spec_on_create() {
    let harness = ResourceUseCaseBaseHarness::new_with_opts(ResourceUseCaseBaseHarnessOpts {
        sanitizer: SanitizerKind::Appending,
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
    let snapshot = harness.get_snapshot_by_id(&result.id).await.unwrap();
    pretty_assertions::assert_eq!(
        snapshot.spec,
        serde_json::json!({ "value": "original-sanitized" })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sanitizer_receives_current_spec_on_update() {
    let harness = ResourceUseCaseBaseHarness::new_with_opts(ResourceUseCaseBaseHarnessOpts {
        sanitizer: SanitizerKind::Appending,
        ..Default::default()
    });
    let account_id = make_account_id();

    let (id, initial_snapshot) = harness
        .apply_and_assert_snapshot(account_id.clone(), "res-a")
        .await;
    // Sanitizer appends "-sanitized" even during initial apply
    pretty_assertions::assert_eq!(
        initial_snapshot.spec,
        serde_json::json!({ "value": "res-a-sanitized" })
    );

    let params = ApplyResourceParams {
        id: Some(id),
        ..make_params(account_id, "res-a", "updated")
    };

    let decision = harness.apply_test_uc().apply(params).await.unwrap();

    assert_eq!(
        decision.expect_applied().outcome,
        ApplyResourceOutcome::Updated
    );

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
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
        sanitizer: SanitizerKind::Appending,
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

    let snapshot = harness.get_snapshot_by_id(&result.id).await.unwrap();
    pretty_assertions::assert_eq!(snapshot.spec, serde_json::json!({ "value": "original" }));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_rejected_when_sanitized_spec_fails_validation() {
    // A sanitizer that clears the value produces an empty string, which fails
    // TestResourceSpec validation → the use case must return Rejected, not panic.
    let harness = ResourceUseCaseBaseHarness::new_with_opts(ResourceUseCaseBaseHarnessOpts {
        sanitizer: SanitizerKind::Clearing,
        ..Default::default()
    });

    let account_id = make_account_id();
    let decision = harness
        .apply_test_uc()
        .apply(make_params(account_id, "res-a", "non-empty"))
        .await
        .unwrap();

    pretty_assertions::assert_matches!(decision, ApplyResourceApplicationDecision::Rejected(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: status assertions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_create_initial_status_is_pending() {
    // With a Dispatching outbox the Applied message is queued but not processed,
    // so auto-reconcile does not run. The resource must be in Pending phase with
    // generation=1 and observed_generation=0 immediately after apply.
    let harness = ResourceUseCaseBaseHarness::new_with_opts(ResourceUseCaseBaseHarnessOpts {
        base_opts: BaseResourceServiceHarnessOpts {
            outbox_provider: OutboxProvider::Dispatching,
        },
        ..Default::default()
    });
    let account_id = make_account_id();

    let result = harness
        .apply_test_uc()
        .apply(make_params(account_id, "res-a", "res-a"))
        .await
        .unwrap()
        .expect_applied();

    let snapshot = harness.get_snapshot_by_id(&result.id).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    assert_eq!(status.phase, ResourcePhase::Pending);
    assert_eq!(snapshot.headers.generation, 1);
    assert_eq!(status.observed_generation, 0);
    assert!(
        snapshot.last_reconciled_at.is_none(),
        "last_reconciled_at must be absent before first reconcile"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_update_resets_status_to_pending() {
    // Verifies that a spec update bumps generation and resets the resource back
    // to Pending, while observed_generation remains at the previously reconciled
    // generation until the next reconcile cycle.
    let harness = ResourceUseCaseBaseHarness::new_with_opts(ResourceUseCaseBaseHarnessOpts {
        base_opts: BaseResourceServiceHarnessOpts {
            outbox_provider: OutboxProvider::Dispatching,
        },
        ..Default::default()
    });
    let account_id = make_account_id();

    // Create + reconcile → gen=1, observed_gen=1, phase=Ready
    let id = harness.apply_and_get_id(account_id.clone(), "res-a").await;
    harness.reconcile_test_uc().execute(&id).await.unwrap();
    harness.flush_outbox().await;

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();
    assert_eq!(status.phase, ResourcePhase::Ready);
    BaseResourceServiceHarness::assert_condition(
        &status,
        ResourceConditionType::Ready,
        ResourceConditionStatus::True,
        Some("Reconciled"),
        None,
    );

    // Spec update → gen bumped to 2, phase reset to Pending
    let update_params = ApplyResourceParams {
        id: Some(id),
        headers: BaseResourceServiceHarness::make_headers_input(account_id, "res-a"),
        spec: TestResourceSpec {
            value: "updated".to_string(),
        },
    };
    harness
        .apply_test_uc()
        .apply(update_params)
        .await
        .unwrap()
        .expect_applied();

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    assert_eq!(status.phase, ResourcePhase::Pending);
    assert_eq!(snapshot.headers.generation, 2);
    // reset_pending_from_spec creates a fresh ResourceStatus, so
    // observed_generation resets to 0 on every spec update —
    // needs_reconciliation() is still true (0 < 2).
    assert_eq!(
        status.observed_generation, 0,
        "observed_generation must be reset to 0 by the spec update"
    );
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
        id: None,
        headers: BaseResourceServiceHarness::make_headers_input(account_id, name),
        spec: TestResourceSpec {
            value: value.to_string(),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
