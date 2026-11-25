// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu_resources::{
    ApplyResourceAction,
    ApplyResourceApplicationDecision,
    ApplyResourceOutcome,
    ApplyResourceParams,
    ApplyResourcePlanningDecision,
    ApplyResourcePrecondition,
    ApplyResourceUseCaseError,
    ResourceName,
    ResourcePhase,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
    TestResource,
    TestResourceSpec,
};
use messaging_outbox::OutboxProvider;

use crate::tests::utils::{ServiceLayerTestHarness, ServiceLayerTestHarnessOpts, SpecPreApplyKind};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// The planner and executor are each tested in isolation in their own test
// modules. Here we only test behaviour that is exclusive to the use-case layer:
// a basic end-to-end wiring smoke test, spec pre-apply integration (all code-paths),
// the boundary where a pre-apply hook produces an invalid spec, and preconditions,
// which span both.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_end_to_end_create_and_retrieve() {
    // Smoke test: verifies planner → executor → persistence wiring is correct.
    // No pre-apply hook; just confirms the resource is created and retrievable.
    let harness = ServiceLayerTestHarness::new();
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    let decision = harness
        .apply_test_uc()
        .apply(make_params(&account_handle, "res-a", "res-a"))
        .await
        .unwrap();

    let result = decision.expect_applied();
    assert_eq!(result.outcome, ApplyResourceOutcome::Created);

    // The controller runtime reconciles the resource after the apply
    harness.settle().await;

    let snapshot = harness.get_snapshot_by_id(&result.id).await.unwrap();
    pretty_assertions::assert_eq!(snapshot.spec, serde_json::json!({ "value": "res-a" }));

    let status = snapshot.basic_status().unwrap();
    assert_eq!(status.phase, ResourcePhase::Ready);
    assert_eq!(status.observed_generation, Some(1));
    assert!(
        status.reconciled_at.is_some(),
        "status.reconciledAt must be populated after auto-reconcile"
    );
    BaseResourceServiceHarness::assert_reconciliation_error(&status, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_spec_pre_apply_modifies_spec_on_create() {
    let harness = ServiceLayerTestHarness::new_with_opts(ServiceLayerTestHarnessOpts {
        spec_pre_apply: SpecPreApplyKind::Appending,
        ..Default::default()
    });
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    let decision = harness
        .apply_test_uc()
        .apply(make_params(&account_handle, "res-a", "original"))
        .await
        .unwrap();

    let result = decision.expect_applied();
    assert_eq!(result.outcome, ApplyResourceOutcome::Created);

    // The hook appends "-prepared"; the persisted spec must reflect that.
    let snapshot = harness.get_snapshot_by_id(&result.id).await.unwrap();
    pretty_assertions::assert_eq!(
        snapshot.spec,
        serde_json::json!({ "value": "original-prepared" })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_spec_pre_apply_modifies_spec_on_update() {
    let harness = ServiceLayerTestHarness::new_with_opts(ServiceLayerTestHarnessOpts {
        spec_pre_apply: SpecPreApplyKind::Appending,
        ..Default::default()
    });
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    let (id, initial_snapshot) = harness
        .apply_and_assert_snapshot(&account_handle, "res-a")
        .await;
    // The hook appends "-prepared" even during initial apply
    pretty_assertions::assert_eq!(
        initial_snapshot.spec,
        serde_json::json!({ "value": "res-a-prepared" })
    );

    let params = ApplyResourceParams {
        id: Some(id),
        ..make_params(&account_handle, "res-a", "updated")
    };

    let decision = harness.apply_test_uc().apply(params).await.unwrap();

    assert_eq!(
        decision.expect_applied().outcome,
        ApplyResourceOutcome::Updated
    );

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    pretty_assertions::assert_eq!(
        snapshot.spec,
        serde_json::json!({ "value": "updated-prepared" })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_spec_pre_apply_modifies_spec_in_plan() {
    // plan() must also run the hook — the returned plan's state must
    // reflect the prepared spec, not the raw input.
    let harness = ServiceLayerTestHarness::new_with_opts(ServiceLayerTestHarnessOpts {
        spec_pre_apply: SpecPreApplyKind::Appending,
        ..Default::default()
    });
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    let decision = harness
        .apply_test_uc()
        .plan(make_params(&account_handle, "res-a", "original"))
        .await
        .unwrap();

    pretty_assertions::assert_matches!(
        decision,
        ApplyResourcePlanningDecision::Planned(ref plan)
            if plan.action == ApplyResourceAction::Create
            && plan.state.spec.value == "original-prepared"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_no_spec_pre_apply_passes_spec_unchanged() {
    let harness = ServiceLayerTestHarness::new();
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    let decision = harness
        .apply_test_uc()
        .apply(make_params(&account_handle, "res-a", "original"))
        .await
        .unwrap();

    let result = decision.expect_applied();

    let snapshot = harness.get_snapshot_by_id(&result.id).await.unwrap();
    pretty_assertions::assert_eq!(snapshot.spec, serde_json::json!({ "value": "original" }));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_rejected_when_prepared_spec_fails_validation() {
    // A hook that clears the value produces an empty string, which fails
    // TestResourceSpec validation → the use case must return Rejected, not panic.
    let harness = ServiceLayerTestHarness::new_with_opts(ServiceLayerTestHarnessOpts {
        spec_pre_apply: SpecPreApplyKind::Clearing,
        ..Default::default()
    });

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    let decision = harness
        .apply_test_uc()
        .apply(make_params(&account_handle, "res-a", "non-empty"))
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
    // generation=1 and observedGeneration absent immediately after apply.
    let harness = ServiceLayerTestHarness::new_with_opts(ServiceLayerTestHarnessOpts {
        base_opts: BaseResourceServiceHarnessOpts {
            outbox_provider: OutboxProvider::Dispatching,
            ..Default::default()
        },
        ..Default::default()
    });
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    let result = harness
        .apply_test_uc()
        .apply(make_params(&account_handle, "res-a", "res-a"))
        .await
        .unwrap()
        .expect_applied();

    let snapshot = harness.get_snapshot_by_id(&result.id).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    assert_eq!(status.phase, ResourcePhase::Pending);
    assert_eq!(snapshot.headers.generation, 1);
    assert_eq!(status.observed_generation, None);
    assert!(
        status.reconciled_at.is_none(),
        "status.reconciledAt must be absent before first reconcile"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_update_resets_status_to_pending() {
    // Verifies that a spec update bumps generation and resets the resource back
    // to Pending, while observed_generation remains at the previously reconciled
    // generation until the next reconcile cycle.
    let harness = ServiceLayerTestHarness::new_with_opts(ServiceLayerTestHarnessOpts {
        base_opts: BaseResourceServiceHarnessOpts {
            outbox_provider: OutboxProvider::Dispatching,
            ..Default::default()
        },
        ..Default::default()
    });
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    // Create + reconcile → gen=1, observed_gen=1, phase=Ready
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;
    harness.settle().await;

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();
    assert_eq!(status.phase, ResourcePhase::Ready);
    BaseResourceServiceHarness::assert_reconciliation_error(&status, None);

    // Spec update → gen bumped to 2, phase reset to Pending
    harness
        .update_spec(&account_handle, id, "res-a", "updated")
        .await;

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    assert_eq!(status.phase, ResourcePhase::Pending);
    assert_eq!(snapshot.headers.generation, 2);
    assert_eq!(
        status.observed_generation,
        Some(1),
        "observedGeneration must keep the last reconciled generation after spec update"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_with_precondition() {
    let harness = ServiceLayerTestHarness::new();
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let apply_uc = harness.apply_test_uc();

    // `Absent` creates a missing resource, and refuses once it exists
    let id = apply_uc
        .apply_with_precondition(
            make_params(&account_handle, "res-a", "v1"),
            ApplyResourcePrecondition::Absent,
        )
        .await
        .unwrap()
        .expect_applied()
        .id;
    assert_matches!(
        apply_uc
            .apply_with_precondition(
                make_params(&account_handle, "res-a", "lost"),
                ApplyResourcePrecondition::Absent,
            )
            .await,
        Err(ApplyResourceUseCaseError::ConcurrentModification(_))
    );

    // `Generation` updates at the current generation only
    apply_uc
        .apply_with_precondition(
            make_params(&account_handle, "res-a", "v2"),
            ApplyResourcePrecondition::Generation(1),
        )
        .await
        .unwrap()
        .expect_applied();
    assert_matches!(
        apply_uc
            .apply_with_precondition(
                make_params(&account_handle, "res-a", "lost"),
                ApplyResourcePrecondition::Generation(1),
            )
            .await,
        Err(ApplyResourceUseCaseError::ConcurrentModification(_))
    );

    // ... and refuses a resource that does not exist
    assert_matches!(
        apply_uc
            .apply_with_precondition(
                make_params(&account_handle, "res-b", "lost"),
                ApplyResourcePrecondition::Generation(1),
            )
            .await,
        Err(ApplyResourceUseCaseError::ConcurrentModification(_))
    );

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    pretty_assertions::assert_eq!(snapshot.spec, serde_json::json!({ "value": "v2" }));
    assert_eq!(snapshot.headers.generation, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_without_name_creates_a_resource_each_time() {
    // With a pre-apply hook, for which the resource to update is looked for too
    let harness = ServiceLayerTestHarness::new_with_opts(ServiceLayerTestHarnessOpts {
        spec_pre_apply: SpecPreApplyKind::Appending,
        ..Default::default()
    });
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    let mut applied = Vec::new();
    for _ in 0..2 {
        let result = harness
            .apply_test_uc()
            .apply(make_nameless_params(&account_handle, "same"))
            .await
            .unwrap()
            .expect_applied();
        assert_eq!(result.outcome, ApplyResourceOutcome::Created);
        assert_generated_name(&result.state.headers.name, "plain-");

        let snapshot = harness.get_snapshot_by_id(&result.id).await.unwrap();
        assert_eq!(snapshot.headers.name, result.state.headers.name);
        applied.push(result);
    }

    assert_ne!(applied[0].id, applied[1].id);
    assert_ne!(applied[0].state.headers.name, applied[1].state.headers.name);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_params(
    account_handle: &odf::ResourceOwnerHandle,
    name: &str,
    value: &str,
) -> ApplyResourceParams<TestResource> {
    ApplyResourceParams {
        id: None,
        headers: BaseResourceServiceHarness::make_headers_input(account_handle.clone(), name)
            .into(),
        spec: TestResourceSpec {
            value: value.to_string(),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_nameless_params(
    account_handle: &odf::ResourceOwnerHandle,
    value: &str,
) -> ApplyResourceParams<TestResource> {
    ApplyResourceParams {
        id: None,
        headers: BaseResourceServiceHarness::make_nameless_headers(account_handle.clone()),
        spec: TestResourceSpec {
            value: value.to_string(),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_generated_name(name: &ResourceName, prefix: &str) {
    let suffix = name
        .as_str()
        .strip_prefix(prefix)
        .unwrap_or_else(|| panic!("'{name}' must start with '{prefix}'"));
    assert_eq!(suffix.len(), 6, "{name}");
    assert!(
        suffix
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
        "{name}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
