// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use kamu_resources::{
    ApplyResourceParams,
    ReconcileResourceUseCaseError,
    Reconciler,
    ResourceAggregateLoader,
    ResourceConditionStatus,
    ResourceConditionType,
    ResourcePersistenceService,
    ResourcePhase,
};
use kamu_resources_services::ReconcileResourceUseCaseHelper;
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
};
use messaging_outbox::{MockOutbox, OutboxProvider};

use super::resource_use_case_base_harness::{
    ResourceUseCaseBaseHarness,
    ResourceUseCaseBaseHarnessOpts,
    SanitizerKind,
};
use crate::tests::utils::{TestResource, TestResourceReconcilerProvider, make_account_id};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: error cases
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_returns_error_for_non_applied_resource() {
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Stub);

    let uid = harness.allocate_resource_uid().await;
    let result = harness.reconcile_test_uc().execute(&uid).await;

    assert!(
        matches!(result, Err(ReconcileResourceUseCaseError::LoadFailed(_))),
        "expected LoadFailed for a non-applied resource, got: {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: state transitions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_start_phase_transitions_resource_to_reconciling() {
    // start_reconciliation_phase commits the Reconciling transition before
    // the actual reconciler work happens, giving a stable hand-off point.
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Stub);

    let account_id = make_account_id();
    let uid = harness.apply_and_get_uid(account_id, "res-a").await;

    let helper = harness.make_reconcile_helper();

    let resource = helper
        .start_reconciliation_phase(uid)
        .await
        .unwrap()
        .expect("resource should need reconciliation after apply");

    let snapshot = harness.get_snapshot_by_uid(&uid).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    assert_eq!(status.phase, ResourcePhase::Reconciling);
    BaseResourceServiceHarness::assert_condition(
        &status,
        ResourceConditionType::Reconciling,
        ResourceConditionStatus::True,
        Some("Processing"),
        None,
    );

    // Finish phase so the event store is left in a consistent state
    helper.finish_reconciliation_phase(resource).await.unwrap();
    harness.flush_outbox().await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_success_transitions_resource_to_ready() {
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Stub);

    let account_id = make_account_id();
    let uid = harness.apply_and_get_uid(account_id, "res-a").await;

    harness.reconcile_test_uc().execute(&uid).await.unwrap();
    harness.flush_outbox().await;

    let snapshot = harness.get_snapshot_by_uid(&uid).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    assert_eq!(status.phase, ResourcePhase::Ready);
    assert_eq!(snapshot.headers.generation, 1);
    assert_eq!(status.observed_generation, 1);
    assert!(
        snapshot.last_reconciled_at.is_some(),
        "last_reconciled_at must be set after successful reconcile"
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
async fn test_reconcile_failure_transitions_resource_to_failed() {
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Failing);

    let account_id = make_account_id();
    let uid = harness.apply_and_get_uid(account_id, "res-a").await;

    // Reconciler errors are outcomes, not use-case failures — execute returns
    // Ok(())
    harness.reconcile_test_uc().execute(&uid).await.unwrap();
    harness.flush_outbox().await;

    let snapshot = harness.get_snapshot_by_uid(&uid).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    assert_eq!(status.phase, ResourcePhase::Failed);
    assert_eq!(snapshot.headers.generation, 1);
    assert_eq!(status.observed_generation, 1);

    BaseResourceServiceHarness::assert_condition(
        &status,
        ResourceConditionType::Ready,
        ResourceConditionStatus::False,
        Some("internal_error"),
        Some(true), // must carry a message describing the failure
    );
    BaseResourceServiceHarness::assert_condition(
        &status,
        ResourceConditionType::Reconciling,
        ResourceConditionStatus::False,
        None,
        None,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: reconciler invocation count
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_invokes_reconciler_exactly_once() {
    let counter = Arc::new(AtomicU32::new(0));
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Counting(
        counter.clone(),
    ));

    let account_id = make_account_id();
    let uid = harness.apply_and_get_uid(account_id, "res-a").await;

    harness.reconcile_test_uc().execute(&uid).await.unwrap();
    harness.flush_outbox().await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_already_reconciled_is_no_op_and_reconciler_is_not_called() {
    // Generation does not change on a no-op: needs_reconciliation() is false
    // because observed_generation == headers.generation. execute() returns
    // Ok(()) immediately without invoking the reconciler.
    let counter = Arc::new(AtomicU32::new(0));
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Counting(
        counter.clone(),
    ));

    let account_id = make_account_id();
    let uid = harness.apply_and_get_uid(account_id, "res-a").await;

    // First execute: Pending → reconciler called once → Ready
    harness.reconcile_test_uc().execute(&uid).await.unwrap();
    harness.flush_outbox().await;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "reconciler must be called on first execute"
    );

    // Second execute: already Ready — no-op, no outbox message posted
    harness.reconcile_test_uc().execute(&uid).await.unwrap();
    harness.flush_outbox().await;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "reconciler must NOT be called again when resource is already reconciled"
    );

    // State must be unchanged after the no-op
    let snapshot = harness.get_snapshot_by_uid(&uid).await.unwrap();
    let status = snapshot.basic_status().unwrap();
    assert_eq!(status.phase, ResourcePhase::Ready);
    assert_eq!(snapshot.headers.generation, 1);
    assert_eq!(status.observed_generation, 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: outbox messages
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_success_posts_reconciliation_succeeded_message() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);
    BaseResourceServiceHarness::expect_reconciliation_succeeded_message(&mut mock_outbox, 1);

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox_and_reconciler(
        mock_outbox,
        TestResourceReconcilerProvider::Stub,
    );

    let account_id = make_account_id();
    let uid = harness.apply_and_get_uid(account_id, "res-a").await;

    harness.reconcile_test_uc().execute(&uid).await.unwrap();
    // MockOutbox verifies expected call counts on drop
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_failure_posts_reconciliation_failed_message() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);
    BaseResourceServiceHarness::expect_reconciliation_failed_message(&mut mock_outbox, 1);

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox_and_reconciler(
        mock_outbox,
        TestResourceReconcilerProvider::Failing,
    );

    let account_id = make_account_id();
    let uid = harness.apply_and_get_uid(account_id, "res-a").await;

    harness.reconcile_test_uc().execute(&uid).await.unwrap();
    // MockOutbox verifies expected call counts on drop
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: concurrent modification
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_finish_phase_after_concurrent_spec_update_returns_error() {
    // Simulates a concurrent apply happening between the two reconcile phases.
    // The event store detects the out-of-date prev_stored_event_id and returns
    // ConcurrentModificationError from finish_reconciliation_phase.
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Stub);

    let account_id = make_account_id();
    let uid = harness.apply_and_get_uid(account_id.clone(), "res-a").await;

    let helper = harness.make_reconcile_helper();

    // Phase 1: commit ReconciliationStarted; capture the aggregate at that version
    let resource_before_concurrent_update = helper
        .start_reconciliation_phase(uid)
        .await
        .unwrap()
        .expect("resource should need reconciliation");

    // Concurrent apply: adds a SpecUpdated event and bumps generation to 2
    let update_params = ApplyResourceParams {
        uid: Some(uid),
        headers: BaseResourceServiceHarness::make_headers_input(account_id, "res-a"),
        spec: crate::tests::utils::TestResourceSpec {
            value: "updated-concurrently".to_string(),
        },
    };
    harness
        .apply_test_uc()
        .apply(update_params)
        .await
        .unwrap()
        .expect_applied();

    // Phase 2 with the stale aggregate: must fail
    let result = helper
        .finish_reconciliation_phase(resource_before_concurrent_update)
        .await;

    assert!(
        matches!(
            result,
            Err(ReconcileResourceUseCaseError::ConcurrentModification(_))
        ),
        "expected ConcurrentModification error, got: {result:?}"
    );

    // The concurrent apply wins: generation=2, phase=Pending
    let snapshot = harness.get_snapshot_by_uid(&uid).await.unwrap();
    let status = snapshot.basic_status().unwrap();
    assert_eq!(snapshot.headers.generation, 2);
    assert_eq!(status.phase, ResourcePhase::Pending);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: deletion interaction
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_after_delete_returns_ok_but_snapshot_is_gone() {
    // Documents current behavior: the aggregate loader still finds the
    // soft-deleted resource in the event store, but the aggregate projection
    // rejects the ReconciliationStarted event on a deleted resource with an
    // InvariantViolation lifecycle error.
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Stub);

    let account_id = make_account_id();
    let uid = harness.apply_and_get_uid(account_id.clone(), "res-a").await;

    harness.delete_resources(account_id, vec![uid]).await;

    assert!(
        harness.get_snapshot_by_uid(&uid).await.is_none(),
        "snapshot must be absent immediately after delete"
    );

    let result = harness.reconcile_test_uc().execute(&uid).await;
    assert!(
        matches!(result, Err(ReconcileResourceUseCaseError::Lifecycle(_))),
        "expected Lifecycle(InvariantViolation) for a deleted resource, got: {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Local harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(ResourceUseCaseBaseHarness, base)]
struct ReconcileTestHarness {
    base: ResourceUseCaseBaseHarness,
    loader: Arc<dyn ResourceAggregateLoader<TestResource>>,
    persistence: Arc<dyn ResourcePersistenceService<TestResource>>,
    reconciler: Arc<dyn Reconciler<TestResource>>,
}

impl ReconcileTestHarness {
    fn dispatching(reconciler_provider: TestResourceReconcilerProvider) -> Self {
        let base = ResourceUseCaseBaseHarness::new_with_opts(ResourceUseCaseBaseHarnessOpts {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Dispatching,
            },
            reconciler_provider,
            sanitizer: SanitizerKind::None,
        });

        let catalog = base.catalog();

        Self {
            loader: catalog.get_one().unwrap(),
            persistence: catalog.get_one().unwrap(),
            reconciler: catalog.get_one().unwrap(),
            base,
        }
    }

    fn make_reconcile_helper(&self) -> ReconcileResourceUseCaseHelper<'_, TestResource> {
        ReconcileResourceUseCaseHelper::new(
            self.loader.as_ref(),
            self.persistence.as_ref(),
            self.outbox(),
            self.reconciler.as_ref(),
            self.time_source(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
