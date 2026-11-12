// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use chrono::TimeDelta;
use kamu_resources::{
    DeclarativeResource,
    ManagedResource,
    ResourceAggregateLoader,
    ResourceID,
    ResourceLifecyclePrimitives,
    ResourcePersistenceService,
    ResourcePhase,
    ResourceProcessOutcome,
    ResourceStatusConflictError,
    ResourceVersion,
    SimpleResourceReconciler,
    SimpleResourceReconciliationError,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
};
use kamu_resources_services::{
    SimpleResourceReconciliationHelper,
    SimpleResourceReconciliationOutcome,
};
use messaging_outbox::{MockOutbox, OutboxProvider};

use crate::tests::utils::{
    ServiceLayerTestHarness,
    ServiceLayerTestHarnessOpts,
    TEST_RECONCILIATION_ERROR_CODE_REJECTED,
    TestResource,
    TestResourceReconcilerProvider,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: error cases
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_process_returns_error_for_non_applied_resource() {
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Stub);

    let id = harness.allocate_resource_id().await;
    let result = harness.process(&id).await;

    assert_matches!(result, Err(_), "expected a load failure, got: {result:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: state transitions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_start_phase_transitions_resource_to_reconciling() {
    // The start phase commits the Reconciling transition before
    // the actual reconciliation work happens, giving a stable hand-off point.
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    let resource = harness.start_reconciliation_phase(id).await;

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    assert_eq!(status.phase, ResourcePhase::Reconciling);

    // Finish phase so the event store is left in a consistent state
    harness.finish_reconciliation_phase(resource).await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_success_transitions_resource_to_ready() {
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    let outcome = harness.process(&id).await.unwrap();

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    // The version after the outcome was recorded, so the runtime leaves the
    // resource alone until it changes
    assert_eq!(
        outcome,
        ResourceProcessOutcome::Done(ResourceVersion::from(&snapshot.headers))
    );
    assert_eq!(status.phase, ResourcePhase::Ready);
    assert_eq!(snapshot.headers.generation, 1);
    assert_eq!(status.observed_generation, Some(1));
    assert!(
        status.reconciled_at.is_some(),
        "status.reconciledAt must be set after successful reconcile"
    );

    BaseResourceServiceHarness::assert_reconciliation_error(&status, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_transient_failure_transitions_resource_to_failed_and_is_done() {
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Rejecting);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    // Retrying does not help, so the runtime waits for the resource to change
    let outcome = harness.process(&id).await.unwrap();

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    assert_eq!(
        outcome,
        ResourceProcessOutcome::Done(ResourceVersion::from(&snapshot.headers))
    );
    assert_eq!(status.phase, ResourcePhase::Failed);
    assert_eq!(snapshot.headers.generation, 1);
    assert_eq!(status.observed_generation, Some(1));

    BaseResourceServiceHarness::assert_reconciliation_error(
        &status,
        Some(TEST_RECONCILIATION_ERROR_CODE_REJECTED),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transient_failure_is_recorded_and_returned_as_an_error() {
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Failing);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    // An error, so that the runtime retries with a backoff
    let result = harness.process(&id).await;
    assert_matches!(result, Err(_), "expected an error, got: {result:?}");

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    assert_eq!(status.phase, ResourcePhase::Failed);
    assert_eq!(status.observed_generation, Some(1));
    BaseResourceServiceHarness::assert_reconciliation_error(&status, Some("internal-error"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transient_failure_is_retried_after_backoff_and_then_succeeds() {
    let harness =
        ReconcileTestHarness::with_fake_time(TestResourceReconcilerProvider::FailingReconcileOnce);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    harness.settle().await;

    assert_eq!(
        harness.load(&id).await.status().phase,
        ResourcePhase::Failed
    );
    let state = harness.controller_state(&id).await;
    assert_eq!(state.attempts, 1);
    assert!(state.last_error.is_some(), "the error is kept: {state:?}");
    assert!(state.not_before.is_some(), "backing off: {state:?}");

    // Not due during the backoff
    harness.settle().await;
    assert_eq!(
        harness.load(&id).await.status().phase,
        ResourcePhase::Failed
    );

    harness.advance_time(TimeDelta::minutes(1));
    harness.settle().await;

    let resource = harness.load(&id).await;
    assert_eq!(resource.status().phase, ResourcePhase::Ready);
    BaseResourceServiceHarness::assert_reconciliation_error(resource.status(), None);
    let state = harness.controller_state(&id).await;
    assert_eq!(state.attempts, 0);
    assert_eq!(state.last_error, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_transient_failure_is_not_retried() {
    let harness = ReconcileTestHarness::with_fake_time(TestResourceReconcilerProvider::Rejecting);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    harness.settle().await;
    harness.advance_time(TimeDelta::hours(1));
    harness.settle().await;

    let resource = harness.load(&id).await;
    assert_eq!(resource.status().phase, ResourcePhase::Failed);
    let state = harness.controller_state(&id).await;
    assert_eq!(state.attempts, 0);
    assert_eq!(state.handled_generation, Some(1));
    assert_eq!(
        state.handled_status_generation,
        Some(resource.headers().status_generation)
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

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    harness.process(&id).await.unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_already_reconciled_is_no_op_and_reconciler_is_not_called() {
    // Generation does not change on a no-op: needs_reconciliation() is false
    // because observed_generation == headers.generation. process() returns
    // `Done` without invoking the reconciler.
    let counter = Arc::new(AtomicU32::new(0));
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Counting(
        counter.clone(),
    ));

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    // First process: Pending → reconciler called once → Ready
    harness.process(&id).await.unwrap();

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "reconciler must be called on first reconcile"
    );

    // Second process: already Ready — no-op, no outbox message posted
    harness.process(&id).await.unwrap();

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "reconciler must NOT be called again when resource is already reconciled"
    );

    // State must be unchanged after the no-op
    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    assert_eq!(status.phase, ResourcePhase::Ready);
    assert_eq!(snapshot.headers.generation, 1);
    assert_eq!(status.observed_generation, Some(1));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: outbox messages
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_success_posts_status_updates_to_ready() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);
    BaseResourceServiceHarness::expect_status_updated_message(
        &mut mock_outbox,
        ResourcePhase::Reconciling,
    );
    BaseResourceServiceHarness::expect_status_updated_message(
        &mut mock_outbox,
        ResourcePhase::Ready,
    );

    let harness = ServiceLayerTestHarness::new_with_mock_outbox_and_reconciler(
        mock_outbox,
        TestResourceReconcilerProvider::Stub,
    );

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    harness.process(&id).await.unwrap();
    // MockOutbox verifies expected call counts on drop
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_transient_failure_posts_status_updates_to_failed() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);
    BaseResourceServiceHarness::expect_status_updated_message(
        &mut mock_outbox,
        ResourcePhase::Reconciling,
    );
    BaseResourceServiceHarness::expect_status_updated_message(
        &mut mock_outbox,
        ResourcePhase::Failed,
    );

    let harness = ServiceLayerTestHarness::new_with_mock_outbox_and_reconciler(
        mock_outbox,
        TestResourceReconcilerProvider::Rejecting,
    );

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    harness.process(&id).await.unwrap();
    // MockOutbox verifies expected call counts on drop
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: concurrent modification
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_finish_phase_discards_outcome_of_superseded_generation() {
    // A concurrent apply between the two reconcile phases bumps the generation.
    // The outcome computed for the old generation is dropped, and the
    // runtime reconciles the new generation.
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    let started = harness.start_reconciliation_phase(id).await;

    harness
        .update_spec(&account_handle, id, "res-a", "updated-concurrently")
        .await;

    let outcome = harness.finish_reconciliation_phase(started).await.unwrap();

    assert_matches!(outcome, SimpleResourceReconciliationOutcome::Superseded);
    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();
    assert_eq!(snapshot.headers.generation, 2);
    assert_eq!(status.phase, ResourcePhase::Pending);
    assert_eq!(status.reconciled_generation, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_finish_phase_after_concurrent_status_update_returns_status_conflict() {
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    let started = harness.start_reconciliation_phase(id).await;

    harness.update_status_concurrently(&id).await;

    let result = harness.finish_reconciliation_phase(started).await;

    assert_matches!(
        result,
        Err(SimpleResourceReconciliationError::StatusConflict(
            ResourceStatusConflictError {
                expected: 2,
                actual: 3,
            }
        )),
        "expected StatusConflict, got: {result:?}"
    );
    assert_eq!(
        harness.load(&id).await.status().phase,
        ResourcePhase::Reconciling
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconciler_write_rolls_back_when_outcome_is_not_recorded() {
    // The reconciler's own write commits only together with the outcome:
    // here it updates the status, so recording the outcome fails and the
    // transaction discards both
    let harness =
        ReconcileTestHarness::transactional(TestResourceReconcilerProvider::InterferingReconcile);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    // Processed again in the next pass, which reads the current state
    let outcome = harness.process(&id).await.unwrap();

    assert_eq!(
        outcome,
        ResourceProcessOutcome::RetryAt(harness.time_source().now())
    );
    // The status is as the start of the reconciliation left it: the
    // reconciler's write is gone with the outcome
    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    assert_eq!(snapshot.headers.status_generation, 2);
    assert_eq!(
        snapshot.basic_status().unwrap().phase,
        ResourcePhase::Reconciling
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_overlapping_reconciliations_of_one_generation_record_one_outcome() {
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    let first = harness.start_reconciliation_phase(id).await;
    // Already `Reconciling`, so the second start writes no status
    let second = harness.start_reconciliation_phase(id).await;

    harness.finish_reconciliation_phase(first).await.unwrap();
    let result = harness.finish_reconciliation_phase(second).await;

    assert_matches!(
        result,
        Err(SimpleResourceReconciliationError::StatusConflict(
            ResourceStatusConflictError {
                expected: 2,
                actual: 3,
            }
        )),
        "expected StatusConflict, got: {result:?}"
    );
    let resource = harness.load(&id).await;
    assert_eq!(resource.status().phase, ResourcePhase::Ready);
    assert_eq!(resource.headers().status_generation, 3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_counts_status_updates() {
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;
    assert_eq!(harness.load(&id).await.headers().status_generation, 1);

    harness.process(&id).await.unwrap();

    let resource = harness.load(&id).await;
    assert_eq!(resource.headers().generation, 1);
    assert_eq!(
        resource.headers().status_generation,
        3,
        "one status update when reconciliation starts, one with its outcome"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: deletion interaction
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_process_of_resource_being_deleted_completes_its_deletion_without_reconciling() {
    let counter = Arc::new(AtomicU32::new(0));
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Counting(
        counter.clone(),
    ));

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    harness
        .request_resources_deletion(account_handle.did, vec![id])
        .await;

    harness.process(&id).await.unwrap();

    let resource = harness.load(&id).await;
    assert_eq!(resource.status().phase, ResourcePhase::Deleted);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_finish_phase_discards_outcome_of_resource_being_deleted() {
    let harness = ReconcileTestHarness::dispatching(TestResourceReconcilerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    let started = harness.start_reconciliation_phase(id).await;

    harness
        .request_resources_deletion(account_handle.did, vec![id])
        .await;

    let outcome = harness.finish_reconciliation_phase(started).await.unwrap();

    assert_matches!(outcome, SimpleResourceReconciliationOutcome::Superseded);
    let resource = harness.load(&id).await;
    assert!(resource.headers().deletion_requested_at.is_some());
    assert_eq!(resource.status().phase, ResourcePhase::Deleting);
    assert_eq!(resource.headers().status_generation, 3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Local harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(ServiceLayerTestHarness, base)]
struct ReconcileTestHarness {
    base: ServiceLayerTestHarness,
    loader: Arc<dyn ResourceAggregateLoader<TestResource>>,
    persistence: Arc<dyn ResourcePersistenceService<TestResource>>,
    lifecycle_primitives: Arc<dyn ResourceLifecyclePrimitives<TestResource>>,
    reconciler: Arc<dyn SimpleResourceReconciler<TestResource>>,
}

impl ReconcileTestHarness {
    fn dispatching(reconciler_provider: TestResourceReconcilerProvider) -> Self {
        Self::new(reconciler_provider, false, false)
    }

    /// Writes roll back with the transaction they ran in
    fn transactional(reconciler_provider: TestResourceReconcilerProvider) -> Self {
        Self::new(reconciler_provider, true, false)
    }

    /// Time stands still until `advance_time`, so backoffs end only then
    fn with_fake_time(reconciler_provider: TestResourceReconcilerProvider) -> Self {
        Self::new(reconciler_provider, false, true)
    }

    fn new(
        reconciler_provider: TestResourceReconcilerProvider,
        transactional: bool,
        fake_time: bool,
    ) -> Self {
        let base = ServiceLayerTestHarness::new_with_opts(ServiceLayerTestHarnessOpts {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Dispatching,
                transactional,
                fake_time,
                ..Default::default()
            },
            reconciler_provider,
            ..Default::default()
        });

        let catalog = base.catalog();

        Self {
            loader: catalog.get_one().unwrap(),
            persistence: catalog.get_one().unwrap(),
            lifecycle_primitives: catalog.get_one().unwrap(),
            reconciler: catalog.get_one().unwrap(),
            base,
        }
    }

    async fn load(&self, id: &ResourceID) -> TestResource {
        self.loader.load(id).await.unwrap()
    }

    /// Another status writer updates the status of the resource.
    async fn update_status_concurrently(&self, id: &ResourceID) {
        let mut resource = self.load(id).await;
        let mut new_status = resource.status().clone();
        new_status.observed_at = Some(self.time_source().now() + chrono::Duration::seconds(1));
        let status_generation = resource.headers().status_generation;
        resource
            .try_update_status(self.time_source().now(), status_generation, new_status)
            .unwrap();
        self.persistence.save(&mut resource).await.unwrap();
    }

    /// The first phase of a reconciliation of a resource that needs one,
    /// without the transaction of the controller
    async fn start_reconciliation_phase(&self, id: ResourceID) -> TestResource {
        self.lifecycle_primitives
            .begin_reconciliation(&id, None)
            .await
            .unwrap()
            .expect("resource should need reconciliation")
    }

    /// The second phase of the reconciliation `started`, without the
    /// transaction of the controller
    async fn finish_reconciliation_phase(
        &self,
        started: TestResource,
    ) -> Result<
        SimpleResourceReconciliationOutcome<TestResource>,
        SimpleResourceReconciliationError<TestResource>,
    > {
        SimpleResourceReconciliationHelper::new(
            self.lifecycle_primitives.as_ref(),
            self.reconciler.as_ref(),
        )
        .finish_reconciliation_phase(started)
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
