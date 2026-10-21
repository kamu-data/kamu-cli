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

use kamu_resources::{
    ApplyResourceParams,
    DeclarativeResource,
    ManagedResource,
    ReconcileResourceUseCaseError,
    ResourceAggregateLoader,
    ResourceController,
    ResourceID,
    ResourcePersistenceService,
    ResourcePhase,
    ResourceStatusConflictError,
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
use crate::tests::utils::{TestResource, TestResourceControllerProvider};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: error cases
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_returns_error_for_non_applied_resource() {
    let harness = ReconcileTestHarness::dispatching(TestResourceControllerProvider::Stub);

    let id = harness.allocate_resource_id().await;
    let result = harness.reconcile_test_uc().execute(&id).await;

    assert_matches!(
        result,
        Err(ReconcileResourceUseCaseError::LoadFailed(_)),
        "expected LoadFailed for a non-applied resource, got: {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: state transitions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_start_phase_transitions_resource_to_reconciling() {
    // start_reconciliation_phase commits the Reconciling transition before
    // the actual controller work happens, giving a stable hand-off point.
    let harness = ReconcileTestHarness::dispatching(TestResourceControllerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    let helper = harness.make_reconcile_helper();

    let resource = helper
        .start_reconciliation_phase(id)
        .await
        .unwrap()
        .expect("resource should need reconciliation after apply");

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    assert_eq!(status.phase, ResourcePhase::Reconciling);

    // Finish phase so the event store is left in a consistent state
    helper.finish_reconciliation_phase(resource).await.unwrap();
    harness.flush_outbox().await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_success_transitions_resource_to_ready() {
    let harness = ReconcileTestHarness::dispatching(TestResourceControllerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    harness.reconcile_test_uc().execute(&id).await.unwrap();
    harness.flush_outbox().await;

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();

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
async fn test_reconcile_failure_transitions_resource_to_failed() {
    let harness = ReconcileTestHarness::dispatching(TestResourceControllerProvider::Failing);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    // Controller errors are outcomes, not use-case failures — execute returns
    // Ok(())
    harness.reconcile_test_uc().execute(&id).await.unwrap();
    harness.flush_outbox().await;

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();

    assert_eq!(status.phase, ResourcePhase::Failed);
    assert_eq!(snapshot.headers.generation, 1);
    assert_eq!(status.observed_generation, Some(1));

    BaseResourceServiceHarness::assert_reconciliation_error(&status, Some("internal-error"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: controller invocation count
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_invokes_controller_exactly_once() {
    let counter = Arc::new(AtomicU32::new(0));
    let harness = ReconcileTestHarness::dispatching(TestResourceControllerProvider::Counting(
        counter.clone(),
    ));

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    harness.reconcile_test_uc().execute(&id).await.unwrap();
    harness.flush_outbox().await;

    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_already_reconciled_is_no_op_and_controller_is_not_called() {
    // Generation does not change on a no-op: needs_reconciliation() is false
    // because observed_generation == headers.generation. execute() returns
    // Ok(()) immediately without invoking the controller.
    let counter = Arc::new(AtomicU32::new(0));
    let harness = ReconcileTestHarness::dispatching(TestResourceControllerProvider::Counting(
        counter.clone(),
    ));

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    // First execute: Pending → controller called once → Ready
    harness.reconcile_test_uc().execute(&id).await.unwrap();
    harness.flush_outbox().await;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "controller must be called on first execute"
    );

    // Second execute: already Ready — no-op, no outbox message posted
    harness.reconcile_test_uc().execute(&id).await.unwrap();
    harness.flush_outbox().await;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "controller must NOT be called again when resource is already reconciled"
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
async fn test_reconcile_success_posts_reconciliation_succeeded_message() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);
    BaseResourceServiceHarness::expect_reconciliation_succeeded_message(&mut mock_outbox, 1);

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox_and_controller(
        mock_outbox,
        TestResourceControllerProvider::Stub,
    );

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    harness.reconcile_test_uc().execute(&id).await.unwrap();
    // MockOutbox verifies expected call counts on drop
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_failure_posts_reconciliation_failed_message() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);
    BaseResourceServiceHarness::expect_reconciliation_failed_message(&mut mock_outbox, 1);

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox_and_controller(
        mock_outbox,
        TestResourceControllerProvider::Failing,
    );

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    harness.reconcile_test_uc().execute(&id).await.unwrap();
    // MockOutbox verifies expected call counts on drop
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests: concurrent modification
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_finish_phase_discards_outcome_of_superseded_generation() {
    // A concurrent apply between the two reconcile phases bumps the generation.
    // The outcome computed for the old generation is dropped, and the
    // `Applied` message of the new generation reconciles it again.
    let harness = ReconcileTestHarness::dispatching(TestResourceControllerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    let helper = harness.make_reconcile_helper();

    let started = helper
        .start_reconciliation_phase(id)
        .await
        .unwrap()
        .expect("resource should need reconciliation");

    let update_params = ApplyResourceParams {
        id: Some(id),
        headers: BaseResourceServiceHarness::make_headers_input(account_handle, "res-a"),
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

    helper.finish_reconciliation_phase(started).await.unwrap();

    let snapshot = harness.get_snapshot_by_id(&id).await.unwrap();
    let status = snapshot.basic_status().unwrap();
    assert_eq!(snapshot.headers.generation, 2);
    assert_eq!(status.phase, ResourcePhase::Pending);
    assert_eq!(status.reconciled_generation, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_finish_phase_after_concurrent_status_update_returns_status_conflict() {
    let harness = ReconcileTestHarness::dispatching(TestResourceControllerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    let helper = harness.make_reconcile_helper();

    let started = helper
        .start_reconciliation_phase(id)
        .await
        .unwrap()
        .expect("resource should need reconciliation");

    harness.update_status_concurrently(&id).await;

    let result = helper.finish_reconciliation_phase(started).await;

    assert_matches!(
        result,
        Err(ReconcileResourceUseCaseError::StatusConflict(
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
async fn test_overlapping_reconciliations_of_one_generation_record_one_outcome() {
    let harness = ReconcileTestHarness::dispatching(TestResourceControllerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    let helper = harness.make_reconcile_helper();

    let first = helper
        .start_reconciliation_phase(id)
        .await
        .unwrap()
        .expect("resource should need reconciliation");
    // Already `Reconciling`, so the second start writes no status
    let second = helper
        .start_reconciliation_phase(id)
        .await
        .unwrap()
        .expect("resource should still need reconciliation");

    helper.finish_reconciliation_phase(first).await.unwrap();
    let result = helper.finish_reconciliation_phase(second).await;

    assert_matches!(
        result,
        Err(ReconcileResourceUseCaseError::StatusConflict(
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
    let harness = ReconcileTestHarness::dispatching(TestResourceControllerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;
    assert_eq!(harness.load(&id).await.headers().status_generation, 1);

    harness.reconcile_test_uc().execute(&id).await.unwrap();

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
async fn test_reconcile_of_resource_being_deleted_is_no_op() {
    let harness = ReconcileTestHarness::dispatching(TestResourceControllerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    harness
        .request_resources_deletion(account_handle.did, vec![id])
        .await;

    harness.reconcile_test_uc().execute(&id).await.unwrap();

    let resource = harness.load(&id).await;
    assert_eq!(resource.status().phase, ResourcePhase::Deleting);
    assert_eq!(resource.headers().status_generation, 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_finish_phase_discards_outcome_of_resource_being_deleted() {
    let harness = ReconcileTestHarness::dispatching(TestResourceControllerProvider::Stub);

    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    let helper = harness.make_reconcile_helper();

    let started = helper
        .start_reconciliation_phase(id)
        .await
        .unwrap()
        .expect("resource should need reconciliation");

    harness
        .request_resources_deletion(account_handle.did, vec![id])
        .await;

    helper.finish_reconciliation_phase(started).await.unwrap();

    let resource = harness.load(&id).await;
    assert!(resource.headers().deletion_requested_at.is_some());
    assert_eq!(resource.status().phase, ResourcePhase::Deleting);
    assert_eq!(resource.headers().status_generation, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Local harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(ResourceUseCaseBaseHarness, base)]
struct ReconcileTestHarness {
    base: ResourceUseCaseBaseHarness,
    loader: Arc<dyn ResourceAggregateLoader<TestResource>>,
    persistence: Arc<dyn ResourcePersistenceService<TestResource>>,
    controller: Arc<dyn ResourceController<TestResource>>,
}

impl ReconcileTestHarness {
    fn dispatching(controller_provider: TestResourceControllerProvider) -> Self {
        let base = ResourceUseCaseBaseHarness::new_with_opts(ResourceUseCaseBaseHarnessOpts {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Dispatching,
            },
            controller_provider,
            sanitizer: SanitizerKind::None,
        });

        let catalog = base.catalog();

        Self {
            loader: catalog.get_one().unwrap(),
            persistence: catalog.get_one().unwrap(),
            controller: catalog.get_one().unwrap(),
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

    fn make_reconcile_helper(&self) -> ReconcileResourceUseCaseHelper<'_, TestResource> {
        ReconcileResourceUseCaseHelper::new(
            self.loader.as_ref(),
            self.persistence.as_ref(),
            self.outbox(),
            self.controller.as_ref(),
            self.time_source(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
