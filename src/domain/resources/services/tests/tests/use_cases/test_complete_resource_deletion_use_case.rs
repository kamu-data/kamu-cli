// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use event_sourcing::ConcurrentModificationError;
use kamu_resources::{
    CompleteResourceDeletionUseCaseError,
    DeclarativeResource,
    ResourceAggregateLoader,
    ResourceID,
    ResourcePhase,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
};
use kamu_resources_services::{
    CompleteResourceDeletionPhases,
    ResourceCleanupAttempt,
    ResourceCleanupTransactionError,
};
use messaging_outbox::{MockOutbox, OutboxProvider};

use super::resource_use_case_base_harness::{
    ResourceUseCaseBaseHarness,
    ResourceUseCaseBaseHarnessOpts,
    SanitizerKind,
};
use crate::tests::utils::{TestResource, TestResourceControllerProvider};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cleanup_completes_requested_deletion() {
    let harness = CompleteDeletionTestHarness::dispatching(TestResourceControllerProvider::Stub);
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;
    harness
        .request_resources_deletion(account_handle.did, vec![id])
        .await;

    harness.complete_deletion(&[id]).await;

    let resource = harness.load(&id).await;
    assert!(resource.headers().deleted_at.is_some());
    assert_eq!(resource.headers().name.as_str(), format!("deleted-{id}"));
    assert_eq!(resource.status().phase, ResourcePhase::Deleted);
    assert!(harness.get_snapshot_by_id(&id).await.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_completed_deletion_posts_deleted_message() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);
    BaseResourceServiceHarness::expect_deletion_requested_message(&mut mock_outbox, 1);
    BaseResourceServiceHarness::expect_deleted_message(&mut mock_outbox, 1);

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox(mock_outbox);
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;
    harness
        .request_resources_deletion(account_handle.did, vec![id])
        .await;

    harness.complete_deletion(&[id]).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failed_cleanup_keeps_deletion_in_progress() {
    let harness = CompleteDeletionTestHarness::dispatching(TestResourceControllerProvider::Failing);
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;
    harness
        .request_resources_deletion(account_handle.did, vec![id])
        .await;
    let status_generation_before = harness.load(&id).await.headers().status_generation;

    // A failed cleanup is an outcome recorded in the status, not an error
    harness.complete_deletion(&[id]).await;

    let resource = harness.load(&id).await;
    assert!(resource.headers().deleted_at.is_none());
    assert_eq!(resource.headers().name.as_str(), "res-a");
    assert_eq!(resource.status().phase, ResourcePhase::Deleting);
    assert_eq!(
        resource.headers().status_generation,
        status_generation_before + 1
    );
    BaseResourceServiceHarness::assert_reconciliation_error(
        resource.status(),
        Some("cleanup-failed"),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_repeated_deletion_request_retries_failed_cleanup() {
    let harness = CompleteDeletionTestHarness::dispatching(
        TestResourceControllerProvider::FailingCleanupOnce,
    );
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;
    harness
        .request_resources_deletion(account_handle.did.clone(), vec![id])
        .await;
    harness.complete_deletion(&[id]).await;
    harness.assert_cleanup_failed(&id).await;

    harness
        .request_resources_deletion(account_handle.did, vec![id])
        .await;
    harness.complete_deletion(&[id]).await;

    let resource = harness.load(&id).await;
    assert_eq!(resource.status().phase, ResourcePhase::Deleted);
    assert_eq!(resource.headers().name.as_str(), format!("deleted-{id}"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_completing_unrequested_deletion_is_no_op() {
    let harness = CompleteDeletionTestHarness::dispatching(TestResourceControllerProvider::Stub);
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;
    let last_event_id_before = harness.load(&id).await.last_stored_event_id();

    harness.complete_deletion(&[id]).await;

    let resource = harness.load(&id).await;
    assert!(resource.headers().deletion_requested_at.is_none());
    assert_eq!(resource.last_stored_event_id(), last_event_id_before);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_completing_deletion_twice_records_it_once() {
    let harness = CompleteDeletionTestHarness::dispatching(TestResourceControllerProvider::Stub);
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res-a").await;
    harness
        .request_resources_deletion(account_handle.did, vec![id])
        .await;

    harness.complete_deletion(&[id]).await;
    let last_event_id = harness.load(&id).await.last_stored_event_id();

    harness.complete_deletion(&[id]).await;

    assert_eq!(
        harness.load(&id).await.last_stored_event_id(),
        last_event_id
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_batch_posts_one_deleted_message() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 2, None);
    BaseResourceServiceHarness::expect_deletion_requested_message(&mut mock_outbox, 2);
    BaseResourceServiceHarness::expect_deleted_message(&mut mock_outbox, 2);

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox(mock_outbox);
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id_a = harness.apply_and_get_id(&account_handle, "res-a").await;
    let id_b = harness.apply_and_get_id(&account_handle, "res-b").await;
    harness
        .request_resources_deletion(account_handle.did, vec![id_a, id_b])
        .await;

    harness.complete_deletion(&[id_a, id_b]).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failed_cleanup_holds_back_only_its_resource() {
    let harness = CompleteDeletionTestHarness::dispatching(
        TestResourceControllerProvider::FailingCleanupOf("res-b"),
    );
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id_a = harness.apply_and_get_id(&account_handle, "res-a").await;
    let id_b = harness.apply_and_get_id(&account_handle, "res-b").await;
    harness
        .request_resources_deletion(account_handle.did, vec![id_a, id_b])
        .await;

    harness.complete_deletion(&[id_a, id_b]).await;

    assert_eq!(
        harness.load(&id_a).await.status().phase,
        ResourcePhase::Deleted
    );
    assert_eq!(
        harness.load(&id_b).await.status().phase,
        ResourcePhase::Deleting
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_status_update_during_cleanup_does_not_hold_back_completion() {
    let harness = CompleteDeletionTestHarness::dispatching(
        TestResourceControllerProvider::InterferingCleanup { then_fails: false },
    );
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id_a = harness.apply_and_get_id(&account_handle, "res-a").await;
    let id_b = harness.apply_and_get_id(&account_handle, "res-b").await;
    harness
        .request_resources_deletion(account_handle.did, vec![id_a, id_b])
        .await;

    harness.complete_deletion(&[id_a, id_b]).await;

    assert_eq!(
        harness.load(&id_a).await.status().phase,
        ResourcePhase::Deleted
    );
    assert_eq!(
        harness.load(&id_b).await.status().phase,
        ResourcePhase::Deleted
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failed_cleanup_after_concurrent_status_update_is_not_recorded() {
    // The other writer's status wins; a repeated deletion request retries
    let harness = CompleteDeletionTestHarness::dispatching(
        TestResourceControllerProvider::InterferingCleanup { then_fails: true },
    );
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
    let id = harness.apply_and_get_id(&account_handle, "res").await;
    harness
        .request_resources_deletion(account_handle.did, vec![id])
        .await;

    harness.complete_deletion(&[id]).await;

    let resource = harness.load(&id).await;
    assert_eq!(resource.status().phase, ResourcePhase::Deleting);
    assert!(resource.status().observed_at.is_some());
    harness.assert_cleanup_not_failed(&id).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_concurrent_modification_of_batch_records_outcomes_one_by_one() {
    let id_a = ResourceID::new(uuid::Uuid::new_v4());
    let id_b = ResourceID::new(uuid::Uuid::new_v4());
    let recorder = OutcomeRecorder::conflicting_on(vec![vec![id_a, id_b]]);

    recorder.record_in_batch(&[id_a, id_b]).await.unwrap();

    assert_eq!(
        recorder.recorded_batches(),
        vec![vec![id_a, id_b], vec![id_a], vec![id_b]]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_concurrent_modification_of_one_outcome_holds_back_only_its_resource() {
    let id_a = ResourceID::new(uuid::Uuid::new_v4());
    let id_b = ResourceID::new(uuid::Uuid::new_v4());
    let recorder = OutcomeRecorder::conflicting_on(vec![vec![id_a, id_b], vec![id_a]]);

    recorder.record_in_batch(&[id_a, id_b]).await.unwrap();

    assert_eq!(
        recorder.recorded_batches(),
        vec![vec![id_a, id_b], vec![id_a], vec![id_b]]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Local harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(ResourceUseCaseBaseHarness, base)]
struct CompleteDeletionTestHarness {
    base: ResourceUseCaseBaseHarness,
    loader: Arc<dyn ResourceAggregateLoader<TestResource>>,
}

impl CompleteDeletionTestHarness {
    /// Messages wait in the outbox, so each step runs only when a test calls it.
    fn dispatching(controller_provider: TestResourceControllerProvider) -> Self {
        let base = ResourceUseCaseBaseHarness::new_with_opts(ResourceUseCaseBaseHarnessOpts {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Dispatching,
            },
            controller_provider,
            sanitizer: SanitizerKind::None,
        });

        Self {
            loader: base.catalog().get_one().unwrap(),
            base,
        }
    }

    async fn load(&self, id: &ResourceID) -> TestResource {
        self.loader.load(id).await.unwrap()
    }

    async fn assert_cleanup_not_failed(&self, id: &ResourceID) {
        let resource = self.load(id).await;
        BaseResourceServiceHarness::assert_reconciliation_error(resource.status(), None);
    }

    async fn assert_cleanup_failed(&self, id: &ResourceID) {
        let resource = self.load(id).await;
        assert_eq!(resource.status().phase, ResourcePhase::Deleting);
        BaseResourceServiceHarness::assert_reconciliation_error(
            resource.status(),
            Some("cleanup-failed"),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Records batches of cleanup outcomes, failing the listed ones with a
/// concurrent modification
struct OutcomeRecorder {
    conflicting: Vec<Vec<ResourceID>>,
    recorded: Mutex<Vec<Vec<ResourceID>>>,
}

impl OutcomeRecorder {
    fn conflicting_on(conflicting: Vec<Vec<ResourceID>>) -> Self {
        Self {
            conflicting,
            recorded: Mutex::new(Vec::new()),
        }
    }

    async fn record_in_batch(
        &self,
        ids: &[ResourceID],
    ) -> Result<(), CompleteResourceDeletionUseCaseError<TestResource>> {
        let attempts = ids
            .iter()
            .map(|&id| ResourceCleanupAttempt {
                id,
                status_generation: 1,
                failure: None,
            })
            .collect();

        self.record_outcomes_in_batch(attempts).await
    }

    fn recorded_batches(&self) -> Vec<Vec<ResourceID>> {
        self.recorded.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl CompleteResourceDeletionPhases<TestResource> for OutcomeRecorder {
    async fn run_cleanup(
        &self,
        _id: ResourceID,
    ) -> Result<Option<ResourceCleanupAttempt>, ResourceCleanupTransactionError<TestResource>> {
        Ok(None)
    }

    async fn record_outcomes(
        &self,
        attempts: Vec<ResourceCleanupAttempt>,
    ) -> Result<(), CompleteResourceDeletionUseCaseError<TestResource>> {
        let ids = attempts
            .iter()
            .map(|attempt| attempt.id)
            .collect::<Vec<_>>();
        let conflicts = self.conflicting.contains(&ids);
        self.recorded.lock().unwrap().push(ids);

        if conflicts {
            Err(
                CompleteResourceDeletionUseCaseError::ConcurrentModification(
                    ConcurrentModificationError {},
                ),
            )
        } else {
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
