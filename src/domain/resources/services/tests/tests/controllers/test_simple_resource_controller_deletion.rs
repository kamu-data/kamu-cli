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

use chrono::TimeDelta;
use kamu_resources::{
    DeclarativeResource,
    ResourceAggregateLoader,
    ResourceID,
    ResourcePhase,
    ResourceProcessOutcome,
    ResourceVersion,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
};
use messaging_outbox::{MockOutbox, OutboxProvider};

use crate::tests::utils::{
    ServiceLayerTestHarness,
    ServiceLayerTestHarnessOpts,
    TestResource,
    TestResourceReconcilerProvider,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cleanup_completes_requested_deletion() {
    let harness = CompleteDeletionTestHarness::new(TestResourceReconcilerProvider::Stub);
    let [id] = harness.apply_and_request_deletion(["res-a"]).await;

    let outcome = harness.process(&id).await.unwrap();

    let resource = harness.load(&id).await;
    assert_eq!(
        outcome,
        ResourceProcessOutcome::Done(ResourceVersion::from(resource.headers()))
    );
    assert!(resource.headers().deleted_at.is_some());
    assert_eq!(resource.headers().name.as_str(), format!("deleted-{id}"));
    assert_eq!(resource.status().phase, ResourcePhase::Deleted);
    assert!(harness.get_snapshot_by_id(&id).await.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failed_cleanup_keeps_deletion_in_progress_and_is_returned_as_an_error() {
    let harness = CompleteDeletionTestHarness::new(TestResourceReconcilerProvider::Failing);
    let [id] = harness.apply_and_request_deletion(["res-a"]).await;
    let status_generation_before = harness.load(&id).await.headers().status_generation;

    // Recorded in the status, and an error, so that the runtime retries it
    let result = harness.process(&id).await;
    assert_matches!(result, Err(_), "expected an error, got: {result:?}");

    let resource = harness.load(&id).await;
    assert!(resource.headers().deleted_at.is_none());
    assert_eq!(resource.headers().name.as_str(), "res-a");
    assert_eq!(resource.status().phase, ResourcePhase::Deleting);
    assert_eq!(
        resource.headers().status_generation,
        status_generation_before + 1
    );
    harness.assert_cleanup_failed(&id).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failed_cleanup_is_retried_after_backoff() {
    let harness =
        CompleteDeletionTestHarness::new(TestResourceReconcilerProvider::FailingCleanupOnce);
    let [id] = harness.apply_and_request_deletion(["res-a"]).await;

    harness.settle().await;
    harness.assert_cleanup_failed(&id).await;
    let state = harness.controller_state(&id).await;
    assert_eq!(state.attempts, 1);
    assert!(state.last_error.is_some(), "the error is kept: {state:?}");

    harness.advance_time(TimeDelta::minutes(1));
    harness.settle().await;

    let resource = harness.load(&id).await;
    assert_eq!(resource.status().phase, ResourcePhase::Deleted);
    assert_eq!(resource.headers().name.as_str(), format!("deleted-{id}"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_processing_deleted_resource_records_nothing() {
    let harness = CompleteDeletionTestHarness::new(TestResourceReconcilerProvider::Stub);
    let [id] = harness.apply_and_request_deletion(["res-a"]).await;

    harness.process(&id).await.unwrap();
    let resource = harness.load(&id).await;
    let last_event_id = resource.last_stored_event_id();

    let outcome = harness.process(&id).await.unwrap();

    assert_eq!(
        outcome,
        ResourceProcessOutcome::Done(ResourceVersion::from(resource.headers()))
    );
    assert_eq!(
        harness.load(&id).await.last_stored_event_id(),
        last_event_id
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_each_completed_deletion_posts_its_own_deleted_message() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 2, None);
    BaseResourceServiceHarness::expect_deletion_requested_message(&mut mock_outbox, 2);
    BaseResourceServiceHarness::expect_deleted_messages(&mut mock_outbox, 2, 1);

    let harness = CompleteDeletionTestHarness::with_mock_outbox(mock_outbox);
    let [id_a, id_b] = harness.apply_and_request_deletion(["res-a", "res-b"]).await;

    harness.process(&id_a).await.unwrap();
    harness.process(&id_b).await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failed_cleanup_holds_back_only_its_resource() {
    let harness =
        CompleteDeletionTestHarness::new(TestResourceReconcilerProvider::FailingCleanupOf("res-b"));
    let [id_a, id_b] = harness.apply_and_request_deletion(["res-a", "res-b"]).await;

    harness.settle().await;

    assert_eq!(
        harness.load(&id_a).await.status().phase,
        ResourcePhase::Deleted
    );
    harness.assert_cleanup_failed(&id_b).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_status_update_during_cleanup_does_not_hold_back_completion() {
    let harness =
        CompleteDeletionTestHarness::new(TestResourceReconcilerProvider::InterferingCleanup {
            then_fails: false,
        });
    let [id_a, id_b] = harness.apply_and_request_deletion(["res-a", "res-b"]).await;

    harness.settle().await;

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
async fn test_deletion_completed_by_another_writer_during_cleanup_is_done() {
    let harness =
        CompleteDeletionTestHarness::new(TestResourceReconcilerProvider::CompletingCleanup);
    let [id] = harness.apply_and_request_deletion(["res"]).await;

    let outcome = harness.process(&id).await.unwrap();

    // Done at the version the other writer left, with nothing written on top
    let resource = harness.load(&id).await;
    assert_eq!(resource.status().phase, ResourcePhase::Deleted);
    assert_eq!(
        outcome,
        ResourceProcessOutcome::Done(ResourceVersion::from(resource.headers()))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failed_cleanup_after_concurrent_status_update_is_not_recorded() {
    // The other writer's status wins; the runtime retries the cleanup
    let harness =
        CompleteDeletionTestHarness::new(TestResourceReconcilerProvider::InterferingCleanup {
            then_fails: true,
        });
    let [id] = harness.apply_and_request_deletion(["res"]).await;

    let result = harness.process(&id).await;
    assert_matches!(result, Err(_), "expected an error, got: {result:?}");

    let resource = harness.load(&id).await;
    assert_eq!(resource.status().phase, ResourcePhase::Deleting);
    assert!(resource.status().observed_at.is_some());
    BaseResourceServiceHarness::assert_reconciliation_error(resource.status(), None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Local harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(ServiceLayerTestHarness, base)]
struct CompleteDeletionTestHarness {
    base: ServiceLayerTestHarness,
    loader: Arc<dyn ResourceAggregateLoader<TestResource>>,
}

impl CompleteDeletionTestHarness {
    /// Applies one resource per name, all of one owner, and requests their
    /// deletion
    async fn apply_and_request_deletion<const N: usize>(
        &self,
        names: [&str; N],
    ) -> [ResourceID; N] {
        let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");
        let ids = self.apply_all(&account_handle, &names).await;
        self.request_resources_deletion(account_handle.did, ids.clone())
            .await;
        ids.try_into().unwrap()
    }

    /// Messages wait in the outbox, so each step runs only when a test calls
    /// it. Time stands still until `advance_time`, so backoffs end only then.
    fn new(reconciler_provider: TestResourceReconcilerProvider) -> Self {
        Self::new_with_opts(ServiceLayerTestHarnessOpts {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Dispatching,
                fake_time: true,
                ..Default::default()
            },
            reconciler_provider,
            ..Default::default()
        })
    }

    fn with_mock_outbox(mock_outbox: MockOutbox) -> Self {
        Self::new_with_opts(ServiceLayerTestHarnessOpts {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Mock(mock_outbox),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn new_with_opts(opts: ServiceLayerTestHarnessOpts) -> Self {
        let base = ServiceLayerTestHarness::new_with_opts(opts);

        Self {
            loader: base.catalog().get_one().unwrap(),
            base,
        }
    }

    async fn load(&self, id: &ResourceID) -> TestResource {
        self.loader.load(id).await.unwrap()
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
