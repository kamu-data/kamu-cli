// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use background_agent_host::{
    BackgroundWorkFinalizer,
    HousekeepingBatchReport,
    HousekeepingConfig,
    HousekeepingJob,
    HousekeepingMetrics,
};
use chrono::{DateTime, TimeDelta, Utc};
use database_common::testing::in_committed_transaction;
use dill::{Catalog, CatalogBuilder};
use event_sourcing::GetEventsOpts;
use futures::TryStreamExt;
use internal_error::InternalError;
use kamu_resources::{
    DeclarativeResource,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceAggregateLoader,
    ResourceDeletionCompletion,
    ResourceID,
    ResourceLifecycleMessage,
    ResourceLifecyclePrimitives,
    ResourceRawEventQuery,
    ResourceRawEventStore,
    ResourceRemovalListener,
    ResourceRepository,
    ResourceRetentionConfig,
    ResourceRetentionPolicy,
    ResourceSchemaProvider,
    resource_type_name,
};
use kamu_resources_services::ResourceRetentionJob;
use kamu_resources_services::testing::{BaseResourceServiceHarnessOpts, TestResource};
use messaging_outbox::{OutboxMessageBoundary, OutboxMessageBridge, OutboxProvider};

use crate::tests::utils::{ServiceLayerTestHarness, ServiceLayerTestHarnessOpts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_completed_deletion_is_purged_after_the_type_retention() {
    use ResourceRetentionPolicy::{PurgeImmediately, RetainFor, RetainForever};

    let days = TimeDelta::days;
    for (default, override_of_type, retention) in [
        (RetainFor(days(7)), None, Some(days(7))),
        (RetainForever, Some(RetainFor(days(30))), Some(days(30))),
        (
            RetainFor(days(7)),
            Some(PurgeImmediately),
            Some(TimeDelta::zero()),
        ),
        (RetainFor(days(7)), Some(RetainForever), None),
        (RetainForever, None, None),
    ] {
        let harness = RetentionHarness::new(retention_config(default, override_of_type)).await;
        let id = harness.apply_and_request_deletion("res").await;

        harness.complete_deletion(harness.catalog(), id).await;

        let deleted_at = harness.deleted_at(&id).await;
        pretty_assertions::assert_eq!(
            harness.purge_at(&id).await,
            retention.map(|retention| deleted_at + retention),
            "default={default:?}, override={override_of_type:?}"
        );
    }
}

#[test_log::test(tokio::test)]
async fn test_resource_being_deleted_has_no_purge_deadline() {
    let harness = RetentionHarness::new(ResourceRetentionConfig {
        default: ResourceRetentionPolicy::PurgeImmediately,
        types: BTreeMap::new(),
    })
    .await;

    let id = harness.apply_and_request_deletion("res").await;

    pretty_assertions::assert_eq!(harness.purge_at(&id).await, None);
}

#[test_log::test(tokio::test)]
async fn test_policy_change_applies_to_later_deletions_only() {
    let harness = RetentionHarness::new_without_retention().await;
    let before = harness.catalog_with(ResourceRetentionConfig::default());
    let after = harness.catalog_with(retention_config(
        ResourceRetentionPolicy::RetainFor(TimeDelta::days(1)),
        None,
    ));

    let earlier = harness
        .apply_and_request_deletion_in(&before, "earlier")
        .await;
    harness.complete_deletion(&before, earlier).await;
    let later = harness.apply_and_request_deletion_in(&after, "later").await;
    harness.complete_deletion(&after, later).await;

    pretty_assertions::assert_eq!(
        harness.purge_at(&earlier).await,
        Some(harness.deleted_at(&earlier).await + TimeDelta::days(7))
    );
    pretty_assertions::assert_eq!(
        harness.purge_at(&later).await,
        Some(harness.deleted_at(&later).await + TimeDelta::days(1))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_due_resource_is_purged_and_one_not_yet_due_kept() {
    let harness = RetentionHarness::new(retention_config(
        ResourceRetentionPolicy::RetainFor(TimeDelta::days(1)),
        None,
    ))
    .await;
    let due = harness.delete_completely("due").await;
    harness.advance_time(TimeDelta::hours(12));
    let not_yet_due = harness.delete_completely("not-yet-due").await;
    harness.advance_time(TimeDelta::hours(13));

    let report = harness
        .run_purge_batch(harness.catalog(), 10)
        .await
        .unwrap();

    pretty_assertions::assert_eq!(report.removed_by_kind, removed_test_resources(1));
    pretty_assertions::assert_eq!(report.backlog, 0);
    assert!(harness.is_purged(&due).await);
    assert!(!harness.is_purged(&not_yet_due).await);
    pretty_assertions::assert_eq!(
        harness.next_due().await,
        harness.purge_at(&not_yet_due).await
    );
}

#[test_log::test(tokio::test)]
async fn test_purge_batches_stay_bounded() {
    let harness = RetentionHarness::purging_immediately().await;
    let mut ids = Vec::new();
    for name in ["first", "second", "third"] {
        ids.push(harness.delete_completely(name).await);
    }

    let first = harness.run_purge_batch(harness.catalog(), 2).await.unwrap();
    let second = harness.run_purge_batch(harness.catalog(), 2).await.unwrap();
    let third = harness.run_purge_batch(harness.catalog(), 2).await.unwrap();

    pretty_assertions::assert_eq!(
        [first, second, third].map(|report| (report.removed(), report.backlog)),
        [(2, 1), (1, 0), (0, 0)]
    );
    for id in &ids {
        assert!(harness.is_purged(id).await);
    }
}

#[test_log::test(tokio::test)]
async fn test_failed_purge_batch_leaves_everything_for_the_next_one() {
    let harness = RetentionHarness::purging_immediately().await;
    let id = harness.delete_completely("res").await;
    let catalog = harness.catalog_with_removal_listener_failing_once();

    harness.run_purge_batch(&catalog, 10).await.unwrap_err();
    assert!(!harness.is_purged(&id).await);
    pretty_assertions::assert_eq!(harness.purged_ids().await, []);

    let report = harness.run_purge_batch(&catalog, 10).await.unwrap();
    pretty_assertions::assert_eq!(report.removed(), 1);
    assert!(harness.is_purged(&id).await);
}

#[test_log::test(tokio::test)]
async fn test_concurrent_purgers_purge_each_resource_once() {
    let harness = RetentionHarness::purging_immediately().await;
    let mut ids = Vec::new();
    for name in ["first", "second", "third"] {
        ids.push(harness.delete_completely(name).await);
    }

    // The second may fail at commit on what the first purged
    let (first, second) = tokio::join!(
        harness.run_purge_batch(harness.catalog(), 10),
        harness.run_purge_batch(harness.catalog(), 10),
    );
    let removed: u64 = [first, second]
        .into_iter()
        .filter_map(Result::ok)
        .map(|report| report.removed())
        .sum();

    pretty_assertions::assert_eq!(removed, 3);
    let mut purged_ids = harness.purged_ids().await;
    purged_ids.sort();
    ids.sort();
    pretty_assertions::assert_eq!(purged_ids, ids);
}

#[test_log::test(tokio::test)]
async fn test_purge_keeps_the_pending_messages_of_purged_resources() {
    let harness = RetentionHarness::purging_immediately().await;
    let id = harness.delete_completely("res").await;
    let pending = harness.lifecycle_messages().await;
    assert!(
        pending.iter().any(|message| matches!(
            message,
            ResourceLifecycleMessage::Deleted(m) if m.resources.iter().any(|r| r.id == id)
        )),
        "the deletion left its message in the outbox"
    );

    harness
        .run_purge_batch(harness.catalog(), 10)
        .await
        .unwrap();

    let messages = harness.lifecycle_messages().await;
    pretty_assertions::assert_eq!(messages[..pending.len()], pending[..]);
    pretty_assertions::assert_eq!(
        messages[pending.len()..],
        [ResourceLifecycleMessage::purged(
            harness.time_source().now(),
            TestResource::type_uri().clone(),
            vec![id],
        )]
    );
    // Consumers still get the messages of the purged resource
    harness.settle().await;
}

#[test_log::test(tokio::test)]
async fn test_cli_finalization_purges_one_batch() {
    let harness = RetentionHarness::purging_immediately().await;
    let id = harness.delete_completely("res").await;

    harness.finalize_with_housekeeping().await;

    assert!(harness.is_purged(&id).await);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn retention_config(
    default: ResourceRetentionPolicy,
    override_of_type: Option<ResourceRetentionPolicy>,
) -> ResourceRetentionConfig {
    ResourceRetentionConfig {
        default,
        types: override_of_type
            .map(|policy| {
                (
                    resource_type_name(TestResource::type_uri()).unwrap(),
                    policy,
                )
            })
            .into_iter()
            .collect(),
    }
}

fn removed_test_resources(count: u64) -> BTreeMap<String, u64> {
    BTreeMap::from([(
        resource_type_name(TestResource::type_uri())
            .unwrap()
            .to_string(),
        count,
    )])
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(ServiceLayerTestHarness, base)]
struct RetentionHarness {
    base: ServiceLayerTestHarness,
    account: odf::ResourceOwnerHandle,
}

impl RetentionHarness {
    async fn new(retention: ResourceRetentionConfig) -> Self {
        Self::new_with(Some(retention)).await
    }

    /// Each transaction takes the retention configuration of its catalog,
    /// [`Self::catalog_with`]
    async fn new_without_retention() -> Self {
        Self::new_with(None).await
    }

    async fn purging_immediately() -> Self {
        Self::new(retention_config(
            ResourceRetentionPolicy::PurgeImmediately,
            None,
        ))
        .await
    }

    async fn new_with(retention: Option<ResourceRetentionConfig>) -> Self {
        let base = ServiceLayerTestHarness::new_with_opts(ServiceLayerTestHarnessOpts {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Dispatching,
                transactional: true,
                fake_time: true,
                retention,
                ..Default::default()
            },
            ..Default::default()
        });
        let account = base.ensure_test_account("test-owner").await;

        Self { base, account }
    }

    /// The harness catalog with a retention configuration
    fn catalog_with(&self, retention: ResourceRetentionConfig) -> Catalog {
        let mut b = CatalogBuilder::new_chained(self.catalog());
        b.add_value(retention);
        b.build()
    }

    async fn apply_and_request_deletion(&self, name: &str) -> ResourceID {
        self.apply_and_request_deletion_in(self.catalog(), name)
            .await
    }

    async fn apply_and_request_deletion_in(&self, catalog: &Catalog, name: &str) -> ResourceID {
        let id = self.apply_and_get_id(&self.account, name).await;
        in_committed_transaction(catalog, |catalog| async move {
            Self::primitives_in(&catalog).request_deletion(&[id]).await
        })
        .await
        .unwrap();
        id
    }

    async fn complete_deletion(&self, catalog: &Catalog, id: ResourceID) {
        let transition = in_committed_transaction(catalog, |catalog| async move {
            Self::primitives_in(&catalog)
                .complete_deletion(&[ResourceDeletionCompletion {
                    id,
                    expected_status_generation: None,
                }])
                .await
        })
        .await
        .unwrap();
        assert_eq!(transition.transitioned.len(), 1);
    }

    async fn deleted_at(&self, id: &ResourceID) -> DateTime<Utc> {
        self.catalog()
            .get_one::<dyn ResourceAggregateLoader<TestResource>>()
            .unwrap()
            .load(id)
            .await
            .unwrap()
            .headers()
            .deleted_at
            .unwrap()
    }

    async fn purge_at(&self, id: &ResourceID) -> Option<DateTime<Utc>> {
        let id = *id;
        in_committed_transaction(self.catalog(), |catalog| async move {
            catalog
                .get_one::<dyn ResourceRepository>()
                .unwrap()
                .find_resources_due_for_purge(DateTime::<Utc>::MAX_UTC, usize::MAX)
                .await
                .unwrap()
                .into_iter()
                .find(|resource| resource.id == id)
                .map(|resource| resource.purge_at)
        })
        .await
    }

    /// The harness catalog with a removal listener whose first call fails
    fn catalog_with_removal_listener_failing_once(&self) -> Catalog {
        let mut b = CatalogBuilder::new_chained(self.catalog());
        b.add_value(RemovalListenerFailingOnce::default())
            .bind::<dyn ResourceRemovalListener, RemovalListenerFailingOnce>();
        b.build()
    }

    async fn delete_completely(&self, name: &str) -> ResourceID {
        let id = self.apply_and_request_deletion(name).await;
        self.complete_deletion(self.catalog(), id).await;
        id
    }

    /// One batch of the retention job, working on `catalog`
    async fn run_purge_batch(
        &self,
        catalog: &Catalog,
        batch_size: usize,
    ) -> Result<HousekeepingBatchReport, InternalError> {
        ResourceRetentionJob::new(catalog.weak_ref())
            .run_batch(self.time_source().now(), batch_size)
            .await
    }

    async fn next_due(&self) -> Option<DateTime<Utc>> {
        ResourceRetentionJob::new(self.catalog().weak_ref())
            .next_due(self.time_source().now())
            .await
            .unwrap()
    }

    /// Finalizes as a CLI command does, housekeeping included
    async fn finalize_with_housekeeping(&self) {
        let mut b = CatalogBuilder::new_chained(self.catalog());
        b.add_value(HousekeepingConfig::default());
        b.add::<HousekeepingMetrics>();
        BackgroundWorkFinalizer::with_housekeeping()
            .finalize(&b.build())
            .await
            .unwrap();
    }

    /// Whether the resource and its events are gone
    async fn is_purged(&self, id: &ResourceID) -> bool {
        let query = ResourceRawEventQuery {
            schema: TestResource::type_uri().clone(),
            id: *id,
        };
        let events: Vec<_> = self
            .catalog()
            .get_one::<dyn ResourceRawEventStore>()
            .unwrap()
            .get_events(&query, GetEventsOpts::default())
            .try_collect()
            .await
            .unwrap();
        let snapshot = self.get_snapshot_by_id(id).await;
        assert_eq!(
            snapshot.is_none(),
            events.is_empty(),
            "the events go with the resource"
        );
        snapshot.is_none()
    }

    async fn lifecycle_messages(&self) -> Vec<ResourceLifecycleMessage> {
        self.catalog()
            .get_one::<dyn OutboxMessageBridge>()
            .unwrap()
            .get_messages_by_producer(
                self.catalog(),
                MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
                OutboxMessageBoundary::default(),
                usize::MAX,
            )
            .await
            .unwrap()
            .into_iter()
            .map(|message| serde_json::from_value(message.content_json).unwrap())
            .collect()
    }

    /// The resources of every `Purged` message, in order
    async fn purged_ids(&self) -> Vec<ResourceID> {
        self.lifecycle_messages()
            .await
            .into_iter()
            .flat_map(|message| match message {
                ResourceLifecycleMessage::Purged(purged) => purged.ids,
                ResourceLifecycleMessage::Applied(_)
                | ResourceLifecycleMessage::DeletionRequested(_)
                | ResourceLifecycleMessage::Deleted(_)
                | ResourceLifecycleMessage::StatusUpdated(_) => Vec::new(),
            })
            .collect()
    }

    fn primitives_in(catalog: &Catalog) -> Arc<dyn ResourceLifecyclePrimitives<TestResource>> {
        catalog.get_one().unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct RemovalListenerFailingOnce {
    has_failed: AtomicBool,
}

#[async_trait::async_trait]
impl ResourceRemovalListener for RemovalListenerFailingOnce {
    async fn on_resources_removed(&self, _ids: &[ResourceID]) -> Result<(), InternalError> {
        if self.has_failed.swap(true, Ordering::SeqCst) {
            Ok(())
        } else {
            Err(InternalError::new("scripted removal listener failure"))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
