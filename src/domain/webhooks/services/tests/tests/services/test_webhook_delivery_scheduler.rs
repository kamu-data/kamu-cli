// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use database_common::PaginationOpts;
use dill::*;
use kamu_accounts::{DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_NAME};
use kamu_adapter_task_webhook::{LogicalPlanWebhookDeliver, WebhookTaskFactoryImpl};
use kamu_datasets::{
    DatasetEntry,
    DatasetReferenceMessage,
    MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
};
use kamu_datasets_services::testing::FakeDatasetEntryService;
use kamu_task_system::*;
use kamu_webhooks::*;
use kamu_webhooks_inmem::{InMemoryWebhookEventRepository, InMemoryWebhookSubscriptionEventStore};
use kamu_webhooks_services::{WebhookDeliveryScheduler, WebhookEventBuilderImpl};
use messaging_outbox::{Outbox, OutboxExt, OutboxImmediateImpl, register_message_dispatcher};
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_subscription_scheduled_on_dataset_update() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());

    let mut mock_task_scheduler = MockTaskScheduler::new();
    TestWebhookDeliverySchedulerHarness::add_task_expectation(
        &mut mock_task_scheduler,
        subscription_id.into_inner(),
    );
    let harness = TestWebhookDeliverySchedulerHarness::new(mock_task_scheduler);

    harness.register_dataset_entry(&dataset_id, "foo");

    harness
        .create_subscription(&dataset_id, subscription_id, true, false) // enabled
        .await;

    let old_hash = odf::Multihash::from_digest_sha3_256(b"old-hash");
    let new_hash = odf::Multihash::from_digest_sha3_256(b"new-hash");

    harness
        .outbox
        .post_message(
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
            DatasetReferenceMessage::updated(
                &dataset_id,
                &odf::BlockRef::Head,
                Some(&old_hash),
                &new_hash,
            ),
        )
        .await
        .unwrap();

    harness
        .assert_dataset_update_event_recorded(&dataset_id, &old_hash, &new_hash)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_subscriptions_in_different_statuses() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let subscription_id_1 = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
    let subscription_id_2 = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
    let subscription_id_3 = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
    let subscription_id_4 = WebhookSubscriptionID::new(uuid::Uuid::new_v4());

    // No task for subscriptions 3 and 4
    let mut mock_task_scheduler = MockTaskScheduler::new();
    TestWebhookDeliverySchedulerHarness::add_task_expectation(
        &mut mock_task_scheduler,
        subscription_id_1.into_inner(),
    );
    TestWebhookDeliverySchedulerHarness::add_task_expectation(
        &mut mock_task_scheduler,
        subscription_id_2.into_inner(),
    );
    let harness = TestWebhookDeliverySchedulerHarness::new(mock_task_scheduler);

    harness.register_dataset_entry(&dataset_id, "foo");

    harness
        .create_subscription(&dataset_id, subscription_id_1, true, false) // enabled
        .await;
    harness
        .create_subscription(&dataset_id, subscription_id_2, true, false) // enabled
        .await;
    harness
        .create_subscription(&dataset_id, subscription_id_3, false, false) // unverified
        .await;
    harness
        .create_subscription(&dataset_id, subscription_id_4, true, true) // paused
        .await;

    let old_hash = odf::Multihash::from_digest_sha3_256(b"old-hash");
    let new_hash = odf::Multihash::from_digest_sha3_256(b"new-hash");

    harness
        .outbox
        .post_message(
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
            DatasetReferenceMessage::updated(
                &dataset_id,
                &odf::BlockRef::Head,
                Some(&old_hash),
                &new_hash,
            ),
        )
        .await
        .unwrap();

    harness
        .assert_dataset_update_event_recorded(&dataset_id, &old_hash, &new_hash)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_in_wrong_dataset() {
    let dataset_id_1 = odf::DatasetID::new_seeded_ed25519(b"foo");
    let dataset_id_2 = odf::DatasetID::new_seeded_ed25519(b"bar");

    let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());

    // No tasks
    let mock_task_scheduler = MockTaskScheduler::new();
    let harness = TestWebhookDeliverySchedulerHarness::new(mock_task_scheduler);

    harness.register_dataset_entry(&dataset_id_1, "foo");
    harness.register_dataset_entry(&dataset_id_2, "bar");

    harness
        .create_subscription(&dataset_id_1, subscription_id, true, false) // enabled
        .await;

    let old_hash = odf::Multihash::from_digest_sha3_256(b"old-hash");
    let new_hash = odf::Multihash::from_digest_sha3_256(b"new-hash");

    // Update different dataset
    harness
        .outbox
        .post_message(
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
            DatasetReferenceMessage::updated(
                &dataset_id_2,
                &odf::BlockRef::Head,
                Some(&old_hash),
                &new_hash,
            ),
        )
        .await
        .unwrap();

    harness
        .assert_dataset_update_event_recorded(&dataset_id_2, &old_hash, &new_hash)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_subscription_non_matching_event_type() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());

    // No tasks
    let mock_task_scheduler = MockTaskScheduler::new();
    let harness = TestWebhookDeliverySchedulerHarness::new(mock_task_scheduler);

    harness.register_dataset_entry(&dataset_id, "foo");

    {
        let mut subscription = WebhookSubscription::new(
            subscription_id,
            url::Url::parse("https://example.com/webhook/").unwrap(),
            WebhookSubscriptionLabel::try_new("").unwrap(),
            Some(dataset_id.clone()),
            vec![WebhookEventTypeCatalog::test()],
            WebhookSubscriptionSecret::try_new("secret").unwrap(),
        );
        subscription
            .save(harness.subscription_event_store.as_ref())
            .await
            .unwrap();
    }

    let old_hash = odf::Multihash::from_digest_sha3_256(b"old-hash");
    let new_hash = odf::Multihash::from_digest_sha3_256(b"new-hash");

    // Update different dataset
    harness
        .outbox
        .post_message(
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
            DatasetReferenceMessage::updated(
                &dataset_id,
                &odf::BlockRef::Head,
                Some(&old_hash),
                &new_hash,
            ),
        )
        .await
        .unwrap();

    harness
        .assert_dataset_update_event_recorded(&dataset_id, &old_hash, &new_hash)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestWebhookDeliverySchedulerHarness {
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
    webhook_event_repository: Arc<dyn WebhookEventRepository>,
    outbox: Arc<dyn Outbox>,
    fake_dataset_entry_service: Arc<FakeDatasetEntryService>,
}

impl TestWebhookDeliverySchedulerHarness {
    fn new(mock_task_scheduler: MockTaskScheduler) -> Self {
        let mut b = CatalogBuilder::new();
        b.add::<WebhookDeliveryScheduler>()
            .add::<WebhookEventBuilderImpl>()
            .add_builder(
                messaging_outbox::OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            )
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add::<InMemoryWebhookSubscriptionEventStore>()
            .add::<InMemoryWebhookEventRepository>()
            .add_value(mock_task_scheduler)
            .bind::<dyn TaskScheduler, MockTaskScheduler>()
            .add::<FakeDatasetEntryService>()
            .add::<WebhookTaskFactoryImpl>();

        register_message_dispatcher::<DatasetReferenceMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
        );

        let catalog = b.build();

        Self {
            subscription_event_store: catalog.get_one().unwrap(),
            outbox: catalog.get_one().unwrap(),
            webhook_event_repository: catalog.get_one().unwrap(),
            fake_dataset_entry_service: catalog.get_one().unwrap(),
        }
    }

    fn register_dataset_entry(&self, dataset_id: &odf::DatasetID, dataset_name: &str) {
        self.fake_dataset_entry_service.add_entry(DatasetEntry::new(
            dataset_id.clone(),
            DEFAULT_ACCOUNT_ID.clone(),
            DEFAULT_ACCOUNT_NAME.clone(),
            odf::DatasetName::new_unchecked(dataset_name),
            Utc::now(),
            odf::DatasetKind::Root,
        ));
    }

    async fn create_subscription(
        &self,
        dataset_id: &odf::DatasetID,
        subscription_id: WebhookSubscriptionID,
        enabled: bool,
        paused: bool,
    ) {
        let mut subscription = WebhookSubscription::new(
            subscription_id,
            url::Url::parse("https://example.com/webhook/").unwrap(),
            WebhookSubscriptionLabel::try_new("").unwrap(),
            Some(dataset_id.clone()),
            vec![WebhookEventTypeCatalog::dataset_ref_updated()],
            WebhookSubscriptionSecret::try_new("secret").unwrap(),
        );
        if enabled {
            subscription.enable().unwrap();
        }
        if paused {
            subscription.pause().unwrap();
        }
        subscription
            .save(self.subscription_event_store.as_ref())
            .await
            .unwrap();
    }

    fn add_task_expectation(
        mock_task_scheduler: &mut MockTaskScheduler,
        subscription_id_uuid: uuid::Uuid,
    ) {
        mock_task_scheduler
            .expect_create_task()
            .withf(move |plan, _| {
                plan.plan_type == LogicalPlanWebhookDeliver::TYPE_ID
                    && LogicalPlanWebhookDeliver::from_logical_plan(plan)
                        .unwrap()
                        .webhook_subscription_id
                        == subscription_id_uuid
            })
            .returning(|plan, _| {
                let task_id = TaskID::new(35);
                let task = Task::new(Utc::now(), task_id, plan, None);
                Ok(task.into())
            });
    }

    async fn assert_dataset_update_event_recorded(
        &self,
        dataset_id: &odf::DatasetID,
        old_hash: &odf::Multihash,
        new_hash: &odf::Multihash,
    ) {
        let events = self
            .webhook_event_repository
            .list_recent_events(PaginationOpts {
                offset: 0,
                limit: 100,
            })
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].event_type,
            WebhookEventTypeCatalog::dataset_ref_updated()
        );
        assert_eq!(
            events[0].payload,
            json!({
                "blockRef": "head",
                "datasetId": dataset_id.to_string(),
                "newHash": new_hash.to_string(),
                "oldHash": old_hash.to_string(),
                "ownerAccountId": DEFAULT_ACCOUNT_ID.to_string(),
                "version": 1
            })
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
