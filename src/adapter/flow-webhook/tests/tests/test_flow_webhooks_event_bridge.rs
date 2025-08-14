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
use dill::*;
use kamu_accounts::{DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_NAME};
use kamu_adapter_flow_webhook::{
    FLOW_TYPE_WEBHOOK_DELIVER,
    FlowScopeSubscription,
    FlowWebhooksEventBridge,
    webhook_deliver_binding,
};
use kamu_datasets::{
    DatasetEntry,
    DatasetReferenceMessage,
    MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
};
use kamu_datasets_services::testing::FakeDatasetEntryService;
use kamu_flow_system::{
    FlowActivationCause,
    FlowActivationCauseAutoPolling,
    FlowID,
    FlowRunService,
    FlowState,
    FlowTimingRecords,
    MockFlowRunService,
};
use kamu_webhooks::*;
use kamu_webhooks_inmem::InMemoryWebhookSubscriptionEventStore;
use messaging_outbox::{Outbox, OutboxExt, OutboxImmediateImpl, register_message_dispatcher};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_subscription_scheduled_on_dataset_update() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());

    let mut mock_flow_run_service = MockFlowRunService::new();
    TestWebhookDeliverySchedulerHarness::add_flow_trigger_expectation(
        &mut mock_flow_run_service,
        &dataset_id,
        subscription_id,
    );

    let harness = TestWebhookDeliverySchedulerHarness::new(mock_flow_run_service);

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_subscriptions_in_different_statuses() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let subscription_id_1 = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
    let subscription_id_2 = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
    let subscription_id_3 = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
    let subscription_id_4 = WebhookSubscriptionID::new(uuid::Uuid::new_v4());

    let mut mock_flow_run_service = MockFlowRunService::new();
    TestWebhookDeliverySchedulerHarness::add_flow_trigger_expectation(
        &mut mock_flow_run_service,
        &dataset_id,
        subscription_id_1,
    );
    TestWebhookDeliverySchedulerHarness::add_flow_trigger_expectation(
        &mut mock_flow_run_service,
        &dataset_id,
        subscription_id_2,
    );

    // No task for subscriptions 3 and 4
    let harness = TestWebhookDeliverySchedulerHarness::new(mock_flow_run_service);

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_in_wrong_dataset() {
    let dataset_id_1 = odf::DatasetID::new_seeded_ed25519(b"foo");
    let dataset_id_2 = odf::DatasetID::new_seeded_ed25519(b"bar");

    let subscription_id: WebhookSubscriptionID = WebhookSubscriptionID::new(uuid::Uuid::new_v4());

    // No tasks
    let harness = TestWebhookDeliverySchedulerHarness::new(MockFlowRunService::new());

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_subscription_non_matching_event_type() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());

    // No tasks
    let harness = TestWebhookDeliverySchedulerHarness::new(MockFlowRunService::new());

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestWebhookDeliverySchedulerHarness {
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
    outbox: Arc<dyn Outbox>,
    fake_dataset_entry_service: Arc<FakeDatasetEntryService>,
}

impl TestWebhookDeliverySchedulerHarness {
    fn new(mock_flow_run_service: MockFlowRunService) -> Self {
        let mut b = CatalogBuilder::new();
        b.add::<FlowWebhooksEventBridge>()
            .add_builder(
                messaging_outbox::OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            )
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add::<InMemoryWebhookSubscriptionEventStore>()
            .add::<FakeDatasetEntryService>()
            .add::<SystemTimeSourceDefault>()
            .add_value(mock_flow_run_service)
            .bind::<dyn FlowRunService, MockFlowRunService>();

        register_message_dispatcher::<DatasetReferenceMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
        );

        let catalog = b.build();

        Self {
            subscription_event_store: catalog.get_one().unwrap(),
            outbox: catalog.get_one().unwrap(),
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

    fn add_flow_trigger_expectation(
        mock_flow_run_service: &mut MockFlowRunService,
        dataset_id: &odf::DatasetID,
        subscription_id: WebhookSubscriptionID,
    ) {
        let dataset_id_clone_1 = dataset_id.clone();
        let dataset_id_clone_2 = dataset_id.clone();

        mock_flow_run_service
            .expect_run_flow_automatically()
            .withf(
                move |flow_binding: &kamu_flow_system::FlowBinding,
                      _,
                      maybe_flow_trigger_rule,
                      maybe_forced_flow_config_rule| {
                    assert_eq!(flow_binding.flow_type, FLOW_TYPE_WEBHOOK_DELIVER);

                    assert!(maybe_flow_trigger_rule.is_none());
                    assert!(maybe_forced_flow_config_rule.is_none());

                    let webhook_scope = FlowScopeSubscription::new(&flow_binding.scope);
                    webhook_scope.maybe_dataset_id().as_ref() == Some(&dataset_id_clone_1)
                        && webhook_scope.webhook_subscription_id() == subscription_id
                },
            )
            .returning(move |_, _, _, _| {
                let now = Utc::now();

                Ok(FlowState {
                    flow_id: FlowID::new(1),
                    flow_binding: webhook_deliver_binding(
                        subscription_id,
                        &WebhookEventTypeCatalog::dataset_ref_updated(),
                        Some(&dataset_id_clone_2),
                    ),
                    start_condition: None,
                    task_ids: vec![],
                    activation_causes: vec![FlowActivationCause::AutoPolling(
                        FlowActivationCauseAutoPolling {
                            activation_time: now,
                        },
                    )],
                    late_activation_causes: vec![],
                    outcome: None,
                    timing: FlowTimingRecords {
                        first_scheduled_at: Some(now),
                        scheduled_for_activation_at: Some(now),
                        running_since: None,
                        awaiting_executor_since: None,
                        last_attempt_finished_at: None,
                    },
                    config_snapshot: None,
                    retry_policy: None,
                })
            });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
