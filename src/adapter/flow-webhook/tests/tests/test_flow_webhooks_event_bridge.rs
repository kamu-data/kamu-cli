// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{Duration, Utc};
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
use kamu_flow_system::*;
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
    TestWebhooksEventBridgeHarness::add_flow_run_expectation(
        &mut mock_flow_run_service,
        &dataset_id,
        subscription_id,
    );

    let harness = TestWebhooksEventBridgeHarness::new(TestWebhooksEventBridgeHarnessOverrides {
        mock_flow_run_service: Some(mock_flow_run_service),
        ..Default::default()
    });

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
    TestWebhooksEventBridgeHarness::add_flow_run_expectation(
        &mut mock_flow_run_service,
        &dataset_id,
        subscription_id_1,
    );
    TestWebhooksEventBridgeHarness::add_flow_run_expectation(
        &mut mock_flow_run_service,
        &dataset_id,
        subscription_id_2,
    );

    // No task for subscriptions 3 and 4
    let harness = TestWebhooksEventBridgeHarness::new(TestWebhooksEventBridgeHarnessOverrides {
        mock_flow_run_service: Some(mock_flow_run_service),
        ..Default::default()
    });

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
    let harness = TestWebhooksEventBridgeHarness::new(Default::default());

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
    let harness = TestWebhooksEventBridgeHarness::new(Default::default());

    harness.register_dataset_entry(&dataset_id, "foo");

    {
        let mut subscription = WebhookSubscription::new(
            subscription_id,
            url::Url::parse("https://example.com/webhook/").unwrap(),
            WebhookSubscriptionLabel::try_new("").unwrap(),
            Some(dataset_id.clone()),
            vec![WebhookEventTypeCatalog::test()],
        );
        subscription
            .create_secret(harness.webhook_secret_generator.generate_secret().unwrap())
            .unwrap();
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

#[test_log::test(tokio::test)]
async fn test_flow_trigger_paused_on_subscription_pause() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());

    let mut mock_flow_trigger_service = MockFlowTriggerService::new();
    TestWebhooksEventBridgeHarness::add_flow_pause_trigger_expectation(
        &mut mock_flow_trigger_service,
        &dataset_id,
        subscription_id,
    );

    let harness = TestWebhooksEventBridgeHarness::new(TestWebhooksEventBridgeHarnessOverrides {
        mock_flow_trigger_service: Some(mock_flow_trigger_service),
        ..Default::default()
    });

    harness.register_dataset_entry(&dataset_id, "foo");

    harness
        .create_subscription(&dataset_id, subscription_id, true, false) // enabled
        .await;

    {
        let mut subscription =
            WebhookSubscription::load(subscription_id, harness.subscription_event_store.as_ref())
                .await
                .unwrap();

        let pause_use_case = harness
            .catalog
            .get_one::<dyn PauseWebhookSubscriptionUseCase>()
            .unwrap();

        pause_use_case.execute(&mut subscription).await.unwrap();
    }

    // The subscription should be in paused afterwards

    {
        let subscription =
            WebhookSubscription::load(subscription_id, harness.subscription_event_store.as_ref())
                .await
                .unwrap();

        assert_eq!(subscription.status(), WebhookSubscriptionStatus::Paused);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_flow_trigger_resumed_on_subscription_resume() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());

    let mut mock_flow_trigger_service = MockFlowTriggerService::new();
    TestWebhooksEventBridgeHarness::add_flow_set_trigger_expectation(
        &mut mock_flow_trigger_service,
        &dataset_id,
        subscription_id,
        FlowTriggerRule::Reactive(ReactiveRule::new(
            BatchingRule::Immediate,
            BreakingChangeRule::Recover,
        )),
        FlowTriggerStopPolicy::AfterConsecutiveFailures {
            failures_count: ConsecutiveFailuresCount::try_new(
                DEFAULT_MAX_WEBHOOK_CONSECUTIVE_FAILURES,
            )
            .unwrap(),
        },
    );

    let harness = TestWebhooksEventBridgeHarness::new(TestWebhooksEventBridgeHarnessOverrides {
        mock_flow_trigger_service: Some(mock_flow_trigger_service),
        ..Default::default()
    });

    harness.register_dataset_entry(&dataset_id, "foo");

    harness
        .create_subscription(&dataset_id, subscription_id, true, false) // enabled
        .await;

    {
        let mut subscription =
            WebhookSubscription::load(subscription_id, harness.subscription_event_store.as_ref())
                .await
                .unwrap();

        subscription.pause().unwrap(); // without use case, to avoid dealing with pausing notification

        let resume_use_case = harness
            .catalog
            .get_one::<dyn ResumeWebhookSubscriptionUseCase>()
            .unwrap();

        resume_use_case.execute(&mut subscription).await.unwrap();
    }

    // The subscription should be in enabled afterwards

    {
        let subscription =
            WebhookSubscription::load(subscription_id, harness.subscription_event_store.as_ref())
                .await
                .unwrap();

        assert_eq!(subscription.status(), WebhookSubscriptionStatus::Enabled);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_subscription_marked_unreachable_on_trigger_stop() {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());

    let harness = TestWebhooksEventBridgeHarness::new(Default::default());

    harness.register_dataset_entry(&dataset_id, "foo");

    harness
        .create_subscription(&dataset_id, subscription_id, true, false) // enabled
        .await;

    // The subscription should be in enabled initially

    {
        let subscription =
            WebhookSubscription::load(subscription_id, harness.subscription_event_store.as_ref())
                .await
                .unwrap();

        assert_eq!(subscription.status(), WebhookSubscriptionStatus::Enabled);
    }

    // Mimic the trigger was stopped by the system
    harness
        .outbox
        .post_message(
            MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
            FlowTriggerUpdatedMessage {
                event_time: Utc::now(),
                event_id: EventID::new(175),
                flow_binding: webhook_deliver_binding(
                    subscription_id,
                    &WebhookEventTypeCatalog::dataset_ref_updated(),
                    Some(&dataset_id),
                ),
                trigger_status: FlowTriggerStatus::StoppedAutomatically,
                rule: FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
                    every: Duration::days(1),
                })),
                stop_policy: FlowTriggerStopPolicy::default(),
            },
        )
        .await
        .unwrap();

    // The subscription should be in unreachable state now

    {
        let subscription =
            WebhookSubscription::load(subscription_id, harness.subscription_event_store.as_ref())
                .await
                .unwrap();

        assert_eq!(
            subscription.status(),
            WebhookSubscriptionStatus::Unreachable
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestWebhooksEventBridgeHarness {
    catalog: dill::Catalog,
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
    outbox: Arc<dyn Outbox>,
    fake_dataset_entry_service: Arc<FakeDatasetEntryService>,
    webhook_secret_generator: Arc<dyn WebhookSecretGenerator>,
}

#[derive(Default)]
struct TestWebhooksEventBridgeHarnessOverrides {
    mock_flow_run_service: Option<MockFlowRunService>,
    mock_flow_trigger_service: Option<MockFlowTriggerService>,
}

impl TestWebhooksEventBridgeHarness {
    fn new(overrides: TestWebhooksEventBridgeHarnessOverrides) -> Self {
        let mock_flow_run_service = overrides.mock_flow_run_service.unwrap_or_default();

        let mock_flow_trigger_service = overrides.mock_flow_trigger_service.unwrap_or_default();

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
            .bind::<dyn FlowRunService, MockFlowRunService>()
            .add_value(mock_flow_trigger_service)
            .bind::<dyn FlowTriggerService, MockFlowTriggerService>()
            .add_value(WebhooksConfig::default());

        kamu_webhooks_services::register_dependencies(&mut b);

        register_message_dispatcher::<DatasetReferenceMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
        );

        register_message_dispatcher::<FlowTriggerUpdatedMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
        );

        register_message_dispatcher::<WebhookSubscriptionLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_SERVICE,
        );

        register_message_dispatcher::<WebhookSubscriptionEventChangesMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_EVENT_CHANGES_SERVICE,
        );

        let catalog = b.build();

        Self {
            subscription_event_store: catalog.get_one().unwrap(),
            outbox: catalog.get_one().unwrap(),
            fake_dataset_entry_service: catalog.get_one().unwrap(),
            webhook_secret_generator: catalog.get_one().unwrap(),
            catalog,
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
        );
        subscription
            .create_secret(self.webhook_secret_generator.generate_secret().unwrap())
            .unwrap();
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

    fn add_flow_pause_trigger_expectation(
        mock_flow_trigger_service: &mut MockFlowTriggerService,
        dataset_id: &odf::DatasetID,
        subscription_id: WebhookSubscriptionID,
    ) {
        let dataset_id_clone = dataset_id.clone();

        mock_flow_trigger_service
            .expect_pause_flow_trigger()
            .withf(move |_, flow_binding| {
                let webhook_scope = FlowScopeSubscription::new(&flow_binding.scope);
                assert_eq!(flow_binding.flow_type, FLOW_TYPE_WEBHOOK_DELIVER);
                assert_eq!(
                    webhook_scope.maybe_dataset_id(),
                    Some(dataset_id_clone.clone())
                );
                assert_eq!(webhook_scope.subscription_id(), subscription_id);

                true
            })
            .returning(|_, _| Ok(()));
    }

    fn add_flow_set_trigger_expectation(
        mock_flow_trigger_service: &mut MockFlowTriggerService,
        dataset_id: &odf::DatasetID,
        subscription_id: WebhookSubscriptionID,
        expect_rule: FlowTriggerRule,
        expect_stop_policy: FlowTriggerStopPolicy,
    ) {
        let dataset_id_clone = dataset_id.clone();

        mock_flow_trigger_service
            .expect_set_trigger()
            .withf(move |_, flow_binding, rule, stop_policy| {
                let webhook_scope = FlowScopeSubscription::new(&flow_binding.scope);
                assert_eq!(flow_binding.flow_type, FLOW_TYPE_WEBHOOK_DELIVER);
                assert_eq!(
                    webhook_scope.maybe_dataset_id(),
                    Some(dataset_id_clone.clone())
                );
                assert_eq!(webhook_scope.subscription_id(), subscription_id);

                assert_eq!(*rule, expect_rule);
                assert_eq!(*stop_policy, expect_stop_policy);

                true
            })
            .returning(|_, flow_binding, rule, stop_policy| {
                Ok(FlowTriggerState {
                    flow_binding,
                    rule,
                    status: FlowTriggerStatus::Active,
                    stop_policy,
                })
            });
    }

    fn add_flow_run_expectation(
        mock_flow_run_service: &mut MockFlowRunService,
        dataset_id: &odf::DatasetID,
        subscription_id: WebhookSubscriptionID,
    ) {
        let dataset_id_clone_1 = dataset_id.clone();
        let dataset_id_clone_2 = dataset_id.clone();

        mock_flow_run_service
            .expect_run_flow_automatically()
            .withf(
                move |_,
                      flow_binding: &kamu_flow_system::FlowBinding,
                      _,
                      maybe_flow_trigger_rule,
                      maybe_forced_flow_config_rule| {
                    assert_eq!(flow_binding.flow_type, FLOW_TYPE_WEBHOOK_DELIVER);

                    assert!(maybe_flow_trigger_rule.is_none());
                    assert!(maybe_forced_flow_config_rule.is_none());

                    let webhook_scope = FlowScopeSubscription::new(&flow_binding.scope);
                    webhook_scope.maybe_dataset_id().as_ref() == Some(&dataset_id_clone_1)
                        && webhook_scope.subscription_id() == subscription_id
                },
            )
            .returning(move |_, _, _, _, _| {
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
                        completed_at: None,
                    },
                    config_snapshot: None,
                    retry_policy: None,
                })
            });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
