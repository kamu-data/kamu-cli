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
use dill::CatalogBuilder;
use init_on_startup::InitOnStartup;
use kamu_accounts::{DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_NAME};
use kamu_adapter_flow_webhook::{WebhookTriggerStartupRecoveryJob, webhook_deliver_binding};
use kamu_datasets::DatasetEntry;
use kamu_datasets_services::testing::FakeDatasetEntryService;
use kamu_flow_system::{FlowBinding, FlowTrigger, FlowTriggerEventStore, FlowTriggerStatus};
use kamu_flow_system_inmem::{InMemoryFlowSystemEventBridge, InMemoryFlowTriggerEventStore};
use kamu_flow_system_services::FlowTriggerServiceImpl;
use kamu_webhooks::{
    WebhookEventTypeCatalog,
    WebhookSecretGenerator,
    WebhookSubscription,
    WebhookSubscriptionEventStore,
    WebhookSubscriptionLabel,
    WebhookSubscriptionStatus,
    WebhooksConfig,
};
use kamu_webhooks_inmem::InMemoryWebhookSubscriptionEventStore;
use kamu_webhooks_services::WebhookSecretGeneratorImpl;
use messaging_outbox::{Outbox, OutboxImmediateImpl};
use time_source::SystemTimeSourceDefault;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_recovery_job_enabled_subscription_sets_active_trigger() {
    let harness = WebhookRecoveryJobHarness::new();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"enabled");
    harness.register_dataset_entry(&dataset_id, "enabled");

    let subscription_id = harness
        .create_subscription(&dataset_id, WebhookSubscriptionStatus::Enabled)
        .await;

    harness.run_job().await;

    let status = harness
        .load_trigger_status(&dataset_id, subscription_id)
        .await;
    assert_eq!(status, FlowTriggerStatus::Active);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_recovery_job_paused_subscription_sets_paused_trigger() {
    let harness = WebhookRecoveryJobHarness::new();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"paused");
    harness.register_dataset_entry(&dataset_id, "paused");

    let subscription_id = harness
        .create_subscription(&dataset_id, WebhookSubscriptionStatus::Paused)
        .await;

    harness.run_job().await;

    let status = harness
        .load_trigger_status(&dataset_id, subscription_id)
        .await;
    assert_eq!(status, FlowTriggerStatus::PausedByUser);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_recovery_job_unreachable_subscription_sets_stopped_trigger() {
    let harness = WebhookRecoveryJobHarness::new();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"unreachable");
    harness.register_dataset_entry(&dataset_id, "unreachable");

    let subscription_id = harness
        .create_subscription(&dataset_id, WebhookSubscriptionStatus::Unreachable)
        .await;

    harness.run_job().await;

    let status = harness
        .load_trigger_status(&dataset_id, subscription_id)
        .await;
    assert_eq!(status, FlowTriggerStatus::StoppedAutomatically);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct WebhookRecoveryJobHarness {
    job: Arc<WebhookTriggerStartupRecoveryJob>,
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
    flow_trigger_event_store: Arc<dyn FlowTriggerEventStore>,
    fake_dataset_entry_service: Arc<FakeDatasetEntryService>,
    webhook_secret_generator: WebhookSecretGeneratorImpl,
}

impl WebhookRecoveryJobHarness {
    fn new() -> Self {
        let mut b = CatalogBuilder::new();
        b.add_builder(OutboxImmediateImpl::builder(
            messaging_outbox::ConsumerFilter::AllConsumers,
        ))
        .bind::<dyn Outbox, OutboxImmediateImpl>()
        .add::<FlowTriggerServiceImpl>()
        .add::<InMemoryFlowSystemEventBridge>()
        .add::<InMemoryFlowTriggerEventStore>()
        .add::<InMemoryWebhookSubscriptionEventStore>()
        .add::<FakeDatasetEntryService>()
        .add::<SystemTimeSourceDefault>()
        .add::<WebhookTriggerStartupRecoveryJob>()
        .add_value(WebhooksConfig::default());

        kamu_webhooks_services::register_dependencies(&mut b);

        let catalog = b.build();

        let webhook_secret_generator =
            WebhookSecretGeneratorImpl::new(Arc::new(WebhooksConfig::default()));

        Self {
            job: catalog.get_one().unwrap(),
            subscription_event_store: catalog.get_one().unwrap(),
            flow_trigger_event_store: catalog.get_one().unwrap(),
            fake_dataset_entry_service: catalog.get_one().unwrap(),
            webhook_secret_generator,
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
        status: WebhookSubscriptionStatus,
    ) -> kamu_webhooks::WebhookSubscriptionID {
        let subscription_id = kamu_webhooks::WebhookSubscriptionID::new(Uuid::new_v4());
        let label = WebhookSubscriptionLabel::try_new("test".to_string()).unwrap();

        let mut subscription = WebhookSubscription::new(
            subscription_id,
            url::Url::parse("https://example.com/webhook/").unwrap(),
            label,
            Some(dataset_id.clone()),
            vec![WebhookEventTypeCatalog::dataset_ref_updated()],
            self.webhook_secret_generator.generate_secret().unwrap(),
        );

        match status {
            WebhookSubscriptionStatus::Enabled => {
                subscription.enable().unwrap();
            }
            WebhookSubscriptionStatus::Paused => {
                subscription.enable().unwrap();
                subscription.pause().unwrap();
            }
            WebhookSubscriptionStatus::Unreachable => {
                subscription.enable().unwrap();
                subscription.mark_unreachable().unwrap();
            }
            WebhookSubscriptionStatus::Unverified | WebhookSubscriptionStatus::Removed => {}
        }

        subscription
            .save(self.subscription_event_store.as_ref())
            .await
            .unwrap();

        subscription_id
    }

    async fn run_job(&self) {
        self.job.run_initialization().await.unwrap();
    }

    async fn load_trigger_status(
        &self,
        dataset_id: &odf::DatasetID,
        subscription_id: kamu_webhooks::WebhookSubscriptionID,
    ) -> FlowTriggerStatus {
        let binding = Self::make_binding(dataset_id, subscription_id);
        let flow_trigger = FlowTrigger::load(&binding, self.flow_trigger_event_store.as_ref())
            .await
            .unwrap();
        let state: kamu_flow_system::FlowTriggerState = flow_trigger.into();
        state.status
    }

    fn make_binding(
        dataset_id: &odf::DatasetID,
        subscription_id: kamu_webhooks::WebhookSubscriptionID,
    ) -> FlowBinding {
        webhook_deliver_binding(
            subscription_id,
            &WebhookEventTypeCatalog::dataset_ref_updated(),
            Some(dataset_id),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
