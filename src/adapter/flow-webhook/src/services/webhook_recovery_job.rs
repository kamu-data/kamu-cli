// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface, meta};
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{DatasetEntryService, JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER};
use kamu_flow_system as fs;
use kamu_webhooks::{
    WebhookEventTypeCatalog,
    WebhookSubscription,
    WebhookSubscriptionQueryService,
    WebhookSubscriptionStatus,
    WebhooksConfig,
};
use messaging_outbox::JOB_MESSAGING_OUTBOX_STARTUP;
use time_source::SystemTimeSource;

use crate::webhook_deliver_binding;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const WEBHOOK_RECOVERY_JOB: &str = "webhook_recovery_job";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: WEBHOOK_RECOVERY_JOB,
    depends_on: &[
        JOB_MESSAGING_OUTBOX_STARTUP,
        JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER,
    ],
    requires_transaction: true,
})]
pub struct WebhookTriggerStartupRecoveryJob {
    dataset_entry_service: Arc<dyn DatasetEntryService>,
    webhook_subscription_query_service: Arc<dyn WebhookSubscriptionQueryService>,
    flow_trigger_service: Arc<dyn fs::FlowTriggerService>,
    time_source: Arc<dyn SystemTimeSource>,
    webhooks_config: Arc<WebhooksConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for WebhookTriggerStartupRecoveryJob {
    #[tracing::instrument(
        level = "info",
        skip_all,
        name = "WebhookTriggerStartupRecoveryJob::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        use futures::TryStreamExt;
        let mut entries_stream = self.dataset_entry_service.all_entries();
        while let Some(dataset_entry) = entries_stream.try_next().await? {
            let subscriptions = self
                .webhook_subscription_query_service
                .list_dataset_webhook_subscriptions(&dataset_entry.id)
                .await?;

            for subscription in subscriptions {
                self.sync_subscription(&subscription).await?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WebhookTriggerStartupRecoveryJob {
    fn webhook_trigger_rule(&self) -> fs::FlowTriggerRule {
        fs::FlowTriggerRule::Reactive(fs::ReactiveRule::new(
            fs::BatchingRule::immediate(),
            fs::BreakingChangeRule::Recover,
        ))
    }

    fn webhook_trigger_stop_policy(&self) -> fs::FlowTriggerStopPolicy {
        fs::FlowTriggerStopPolicy::AfterConsecutiveFailures {
            failures_count: fs::ConsecutiveFailuresCount::try_new(
                self.webhooks_config.max_consecutive_failures,
            )
            .unwrap(),
        }
    }

    async fn sync_subscription(
        &self,
        subscription: &WebhookSubscription,
    ) -> Result<(), InternalError> {
        for event_type in subscription.event_types() {
            if event_type.as_ref() != WebhookEventTypeCatalog::DATASET_REF_UPDATED {
                continue;
            }

            let flow_binding =
                webhook_deliver_binding(subscription.id(), event_type, subscription.dataset_id());

            match subscription.status() {
                WebhookSubscriptionStatus::Enabled => {
                    self.ensure_trigger_active(&flow_binding).await?;
                }
                WebhookSubscriptionStatus::Paused => {
                    self.ensure_trigger_paused(&flow_binding).await?;
                }
                WebhookSubscriptionStatus::Unreachable => {
                    self.ensure_trigger_auto_stopped(&flow_binding).await?;
                }
                WebhookSubscriptionStatus::Unverified | WebhookSubscriptionStatus::Removed => {}
            }
        }

        Ok(())
    }

    async fn ensure_trigger_active(
        &self,
        flow_binding: &fs::FlowBinding,
    ) -> Result<(), InternalError> {
        let maybe_trigger = self.flow_trigger_service.find_trigger(flow_binding).await?;
        if let Some(trigger) = maybe_trigger
            && trigger.status == fs::FlowTriggerStatus::Active
        {
            return Ok(());
        }

        self.flow_trigger_service
            .set_trigger(
                self.time_source.now(),
                flow_binding.clone(),
                self.webhook_trigger_rule(),
                self.webhook_trigger_stop_policy(),
            )
            .await
            .int_err()?;

        Ok(())
    }

    async fn ensure_trigger_paused(
        &self,
        flow_binding: &fs::FlowBinding,
    ) -> Result<(), InternalError> {
        let maybe_trigger = self.flow_trigger_service.find_trigger(flow_binding).await?;
        let now = self.time_source.now();

        match maybe_trigger.map(|trigger| trigger.status) {
            Some(fs::FlowTriggerStatus::PausedByUser | fs::FlowTriggerStatus::ScopeRemoved) => {
                Ok(())
            }
            Some(fs::FlowTriggerStatus::Active) => {
                self.flow_trigger_service
                    .pause_flow_trigger(now, flow_binding)
                    .await
            }
            Some(fs::FlowTriggerStatus::StoppedAutomatically) => {
                self.flow_trigger_service
                    .resume_flow_trigger(now, flow_binding)
                    .await?;
                self.flow_trigger_service
                    .pause_flow_trigger(now, flow_binding)
                    .await
            }
            None => {
                self.flow_trigger_service
                    .set_trigger(
                        now,
                        flow_binding.clone(),
                        self.webhook_trigger_rule(),
                        self.webhook_trigger_stop_policy(),
                    )
                    .await
                    .int_err()?;
                self.flow_trigger_service
                    .pause_flow_trigger(now, flow_binding)
                    .await
            }
        }
    }

    async fn ensure_trigger_auto_stopped(
        &self,
        flow_binding: &fs::FlowBinding,
    ) -> Result<(), InternalError> {
        let maybe_trigger = self.flow_trigger_service.find_trigger(flow_binding).await?;
        let now = self.time_source.now();

        match maybe_trigger.map(|trigger| trigger.status) {
            Some(
                fs::FlowTriggerStatus::StoppedAutomatically | fs::FlowTriggerStatus::ScopeRemoved,
            ) => Ok(()),
            Some(fs::FlowTriggerStatus::Active) => {
                self.flow_trigger_service
                    .apply_trigger_auto_stop_decision(now, flow_binding)
                    .await?;
                Ok(())
            }
            Some(fs::FlowTriggerStatus::PausedByUser) => {
                self.flow_trigger_service
                    .resume_flow_trigger(now, flow_binding)
                    .await?;

                self.flow_trigger_service
                    .apply_trigger_auto_stop_decision(now, flow_binding)
                    .await?;
                Ok(())
            }
            None => {
                self.flow_trigger_service
                    .set_trigger(
                        now,
                        flow_binding.clone(),
                        self.webhook_trigger_rule(),
                        self.webhook_trigger_stop_policy(),
                    )
                    .await
                    .int_err()?;
                self.flow_trigger_service
                    .apply_trigger_auto_stop_decision(now, flow_binding)
                    .await?;
                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
