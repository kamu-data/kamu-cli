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
use dill::{Catalog, CatalogBuilder};
use kamu_adapter_flow_dataset::{
    DATASET_RESOURCE_TYPE,
    DatasetResourceUpdateDetails,
    DatasetUpdateSource,
};
use kamu_adapter_flow_webhook::{
    FLOW_TYPE_WEBHOOK_DELIVER,
    FlowScopeSubscription,
    WebhookDeliveryBaseResolver,
};
use kamu_flow_system::*;
use kamu_flow_system_inmem::{
    InMemoryFlowActivationLinkRepository,
    InMemoryFlowEventStore,
    InMemoryFlowSystemEventBridge,
};
use kamu_flow_system_services::FlowQueryServiceImpl;
use kamu_task_system::{TaskID, TaskOutcome};
use kamu_wakeup_listener_inmem::InMemoryWakeupHub;
use kamu_webhooks::{
    WebhookSubscription,
    WebhookSubscriptionEventStore,
    WebhookSubscriptionLabel,
    WebhookSubscriptionSecret,
};
use kamu_webhooks_inmem::InMemoryWebhookSubscriptionEventStore;
use kamu_webhooks_services::WebhookSubscriptionQueryServiceImpl;
use secrecy::SecretString;
use time_source::SystemTimeSourceDefault;
use wakeup_listener::WakeupListenerMetrics;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Subscriptions and delivery flows kept in memory, where
/// `WebhookDeliveryBaseResolver` reads the head a receiver already knows
pub(crate) struct WebhookDeliveryHistory {
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
    flow_event_store: Arc<dyn FlowEventStore>,
}

impl WebhookDeliveryHistory {
    /// Registers the resolver with the in-memory stores it reads
    pub(crate) fn register(b: &mut CatalogBuilder) {
        b.add::<WebhookDeliveryBaseResolver>()
            .add::<FlowQueryServiceImpl>()
            .add::<InMemoryFlowEventStore>()
            .add::<InMemoryFlowSystemEventBridge>()
            .add::<InMemoryWakeupHub>()
            .add::<WakeupListenerMetrics>()
            .add::<SystemTimeSourceDefault>()
            .add::<InMemoryFlowActivationLinkRepository>()
            .add::<WebhookSubscriptionQueryServiceImpl>()
            .add::<InMemoryWebhookSubscriptionEventStore>();
    }

    pub(crate) fn from_catalog(catalog: &Catalog) -> Self {
        Self {
            subscription_event_store: catalog.get_one().unwrap(),
            flow_event_store: catalog.get_one().unwrap(),
        }
    }

    /// Stores the subscription of a webhook flow scope, created when the
    /// dataset's head was `dataset_head`
    pub(crate) async fn create_subscription(
        &self,
        webhook_flow_scope: &FlowScope,
        dataset_head: Option<&odf::Multihash>,
    ) {
        let scope = FlowScopeSubscription::new(webhook_flow_scope);
        let mut subscription = WebhookSubscription::new(
            scope.subscription_id(),
            url::Url::parse("https://example.com/webhook").unwrap(),
            WebhookSubscriptionLabel::try_new("test").unwrap(),
            scope.maybe_dataset_id(),
            vec![scope.event_type()],
            WebhookSubscriptionSecret::try_new(None, &SecretString::from("secret")).unwrap(),
            dataset_head.cloned(),
        );
        subscription
            .save(self.subscription_event_store.as_ref())
            .await
            .unwrap();
    }

    /// Stores a delivery flow for the update `old_head` -> `new_head`, finished
    /// with `task_outcome`
    pub(crate) async fn finish_delivery(
        &self,
        webhook_flow_scope: &FlowScope,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
        task_outcome: TaskOutcome,
    ) {
        let now = Utc::now();
        let flow_id = self.flow_event_store.new_flow_id().await.unwrap();
        let task_id = TaskID::new(u64::from(flow_id));

        let mut flow = Flow::new(
            now,
            flow_id,
            FlowBinding::new(FLOW_TYPE_WEBHOOK_DELIVER, webhook_flow_scope.clone()),
            FlowActivationCause::ResourceUpdate(FlowActivationCauseResourceUpdate {
                activation_time: now,
                changes: ResourceChanges::NewData(ResourceDataChanges {
                    blocks_added: 1,
                    records_added: 1,
                    new_watermark: None,
                }),
                resource_type: DATASET_RESOURCE_TYPE.to_string(),
                details: serde_json::to_value(DatasetResourceUpdateDetails {
                    dataset_id: FlowScopeSubscription::new(webhook_flow_scope)
                        .maybe_dataset_id()
                        .unwrap(),
                    source: DatasetUpdateSource::HttpIngest { source_name: None },
                    new_head: new_head.clone(),
                    old_head_maybe: Some(old_head.clone()),
                })
                .unwrap(),
            }),
            None,
            None,
        );
        flow.schedule_for_activation(now, now).unwrap();
        flow.set_relevant_start_condition(
            now,
            FlowStartCondition::Executor(FlowStartConditionExecutor { task_id }),
        )
        .unwrap();
        flow.on_task_scheduled(now, task_id).unwrap();
        flow.on_task_running(now, task_id).unwrap();
        flow.on_task_finished(now, task_id, task_outcome).unwrap();
        flow.save(self.flow_event_store.as_ref()).await.unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
