// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use internal_error::InternalError;
use kamu_flow_system::{self as fs, ConsecutiveFailuresCount};
use kamu_webhooks::*;
use messaging_outbox::*;
use time_source::SystemTimeSource;

use crate::{
    FlowScopeSubscription,
    MESSAGE_CONSUMER_KAMU_FLOW_WEBHOOKS_EVENT_BRIDGE,
    webhook_deliver_binding,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<WebhookSubscriptionLifecycleMessage>)]
#[interface(dyn MessageConsumerT<WebhookSubscriptionEventChangesMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_FLOW_WEBHOOKS_EVENT_BRIDGE,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_SERVICE,
        MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_EVENT_CHANGES_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct FlowWebhooksEventBridge {
    time_source: Arc<dyn SystemTimeSource>,
    flow_trigger_service: Arc<dyn fs::FlowTriggerService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for FlowWebhooksEventBridge {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<WebhookSubscriptionEventChangesMessage> for FlowWebhooksEventBridge {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowWebhooksEventBridge[WebhookSubscriptionEventChangesMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &WebhookSubscriptionEventChangesMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received webhook subscription event changes message");

        match message {
            WebhookSubscriptionEventChangesMessage::EventEnabled(message) => {
                if message.event_type.as_ref() == WebhookEventTypeCatalog::DATASET_REF_UPDATED {
                    let flow_binding = webhook_deliver_binding(
                        message.webhook_subscription_id,
                        &message.event_type,
                        message.dataset_id.as_ref(),
                    );
                    self.flow_trigger_service
                        .set_trigger(
                            self.time_source.now(),
                            flow_binding,
                            false, // Enabled
                            fs::FlowTriggerRule::Reactive(fs::ReactiveRule::new(
                                fs::BatchingRule::immediate(),
                                fs::BreakingChangeRule::Recover,
                            )),
                            // TODO: externalize configuration
                            fs::FlowTriggerAutoPausePolicy::AfterConsecutiveFailures {
                                failures_count: ConsecutiveFailuresCount::try_new(5).unwrap(),
                            },
                        )
                        .await
                        .int_err()?;
                }
            }
            WebhookSubscriptionEventChangesMessage::EventDisabled(message) => {
                if message.event_type.as_ref() == WebhookEventTypeCatalog::DATASET_REF_UPDATED {
                    let flow_binding = webhook_deliver_binding(
                        message.webhook_subscription_id,
                        &message.event_type,
                        message.dataset_id.as_ref(),
                    );
                    self.flow_trigger_service
                        .set_trigger(
                            self.time_source.now(),
                            flow_binding,
                            true, // Paused
                            fs::FlowTriggerRule::Reactive(fs::ReactiveRule::new(
                                fs::BatchingRule::immediate(),
                                fs::BreakingChangeRule::Recover,
                            )),
                            // TODO: externalize configuration
                            fs::FlowTriggerAutoPausePolicy::AfterConsecutiveFailures {
                                failures_count: ConsecutiveFailuresCount::try_new(5).unwrap(),
                            },
                        )
                        .await
                        .int_err()?;
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<WebhookSubscriptionLifecycleMessage> for FlowWebhooksEventBridge {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowWebhooksEventBridge[WebhookSubscriptionLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        catalog: &Catalog,
        message: &WebhookSubscriptionLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received webhook subscription lifecycle message");

        match message {
            // Webhook resource is removed,
            // so we need to wipe all the related data in the flow system
            WebhookSubscriptionLifecycleMessage::Deleted(deleted_message) => {
                for event_type in &deleted_message.event_types {
                    let flow_scope = FlowScopeSubscription::make_scope(
                        deleted_message.webhook_subscription_id,
                        event_type,
                        deleted_message.dataset_id.as_ref(),
                    );

                    tracing::debug!(
                        ?flow_scope,
                        "Handling flow scope removal for deleted webhook subscription"
                    );
                    let flow_scope_removal_handlers = catalog
                        .get::<dill::AllOf<dyn fs::FlowScopeRemovalHandler>>()
                        .unwrap();
                    for handler in flow_scope_removal_handlers {
                        handler.handle_flow_scope_removal(&flow_scope).await?;
                    }
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
