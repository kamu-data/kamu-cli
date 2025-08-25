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
use kamu_flow_system::{self as fs, MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE};
use kamu_webhooks::*;
use messaging_outbox::*;
use time_source::SystemTimeSource;

use crate::{
    FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION,
    FlowScopeSubscription,
    MESSAGE_CONSUMER_KAMU_FLOW_WEBHOOKS_EVENT_BRIDGE,
    webhook_deliver_binding,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<WebhookSubscriptionLifecycleMessage>)]
#[interface(dyn MessageConsumerT<WebhookSubscriptionEventChangesMessage>)]
#[interface(dyn MessageConsumerT<fs::FlowTriggerUpdatedMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_FLOW_WEBHOOKS_EVENT_BRIDGE,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_SERVICE,
        MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_EVENT_CHANGES_SERVICE,
        MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct FlowWebhooksEventBridge {
    time_source: Arc<dyn SystemTimeSource>,
    webhooks_config: Arc<WebhooksConfig>,
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
        catalog: &Catalog,
        message: &WebhookSubscriptionEventChangesMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received webhook subscription event changes message");

        let flow_trigger_service = catalog.get_one::<dyn fs::FlowTriggerService>().unwrap();

        match message {
            WebhookSubscriptionEventChangesMessage::EventEnabled(message) => {
                if message.event_type.as_ref() == WebhookEventTypeCatalog::DATASET_REF_UPDATED {
                    let flow_binding = webhook_deliver_binding(
                        message.webhook_subscription_id,
                        &message.event_type,
                        message.dataset_id.as_ref(),
                    );
                    flow_trigger_service
                        .set_trigger(
                            self.time_source.now(),
                            flow_binding,
                            false, // Enabled
                            fs::FlowTriggerRule::Reactive(fs::ReactiveRule::new(
                                fs::BatchingRule::immediate(),
                                fs::BreakingChangeRule::Recover,
                            )),
                            fs::FlowTriggerStopPolicy::AfterConsecutiveFailures {
                                failures_count: fs::ConsecutiveFailuresCount::try_new(
                                    self.webhooks_config.max_consecutive_failures,
                                )
                                .unwrap(),
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
                    flow_trigger_service
                        .set_trigger(
                            self.time_source.now(),
                            flow_binding,
                            true, // Paused
                            fs::FlowTriggerRule::Reactive(fs::ReactiveRule::new(
                                fs::BatchingRule::immediate(),
                                fs::BreakingChangeRule::Recover,
                            )),
                            fs::FlowTriggerStopPolicy::AfterConsecutiveFailures {
                                failures_count: fs::ConsecutiveFailuresCount::try_new(
                                    self.webhooks_config.max_consecutive_failures,
                                )
                                .unwrap(),
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

            WebhookSubscriptionLifecycleMessage::MarkedUnreachable(_) => {
                // Do nothing.
                // Triggers must have been disabled already before this happened
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<fs::FlowTriggerUpdatedMessage> for FlowWebhooksEventBridge {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowWebhooksEventBridge[FlowTriggerUpdatedMessage]"
    )]
    async fn consume_message(
        &self,
        catalog: &Catalog,
        message: &fs::FlowTriggerUpdatedMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received flow trigger updated message");

        // If webhook flow is auto-stopped due to crossing failures threshold,
        // we need to mark the subscription as unreachable
        if message.trigger_status == fs::FlowTriggerStatus::StoppedAutomatically
            && message.flow_binding.scope.scope_type() == FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION
        {
            tracing::debug!(?message.flow_binding, "Marking webhook subscription as unreachable");

            let webhook_subscription_query_service = catalog
                .get_one::<dyn WebhookSubscriptionQueryService>()
                .unwrap();

            let subscription_scope = FlowScopeSubscription::new(&message.flow_binding.scope);

            if let Some(mut webhook_subscription) = webhook_subscription_query_service
                .find_webhook_subscription(
                    subscription_scope.subscription_id(),
                    WebhookSubscriptionQueryMode::Active,
                )
                .await?
            {
                let mark_webhoook_subscription_unreachable_use_case = catalog
                    .get_one::<dyn MarkWebhookSubscriptionUnreachableUseCase>()
                    .unwrap();
                mark_webhoook_subscription_unreachable_use_case
                    .execute(&mut webhook_subscription)
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
