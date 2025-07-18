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
use kamu_flow_system as fs;
use kamu_webhooks::*;
use messaging_outbox::*;
use time_source::SystemTimeSource;

use crate::FLOW_TYPE_WEBHOOK_DELIVER;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<WebhookSubscriptionLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_WEBHOOK_TRIGGER_ENABLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct WebhookTriggerEnabler {
    time_source: Arc<dyn SystemTimeSource>,
    flow_trigger_service: Arc<dyn fs::FlowTriggerService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WebhookTriggerEnabler {
    fn wants_dataset_updates(&self, event_types: &[WebhookEventType]) -> bool {
        // Check if any of the event types need dataset updates
        event_types.iter().any(|event_type| {
            matches!(
                event_type.as_ref(),
                WebhookEventTypeCatalog::DATASET_REF_UPDATED
            )
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for WebhookTriggerEnabler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<WebhookSubscriptionLifecycleMessage> for WebhookTriggerEnabler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "WebhookTriggerEnabler[WebhookSubscriptionLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &WebhookSubscriptionLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received webhook subscription lifecycle message");

        match message {
            WebhookSubscriptionLifecycleMessage::Enabled(enabled_message) => {
                if self.wants_dataset_updates(&enabled_message.event_types) {
                    let flow_binding = fs::FlowBinding::for_webhook_subscription(
                        enabled_message.webhook_subscription_id.into_inner(),
                        enabled_message.dataset_id.clone(),
                        FLOW_TYPE_WEBHOOK_DELIVER,
                    );
                    self.flow_trigger_service
                        .set_trigger(
                            self.time_source.now(),
                            flow_binding,
                            false,
                            fs::FlowTriggerRule::Batching(fs::BatchingRule::empty()),
                        )
                        .await
                        .int_err()?;
                }
            }
            WebhookSubscriptionLifecycleMessage::Paused(paused_message) => {
                if self.wants_dataset_updates(&paused_message.event_types) {
                    let flow_binding = fs::FlowBinding::for_webhook_subscription(
                        paused_message.webhook_subscription_id.into_inner(),
                        paused_message.dataset_id.clone(),
                        FLOW_TYPE_WEBHOOK_DELIVER,
                    );
                    self.flow_trigger_service
                        .set_trigger(
                            self.time_source.now(),
                            flow_binding,
                            true, // Paused
                            fs::FlowTriggerRule::Batching(fs::BatchingRule::empty()),
                        )
                        .await
                        .int_err()?;
                }
            }
            WebhookSubscriptionLifecycleMessage::Updated(_) => {
                // TODO: a more complex case when even types change
                unimplemented!()
            }
            WebhookSubscriptionLifecycleMessage::Deleted(deleted_message) => {
                let flow_scope = fs::FlowScope::WebhookSubscription {
                    subscription_id: deleted_message.webhook_subscription_id.into_inner(),
                    dataset_id: deleted_message.dataset_id.clone(),
                };
                self.flow_trigger_service
                    .on_trigger_scope_removed(&flow_scope)
                    .await?;
            }

            WebhookSubscriptionLifecycleMessage::Created(_) => { /* Nothing to do */ }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
