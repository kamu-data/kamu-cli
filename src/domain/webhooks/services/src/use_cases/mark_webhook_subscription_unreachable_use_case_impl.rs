// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use kamu_webhooks::*;
use messaging_outbox::{Outbox, OutboxExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MarkWebhookSubscriptionUnreachableUseCase)]
pub struct MarkWebhookSubscriptionUnreachableUseCaseImpl {
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MarkWebhookSubscriptionUnreachableUseCase for MarkWebhookSubscriptionUnreachableUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = MarkWebhookSubscriptionUnreachableUseCaseImpl_execute,
        skip_all,
        fields(subscription_id=%subscription.id()),
    )]
    async fn execute(
        &self,
        subscription: &mut WebhookSubscription,
    ) -> Result<(), MarkWebhookSubscriptionUnreachableError> {
        let old_status = subscription.status();

        subscription
            .mark_unreachable()
            .map_err(|e: ProjectionError<WebhookSubscriptionState>| {
                tracing::error!(error=?e, error_msg=%e, "Webhook subscription mark unreachable failed");
                MarkWebhookSubscriptionUnreachableError::MarkUnreachableUnexpected(
                    MarkWebhookSubscriptionUnreachableUnexpectedError {
                        status: subscription.status(),
                    },
                )
            })?;

        subscription
            .save(self.subscription_event_store.as_ref())
            .await
            .map_err(|e| MarkWebhookSubscriptionUnreachableError::Internal(e.int_err()))?;

        let latest_status = subscription.status();

        if old_status != latest_status {
            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_SERVICE,
                    WebhookSubscriptionLifecycleMessage::marked_unreachable(
                        subscription.id(),
                        subscription.dataset_id().cloned(),
                        subscription.event_types().to_vec(),
                    ),
                )
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
