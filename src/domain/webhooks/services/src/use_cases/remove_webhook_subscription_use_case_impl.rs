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
#[interface(dyn RemoveWebhookSubscriptionUseCase)]
pub struct RemoveWebhookSubscriptionUseCaseImpl {
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl RemoveWebhookSubscriptionUseCase for RemoveWebhookSubscriptionUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = RemoveWebhookSubscriptionUseCaseImpl_execute,
        skip_all,
        fields(subscription_id=%subscription.id()),
    )]
    async fn execute(
        &self,
        subscription: &mut WebhookSubscription,
    ) -> Result<(), RemoveWebhookSubscriptionError> {
        subscription
            .remove()
            .map_err(|e| RemoveWebhookSubscriptionError::Internal(e.int_err()))?;
        subscription
            .save(self.subscription_event_store.as_ref())
            .await
            .map_err(|e| RemoveWebhookSubscriptionError::Internal(e.int_err()))?;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_SERVICE,
                WebhookSubscriptionLifecycleMessage::deleted(
                    subscription.id(),
                    subscription.dataset_id().cloned(),
                    subscription.event_types().to_vec(),
                ),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
