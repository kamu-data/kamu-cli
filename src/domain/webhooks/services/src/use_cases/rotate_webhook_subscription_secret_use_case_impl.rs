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
#[interface(dyn RotateWebhookSubscriptionSecretUseCase)]
pub struct RotateWebhookSubscriptionSecretUseCaseImpl {
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
    webhook_secret_generator: Arc<dyn WebhookSecretGenerator>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl RotateWebhookSubscriptionSecretUseCase for RotateWebhookSubscriptionSecretUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = RotateWebhookSubscriptionSecretUseCaseImpl_execute,
        skip_all,
        fields(subscription_id=%subscription.id()),
    )]
    async fn execute(
        &self,
        subscription: &mut WebhookSubscription,
    ) -> Result<WebhookSubscriptionSecret, RotateWebhookSubscriptionSecretError> {
        let secret = self.webhook_secret_generator.generate_secret().int_err()?;

        subscription.rotate_secret(secret.clone()).int_err()?;

        subscription
            .save(self.subscription_event_store.as_ref())
            .await
            .int_err()?;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_SECRET_CHANGES_SERVICE,
                WebhookSubscriptionSecretChangesMessage::secret_rotated(
                    subscription.id(),
                    secret.clone(),
                ),
            )
            .await?;

        Ok(secret)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
