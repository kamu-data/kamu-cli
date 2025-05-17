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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn PauseWebhookSubscriptionUseCase)]
pub struct PauseWebhookSubscriptionUseCaseImpl {
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl PauseWebhookSubscriptionUseCase for PauseWebhookSubscriptionUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = PauseWebhookSubscriptionUseCaseImpl_execute,
        skip_all,
        fields(%subscription_id)
    )]
    async fn execute(
        &self,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<(), PauseWebhookSubscriptionError> {
        let mut subscription = crate::helpers::resolve_webhook_subscription(
            subscription_id,
            self.subscription_event_store.clone(),
            PauseWebhookSubscriptionError::NotFound,
            PauseWebhookSubscriptionError::Internal,
        )
        .await?;

        // TODO: idempotency

        subscription
            .pause()
            .map_err(|e| PauseWebhookSubscriptionError::Internal(e.int_err()))?;
        subscription
            .save(self.subscription_event_store.as_ref())
            .await
            .map_err(|e| PauseWebhookSubscriptionError::Internal(e.int_err()))?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
