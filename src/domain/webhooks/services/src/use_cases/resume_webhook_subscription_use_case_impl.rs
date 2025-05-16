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
#[interface(dyn ResumeWebhookSubscriptionUseCase)]
pub struct ResumeWebhookSubscriptionUseCaseImpl {
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl ResumeWebhookSubscriptionUseCase for ResumeWebhookSubscriptionUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = ResumeWebhookSubscriptionUseCaseImpl_execute,
        skip_all,
        fields(%subscription_id)
    )]
    async fn execute(
        &self,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<(), ResumeWebhookSubscriptionError> {
        // TODO: security checks

        let mut subscription = crate::helpers::resolve_webhook_subscription(
            subscription_id,
            self.subscription_event_store.clone(),
            ResumeWebhookSubscriptionError::NotFound,
            ResumeWebhookSubscriptionError::Internal,
        )
        .await?;

        // TODO: idempotency

        subscription
            .resume()
            .map_err(|e| ResumeWebhookSubscriptionError::Internal(e.int_err()))?;
        subscription
            .save(self.subscription_event_store.as_ref())
            .await
            .map_err(|e| ResumeWebhookSubscriptionError::Internal(e.int_err()))?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
