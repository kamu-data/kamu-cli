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
        fields(subscription_id=%subscription.id()),
    )]
    async fn execute(
        &self,
        subscription: &mut WebhookSubscription,
    ) -> Result<(), ResumeWebhookSubscriptionError> {
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
