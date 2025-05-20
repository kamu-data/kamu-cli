// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::*;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetWebhooks<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[Object]
impl<'a> DatasetWebhooks<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    /// Lists all webhook subscriptions for the dataset
    pub async fn subscriptions(&self, ctx: &Context<'_>) -> Result<Vec<WebhookSubscription>> {
        utils::check_dataset_maintain_access(ctx, self.dataset_request_state).await?;

        let webhook_subscription_query_svc =
            from_catalog_n!(ctx, dyn kamu_webhooks::WebhookSubscriptionQueryService);

        match webhook_subscription_query_svc
            .list_dataset_webhook_subscriptions(self.dataset_request_state.dataset_id())
            .await
        {
            Ok(subscriptions) => Ok(subscriptions
                .into_iter()
                .map(|s| WebhookSubscription::new(s.into()))
                .collect()),
            Err(err) => Err(err.into()),
        }
    }

    /// Returns a webhook subscription by ID
    pub async fn subscription(
        &self,
        ctx: &Context<'_>,
        id: WebhookSubscriptionID,
    ) -> Result<Option<WebhookSubscription>> {
        utils::check_dataset_maintain_access(ctx, self.dataset_request_state).await?;

        let webhook_subscription_query_svc =
            from_catalog_n!(ctx, dyn kamu_webhooks::WebhookSubscriptionQueryService);

        match webhook_subscription_query_svc
            .find_webhook_subscription_in_dataset(
                self.dataset_request_state.dataset_id(),
                id.into(),
            )
            .await
        {
            Ok(subscription) => Ok(subscription.map(|s| WebhookSubscription::new(s.into()))),
            Err(err) => Err(err.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
