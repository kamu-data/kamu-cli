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

#[component]
#[interface(dyn WebhookSubscriptionQueryService)]
pub struct WebhookSubscriptionQueryServiceImpl {
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookSubscriptionQueryService for WebhookSubscriptionQueryServiceImpl {
    async fn list_dataset_webhook_subscriptions(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<WebhookSubscription>, InternalError> {
        let subscription_ids = self
            .subscription_event_store
            .list_subscription_ids_by_dataset(dataset_id)
            .await
            .map_err(|e| match e {
                ListWebhookSubscriptionsError::Internal(e) => e,
            })?;

        let subscriptions = kamu_webhooks::WebhookSubscription::load_multi(
            &subscription_ids,
            self.subscription_event_store.as_ref(),
        )
        .await
        .map_err(|e| match e {
            GetEventsError::Internal(e) => e,
        })?;

        let mut res = Vec::with_capacity(subscriptions.len());
        for subscription_res in subscriptions {
            let subscription = match subscription_res {
                Ok(subscription) => subscription,
                Err(LoadError::NotFound(_)) => continue,
                Err(LoadError::ProjectionError(e)) => return Err(e.int_err()),
                Err(LoadError::Internal(e)) => return Err(e),
            };
            res.push(subscription);
        }

        Ok(res)
    }

    async fn list_all_webhook_subscriptions(
        &self,
    ) -> Result<Vec<WebhookSubscription>, InternalError> {
        let subscription_ids = self
            .subscription_event_store
            .list_all_subscription_ids()
            .await
            .map_err(|e| match e {
                ListWebhookSubscriptionsError::Internal(e) => e,
            })?;

        let subscriptions = WebhookSubscription::load_multi(
            &subscription_ids,
            self.subscription_event_store.as_ref(),
        )
        .await
        .map_err(|e| match e {
            GetEventsError::Internal(e) => e,
        })?;

        let mut res = Vec::with_capacity(subscriptions.len());
        for subscription_res in subscriptions {
            let subscription = match subscription_res {
                Ok(subscription) => subscription,
                Err(LoadError::NotFound(_)) => continue,
                Err(LoadError::ProjectionError(e)) => return Err(e.int_err()),
                Err(LoadError::Internal(e)) => return Err(e),
            };
            res.push(subscription);
        }

        Ok(res)
    }

    async fn find_webhook_subscription_in_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<Option<WebhookSubscription>, InternalError> {
        match WebhookSubscription::load(subscription_id, self.subscription_event_store.as_ref())
            .await
        {
            Ok(subscription) => match subscription.dataset_id() {
                Some(subscription_dataset_id) => {
                    if subscription_dataset_id == dataset_id
                        && subscription.status() != WebhookSubscriptionStatus::Removed
                    {
                        Ok(Some(subscription))
                    } else {
                        Ok(None)
                    }
                }
                None => Ok(None),
            },
            Err(LoadError::NotFound(_)) => Ok(None),
            Err(LoadError::ProjectionError(e)) => Err(e.int_err()),
            Err(LoadError::Internal(e)) => Err(e),
        }
    }

    async fn find_webhook_subscription(
        &self,
        subscription_id: WebhookSubscriptionID,
        query_mode: WebhookSubscriptionQueryMode,
    ) -> Result<Option<WebhookSubscription>, InternalError> {
        match WebhookSubscription::load(subscription_id, self.subscription_event_store.as_ref())
            .await
        {
            Ok(subscription) => match query_mode {
                WebhookSubscriptionQueryMode::Active => {
                    if subscription.status() != WebhookSubscriptionStatus::Removed {
                        Ok(Some(subscription))
                    } else {
                        Ok(None)
                    }
                }
                WebhookSubscriptionQueryMode::IncludingRemoved => Ok(Some(subscription)),
            },
            Err(LoadError::NotFound(_)) => Ok(None),
            Err(LoadError::ProjectionError(e)) => Err(e.int_err()),
            Err(LoadError::Internal(e)) => Err(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
