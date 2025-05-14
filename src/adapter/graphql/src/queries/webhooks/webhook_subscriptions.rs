// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::Object;
use kamu_webhooks::LoadError;

use crate::prelude::*;
use crate::queries::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebhookSubscriptions;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl WebhookSubscriptions {
    async fn by_dataset(
        &self,
        ctx: &Context<'_>,
        dataset_id: DatasetID<'_>,
    ) -> Result<Vec<WebhookSubscription>> {
        let subscription_event_store =
            from_catalog_n!(ctx, dyn kamu_webhooks::WebhookSubscriptionEventStore);

        let dataset_id: odf::DatasetID = dataset_id.into();
        let subscription_ids = subscription_event_store
            .list_subscription_ids_by_dataset(&dataset_id)
            .await
            .map_err(|e| match e {
                kamu_webhooks::ListWebhookSubscriptionsError::Internal(e) => GqlError::Internal(e),
            })?;

        let subscriptions = kamu_webhooks::WebhookSubscription::load_multi(
            subscription_ids,
            subscription_event_store.as_ref(),
        )
        .await
        .map_err(|e| GqlError::Internal(e.int_err()))?;

        let mut res = Vec::with_capacity(subscriptions.len());
        for subscription_res in subscriptions {
            let subscription = match subscription_res {
                Ok(subscription) => subscription,
                Err(LoadError::NotFound(_)) => continue,
                Err(LoadError::ProjectionError(e)) => return Err(GqlError::Internal(e.int_err())),
                Err(LoadError::Internal(e)) => return Err(GqlError::Internal(e)),
            };
            let subscription = WebhookSubscription::new(subscription.into());
            res.push(subscription);
        }

        Ok(res)
    }

    async fn by_id(
        &self,
        ctx: &Context<'_>,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<Option<WebhookSubscription>> {
        let subscription_event_store =
            from_catalog_n!(ctx, dyn kamu_webhooks::WebhookSubscriptionEventStore);

        let subscription = match kamu_webhooks::WebhookSubscription::load(
            subscription_id.into(),
            subscription_event_store.as_ref(),
        )
        .await
        {
            Ok(subscription) => subscription,
            Err(LoadError::NotFound(_)) => {
                return Ok(None);
            }
            Err(LoadError::ProjectionError(e)) => return Err(GqlError::Internal(e.int_err())),
            Err(LoadError::Internal(e)) => return Err(GqlError::Internal(e)),
        };

        let subscription = WebhookSubscription::new(subscription.into());
        Ok(Some(subscription))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
