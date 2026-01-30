// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{WebhookSubscription, WebhookSubscriptionID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait WebhookSubscriptionQueryService: Send + Sync {
    async fn list_dataset_webhook_subscriptions(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<WebhookSubscription>, InternalError>;

    async fn list_all_webhook_subscriptions(
        &self,
    ) -> Result<Vec<WebhookSubscription>, InternalError>;

    async fn find_webhook_subscription_in_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<Option<WebhookSubscription>, InternalError>;

    async fn find_webhook_subscription(
        &self,
        subscription_id: WebhookSubscriptionID,
        query_mode: WebhookSubscriptionQueryMode,
    ) -> Result<Option<WebhookSubscription>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
pub enum WebhookSubscriptionQueryMode {
    Active,
    IncludingRemoved,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
