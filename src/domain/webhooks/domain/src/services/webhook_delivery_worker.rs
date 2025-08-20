// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{WebhookDeliveryID, WebhookEventType, WebhookSubscriptionID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait WebhookDeliveryWorker: Send + Sync {
    async fn deliver_webhook(
        &self,
        webhook_delivery_id: WebhookDeliveryID,
        webhook_subscription_id: WebhookSubscriptionID,
        event_type: WebhookEventType,
        payload: serde_json::Value,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
