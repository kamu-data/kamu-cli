// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{WebhookEventType, WebhookSubscriptionID, WebhookWatermark};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Watermarks of subscriptions, one per enabled event type that has one
#[async_trait::async_trait]
pub trait WebhookWatermarkRepository: Send + Sync {
    async fn find_watermark(
        &self,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
    ) -> Result<Option<WebhookWatermark>, InternalError>;

    /// Creates the watermark or replaces the existing one
    async fn set_watermark(
        &self,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
        watermark: WebhookWatermark,
    ) -> Result<(), InternalError>;

    async fn remove_watermark(
        &self,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
    ) -> Result<(), InternalError>;

    /// Removes the watermarks of all event types of the subscription
    async fn remove_watermarks(
        &self,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
