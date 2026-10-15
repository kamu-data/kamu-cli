// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{WebhookEventType, WebhookSubscription, WebhookWatermark};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the watermark of an event type whose deliveries continue from what
/// the receiver has already seen. Event types without a provider have no
/// watermark
#[async_trait::async_trait]
pub trait WebhookEventWatermarkProvider: Send + Sync {
    fn event_type(&self) -> WebhookEventType;

    /// Position at the moment the event type is enabled on the subscription:
    /// events after it are delivered. `None` when there is no position yet
    async fn initial_watermark(
        &self,
        subscription: &WebhookSubscription,
    ) -> Result<Option<WebhookWatermark>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
