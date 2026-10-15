// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_webhooks::{
    WebhookEventType,
    WebhookEventWatermarkProvider,
    WebhookSubscription,
    WebhookWatermarkRepository,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Starts the watermarks of newly enabled event types from the current
/// position, so that their receivers get events after it
#[dill::component(pub)]
pub struct WebhookWatermarkInitializer {
    watermark_providers: Vec<Arc<dyn WebhookEventWatermarkProvider>>,
    watermark_repo: Arc<dyn WebhookWatermarkRepository>,
}

impl WebhookWatermarkInitializer {
    pub async fn initialize_watermarks(
        &self,
        subscription: &WebhookSubscription,
        event_types: &[WebhookEventType],
    ) -> Result<(), InternalError> {
        for event_type in event_types {
            let Some(provider) = self
                .watermark_providers
                .iter()
                .find(|provider| provider.event_type() == *event_type)
            else {
                continue;
            };

            match provider.initial_watermark(subscription).await? {
                Some(watermark) => {
                    self.watermark_repo
                        .set_watermark(subscription.id(), event_type, watermark)
                        .await?;
                }
                None => {
                    self.watermark_repo
                        .remove_watermark(subscription.id(), event_type)
                        .await?;
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
