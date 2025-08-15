// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Duration;

use crate::{
    WebhookDeliveryID,
    WebhookEventType,
    WebhookRequest,
    WebhookResponse,
    WebhookSubscriptionID,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WebhookDelivery {
    pub webhook_delivery_id: WebhookDeliveryID,
    pub webhook_subscription_id: WebhookSubscriptionID,
    pub event_type: WebhookEventType,
    pub request: WebhookRequest,
    pub response: Option<WebhookResponse>,
}

impl WebhookDelivery {
    pub fn new(
        webhook_delivery_id: WebhookDeliveryID,
        webhook_subscription_id: WebhookSubscriptionID,
        event_type: WebhookEventType,
        request: WebhookRequest,
    ) -> Self {
        Self {
            webhook_delivery_id,
            webhook_subscription_id,
            event_type,
            request,
            response: None,
        }
    }

    pub fn set_response(&mut self, response: WebhookResponse) {
        self.response = Some(response);
    }

    pub fn is_successful(&self) -> bool {
        self.response
            .as_ref()
            .map(|r| r.status_code.is_success())
            .unwrap_or(false)
    }

    pub fn duration(&self) -> Option<Duration> {
        self.response
            .as_ref()
            .map(|r| r.finished_at - self.request.started_at)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
