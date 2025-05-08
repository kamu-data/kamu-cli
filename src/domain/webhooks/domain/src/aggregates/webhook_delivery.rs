// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Duration;
use kamu_task_system as ts;

use crate::{WebhookEventId, WebhookRequest, WebhookResponse, WebhookSubscriptionId};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WebhookDelivery {
    pub task_attempt_id: ts::TaskAttemptID,
    pub webhook_subscription_id: WebhookSubscriptionId,
    pub webhook_event_id: WebhookEventId,

    pub request: WebhookRequest,
    pub response: Option<WebhookResponse>,
}

impl WebhookDelivery {
    pub fn new(
        task_attempt_id: ts::TaskAttemptID,
        webhook_subscription_id: WebhookSubscriptionId,
        webhook_event_id: WebhookEventId,
        request: WebhookRequest,
    ) -> Self {
        Self {
            task_attempt_id,
            webhook_subscription_id,
            webhook_event_id,
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
