// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use messaging_outbox::Message;
use serde::{Deserialize, Serialize};

use crate::{WebhookEventType, WebhookSubscriptionID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const WEBHOOK_SUBSCRIPTION_EVENT_CHANGES_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to switching the event types
///  within a webhook subscription
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WebhookSubscriptionEventChangesMessage {
    EventEnabled(WebhookSubscriptionEventChangesMessageEventEnabled),
    EventDisabled(WebhookSubscriptionEventChangesMessageEventDisabled),
}

impl WebhookSubscriptionEventChangesMessage {
    pub fn event_enabled(
        webhook_subscription_id: WebhookSubscriptionID,
        dataset_id: Option<&odf::DatasetID>,
        event_type: WebhookEventType,
    ) -> Self {
        Self::EventEnabled(WebhookSubscriptionEventChangesMessageEventEnabled {
            webhook_subscription_id,
            dataset_id: dataset_id.cloned(),
            event_type,
        })
    }

    pub fn event_disabled(
        webhook_subscription_id: WebhookSubscriptionID,
        dataset_id: Option<&odf::DatasetID>,
        event_type: WebhookEventType,
    ) -> Self {
        Self::EventDisabled(WebhookSubscriptionEventChangesMessageEventDisabled {
            webhook_subscription_id,
            dataset_id: dataset_id.cloned(),
            event_type,
        })
    }
}

impl Message for WebhookSubscriptionEventChangesMessage {
    fn version() -> u32 {
        WEBHOOK_SUBSCRIPTION_EVENT_CHANGES_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents the payload for enabling a specific event type within a webhook
/// subscription
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionEventChangesMessageEventEnabled {
    pub webhook_subscription_id: WebhookSubscriptionID,
    pub dataset_id: Option<odf::DatasetID>,
    pub event_type: WebhookEventType,
}

/// Represents the payload for disabling a specific event type within a webhook
/// subscription
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionEventChangesMessageEventDisabled {
    pub webhook_subscription_id: WebhookSubscriptionID,
    pub dataset_id: Option<odf::DatasetID>,
    pub event_type: WebhookEventType,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
