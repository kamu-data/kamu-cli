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

const WEBHOOK_SUBSCRIPTION_LIFECYCLE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to the lifecycle of a webhook subscription
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WebhookSubscriptionLifecycleMessage {
    MarkedUnreachable(WebhookSubscriptionLifecycleMessageMarkedUnreachable),
    Deleted(WebhookSubscriptionLifecycleMessageDeleted),
}

impl WebhookSubscriptionLifecycleMessage {
    pub fn marked_unreachable(
        webhook_subscription_id: WebhookSubscriptionID,
        dataset_id: Option<odf::DatasetID>,
        event_types: Vec<WebhookEventType>,
    ) -> Self {
        Self::MarkedUnreachable(WebhookSubscriptionLifecycleMessageMarkedUnreachable {
            webhook_subscription_id,
            dataset_id,
            event_types,
        })
    }

    pub fn deleted(
        webhook_subscription_id: WebhookSubscriptionID,
        dataset_id: Option<odf::DatasetID>,
        event_types: Vec<WebhookEventType>,
    ) -> Self {
        Self::Deleted(WebhookSubscriptionLifecycleMessageDeleted {
            webhook_subscription_id,
            dataset_id,
            event_types,
        })
    }
}

impl Message for WebhookSubscriptionLifecycleMessage {
    fn version() -> u32 {
        WEBHOOK_SUBSCRIPTION_LIFECYCLE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionLifecycleMessageMarkedUnreachable {
    pub webhook_subscription_id: WebhookSubscriptionID,
    pub dataset_id: Option<odf::DatasetID>,
    pub event_types: Vec<WebhookEventType>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionLifecycleMessageDeleted {
    pub webhook_subscription_id: WebhookSubscriptionID,
    pub dataset_id: Option<odf::DatasetID>,
    pub event_types: Vec<WebhookEventType>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
