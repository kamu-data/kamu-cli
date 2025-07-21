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

use crate::WebhookSubscriptionID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const WEBHOOK_SUBSCRIPTION_LIFECYCLE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to the lifecycle of a webhook subscription
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WebhookSubscriptionLifecycleMessage {
    Deleted(WebhookSubscriptionLifecycleMessageDeleted),
}

impl WebhookSubscriptionLifecycleMessage {
    pub fn deleted(
        webhook_subscription_id: WebhookSubscriptionID,
        dataset_id: Option<odf::DatasetID>,
    ) -> Self {
        Self::Deleted(WebhookSubscriptionLifecycleMessageDeleted {
            webhook_subscription_id,
            dataset_id,
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
pub struct WebhookSubscriptionLifecycleMessageDeleted {
    pub webhook_subscription_id: WebhookSubscriptionID,
    pub dataset_id: Option<odf::DatasetID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
