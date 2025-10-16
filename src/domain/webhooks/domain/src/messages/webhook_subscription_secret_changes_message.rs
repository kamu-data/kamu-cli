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

use crate::{WebhookSubscriptionID, WebhookSubscriptionSecret};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const WEBHOOK_SUBSCRIPTION_SECRET_CHANGES_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to rotating subscription secret
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WebhookSubscriptionSecretChangesMessage {
    SecretRotated(WebhookSubscriptionSecretChangesMessageSecretRotated),
}

impl WebhookSubscriptionSecretChangesMessage {
    pub fn secret_rotated(
        webhook_subscription_id: WebhookSubscriptionID,
        new_secret: WebhookSubscriptionSecret,
    ) -> Self {
        Self::SecretRotated(WebhookSubscriptionSecretChangesMessageSecretRotated {
            webhook_subscription_id,
            new_secret,
        })
    }
}

impl Message for WebhookSubscriptionSecretChangesMessage {
    fn version() -> u32 {
        WEBHOOK_SUBSCRIPTION_SECRET_CHANGES_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionSecretChangesMessageSecretRotated {
    pub webhook_subscription_id: WebhookSubscriptionID,
    pub new_secret: WebhookSubscriptionSecret,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
