// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crypto_utils::EncryptionError;
use kamu_webhooks::{WebhookSecretGenerator, WebhookSubscriptionSecret, WebhooksConfig};
use secrecy::SecretString;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebhookSecretGeneratorImpl {
    webhook_secret_encryption_key: Option<SecretString>,
}

#[dill::component(pub)]
#[dill::interface(dyn WebhookSecretGenerator)]
impl WebhookSecretGeneratorImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(webhooks_config: Arc<WebhooksConfig>) -> Self {
        Self {
            webhook_secret_encryption_key: webhooks_config
                .secret_encryption_key
                .as_ref()
                .map(|key| SecretString::from(key.clone())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WebhookSecretGenerator for WebhookSecretGeneratorImpl {
    fn generate_secret(&self) -> Result<WebhookSubscriptionSecret, EncryptionError> {
        use rand::RngCore;

        let mut bytes = [0u8; 32]; // 32 bytes = 256 bits
        rand::rng().fill_bytes(&mut bytes);
        let raw_secret = SecretString::from(hex::encode(bytes));

        WebhookSubscriptionSecret::try_new(self.webhook_secret_encryption_key.as_ref(), &raw_secret)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
