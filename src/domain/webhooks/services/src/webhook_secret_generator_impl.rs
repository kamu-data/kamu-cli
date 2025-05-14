// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{component, interface};
use kamu_webhooks::WebhookSecretGenerator;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn WebhookSecretGenerator)]
pub struct WebhookSecretGeneratorImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookSecretGenerator for WebhookSecretGeneratorImpl {
    async fn generate_secret(&self) -> String {
        use rand::RngCore;

        let mut bytes = [0u8; 32]; // 32 bytes = 256 bits
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        hex::encode(bytes)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
