// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::WebhookSubscriptionSecret;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait WebhookSigner: Send + Sync {
    fn generate_rfc9421_headers(
        &self,
        secret: &WebhookSubscriptionSecret,
        timestamp: DateTime<Utc>,
        payload: &[u8],
        target_url: &url::Url,
    ) -> WebhookRFC9421Headers;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct WebhookRFC9421Headers {
    pub signature: String,
    pub signature_input: String,
    pub content_digest: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
