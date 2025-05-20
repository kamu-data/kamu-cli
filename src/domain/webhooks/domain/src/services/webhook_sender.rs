// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::WebhookResponse;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(any(feature = "testing", test), mockall::automock)]
#[async_trait::async_trait]
pub trait WebhookSender: Send + Sync {
    async fn send_webhook(
        &self,
        target_url: url::Url,
        payload_bytes: bytes::Bytes,
        headers: http::HeaderMap,
    ) -> Result<WebhookResponse, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
