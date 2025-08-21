// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

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
    ) -> Result<WebhookResponse, WebhookSendError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum WebhookSendError {
    #[error(transparent)]
    FailedToConnect(#[from] WebhookSendFailedToConnectError),

    #[error(transparent)]
    ConnectionTimeout(#[from] WebhookSendConnectionTimeoutError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Failed to connect to webhook target URL: '{target_url}'")]
pub struct WebhookSendFailedToConnectError {
    pub target_url: url::Url,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Webhook target URL '{target_url}' timed out after {timeout:?}")]
pub struct WebhookSendConnectionTimeoutError {
    pub target_url: url::Url,
    pub timeout: std::time::Duration,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
