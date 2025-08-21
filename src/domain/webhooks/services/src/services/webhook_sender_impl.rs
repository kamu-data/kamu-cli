// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use dill::{component, interface};
use kamu_webhooks::{
    ErrorIntoInternal,
    WebhookResponse,
    WebhookSendConnectionTimeoutError,
    WebhookSendError,
    WebhookSendFailedToConnectError,
    WebhookSender,
};

use crate::KAMU_WEBHOOK_USER_AGENT;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebhookSenderImpl {
    client: reqwest::Client,
    timeout_setting: std::time::Duration,
}

#[component(pub)]
#[interface(dyn WebhookSender)]
impl WebhookSenderImpl {
    pub fn new() -> Self {
        // TODO: externalize configuration of the timeout
        let timeout_setting = std::time::Duration::from_secs(10);

        Self {
            timeout_setting,
            client: reqwest::Client::builder()
                .timeout(timeout_setting)
                .user_agent(KAMU_WEBHOOK_USER_AGENT)
                .build()
                .expect("Failed to build HTTP client"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookSender for WebhookSenderImpl {
    #[tracing::instrument(
        level="debug",
        skip_all,
        fields(target_url = %target_url)
    )]
    async fn send_webhook(
        &self,
        target_url: url::Url,
        payload_bytes: bytes::Bytes,
        headers: http::HeaderMap,
    ) -> Result<WebhookResponse, WebhookSendError> {
        // Form a request with the provided URL, payload, and headers
        let mut request = self.client.post(target_url.clone()).body(payload_bytes);
        for (name, value) in &headers {
            request = request.header(name, value);
        }

        // Send request and handle the response
        tracing::debug!(?request, "Sending webhook request");
        let response = match request.send().await {
            // Sent successfully
            Ok(response) => response,

            // Error occurred while sending
            Err(err) => {
                tracing::error!(
                    error = ?err,
                    error_msg = %err,
                    "Failed to send webhook request"
                );
                if err.is_timeout() {
                    return Err(WebhookSendError::ConnectionTimeout(
                        WebhookSendConnectionTimeoutError {
                            target_url,
                            timeout: self.timeout_setting,
                        },
                    ));
                }
                if err.is_connect() {
                    return Err(WebhookSendError::FailedToConnect(
                        WebhookSendFailedToConnectError { target_url },
                    ));
                }
                // Unknown error
                return Err(WebhookSendError::Internal(err.int_err()));
            }
        };

        // Extract response data
        let status_code = response.status();
        let response_headers = response.headers().clone();
        let response_body = response.text().await.unwrap_or_default();

        tracing::debug!(
            status_code = %status_code,
            response_headers = ?response_headers,
            response_body = %response_body,
            "Webhook response received"
        );

        let webhook_response = WebhookResponse {
            status_code,
            headers: response_headers,
            body: response_body,
            finished_at: Utc::now(),
        };

        Ok(webhook_response)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
