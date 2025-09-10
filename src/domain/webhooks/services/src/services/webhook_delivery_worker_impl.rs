// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use database_common_macros::{transactional_method1, transactional_method2};
use dill::{component, interface};
use kamu_webhooks::*;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn WebhookDeliveryWorker)]
pub struct WebhookDeliveryWorkerImpl {
    catalog: dill::Catalog,
    webhook_signer: Arc<dyn WebhookSigner>,
    webhook_sender: Arc<dyn WebhookSender>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WebhookDeliveryWorkerImpl {
    #[tracing::instrument(
        level="debug",
        skip_all,
        fields(
            %webhook_delivery_id,
            %webhook_subscription_id,
        )
    )]
    #[transactional_method2(
        webhook_subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
        webhook_delivery_repo: Arc<dyn WebhookDeliveryRepository>
    )]
    async fn prepare_delivery(
        &self,
        webhook_delivery_id: WebhookDeliveryID,
        webhook_subscription_id: WebhookSubscriptionID,
        event_type: WebhookEventType,
        payload: serde_json::Value,
    ) -> Result<WebhookDeliveryData, InternalError> {
        let subscription = WebhookSubscription::load(
            &webhook_subscription_id,
            webhook_subscription_event_store.as_ref(),
        )
        .await
        .int_err()?;

        let payload_raw_bytes = serde_json::to_vec(&payload).int_err()?;
        let payload_bytes = bytes::Bytes::from(payload_raw_bytes);

        let created_at = Utc::now();

        let headers = self.generate_headers(
            &subscription,
            &webhook_delivery_id,
            &event_type,
            &payload_bytes,
            created_at,
        )?;

        tracing::debug!(?headers, "Webhook delivery headers generated");

        let delivery = WebhookDelivery::new(
            webhook_delivery_id,
            subscription.id(),
            event_type,
            WebhookRequest {
                headers: headers.clone(),
                started_at: created_at,
                payload,
            },
        );

        webhook_delivery_repo.create(delivery).await.int_err()?;

        Ok(WebhookDeliveryData {
            target_url: subscription.target_url().clone(),
            headers,
            payload_bytes,
        })
    }

    fn generate_headers(
        &self,
        subscription: &WebhookSubscription,
        delivery_id: &WebhookDeliveryID,
        event_type: &WebhookEventType,
        payload_bytes: &[u8],
        created_at: DateTime<Utc>,
    ) -> Result<http::HeaderMap, InternalError> {
        let rfc9421_headers = self.webhook_signer.generate_rfc9421_headers(
            subscription.secret(),
            created_at,
            payload_bytes,
            subscription.target_url(),
        );

        tracing::debug!(?rfc9421_headers, "Generated RFC 9421 headers");

        let mut headers = http::HeaderMap::new();

        // Basic headers
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );

        // RFC 9421 headers
        headers.insert(
            http::header::HeaderName::from_static(HEADER_CONTENT_DIGEST),
            http::HeaderValue::from_str(&rfc9421_headers.content_digest).int_err()?,
        );
        headers.insert(
            http::header::HeaderName::from_static(HEADER_SIGNATURE),
            http::HeaderValue::from_str(&rfc9421_headers.signature).int_err()?,
        );
        headers.insert(
            http::header::HeaderName::from_static(HEADER_SIGNATURE_INPUT),
            http::HeaderValue::from_str(&rfc9421_headers.signature_input).int_err()?,
        );

        // Custom headers
        headers.insert(
            http::header::HeaderName::from_static(HEADER_WEBHOOK_TIMESTAMP),
            http::HeaderValue::from_str(&created_at.timestamp().to_string()).int_err()?,
        );
        headers.insert(
            http::header::HeaderName::from_static(HEADER_WEBHOOK_DELIVERY_ID),
            http::HeaderValue::from_str(&delivery_id.to_string()).int_err()?,
        );
        headers.insert(
            http::header::HeaderName::from_static(HEADER_WEBHOOK_SUBSCRIPTION_ID),
            http::HeaderValue::from_str(&subscription.id().to_string()).int_err()?,
        );
        headers.insert(
            http::header::HeaderName::from_static(HEADER_WEBHOOK_EVENT_TYPE),
            http::HeaderValue::from_str(event_type.as_ref()).int_err()?,
        );
        headers.insert(
            http::header::HeaderName::from_static(HEADER_WEBHOOK_DELIVERY_ATTEMPT),
            http::HeaderValue::from_str("1").int_err()?,
        );

        Ok(headers)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%webhook_delivery_id))]
    #[transactional_method1(webhook_delivery_repo: Arc<dyn WebhookDeliveryRepository>)]
    async fn write_delivery_response(
        &self,
        webhook_delivery_id: WebhookDeliveryID,
        webhook_response: WebhookResponse,
    ) -> Result<(), InternalError> {
        webhook_delivery_repo
            .update_response(webhook_delivery_id, webhook_response)
            .await
            .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookDeliveryWorker for WebhookDeliveryWorkerImpl {
    async fn deliver_webhook(
        &self,
        webhook_delivery_id: WebhookDeliveryID,
        webhook_subscription_id: WebhookSubscriptionID,
        event_type: WebhookEventType,
        payload: serde_json::Value,
    ) -> Result<(), WebhookDeliveryError> {
        // Prepare delivery, headers
        let delivery_data = self
            .prepare_delivery(
                webhook_delivery_id,
                webhook_subscription_id,
                event_type,
                payload,
            )
            .await?;

        // Send the webhook
        let webhook_response = self
            .webhook_sender
            .send_webhook(
                delivery_data.target_url.clone(),
                delivery_data.payload_bytes,
                delivery_data.headers,
            )
            .await?;

        // Write the response back to the database
        let response_status = webhook_response.status_code;
        self.write_delivery_response(webhook_delivery_id, webhook_response)
            .await?;

        // Fail delivery, unless the response is successful
        if response_status.is_success() {
            Ok(())
        } else {
            Err(WebhookDeliveryError::UnsuccessfulResponse(
                WebhookUnsuccessfulResponseError {
                    target_url: delivery_data.target_url,
                    status_code: response_status.as_u16(),
                },
            ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct WebhookDeliveryData {
    target_url: url::Url,
    headers: http::HeaderMap,
    payload_bytes: bytes::Bytes,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
