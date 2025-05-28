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
use database_common_macros::{transactional_method1, transactional_method3};
use dill::{component, interface};
use kamu_task_system as ts;
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
            task_id = %task_id,
            webhook_subscription_id = %webhook_subscription_id,
            webhook_event_id = %webhook_event_id
        )
    )]
    #[transactional_method3(
        webhook_subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
        webhook_event_repo: Arc<dyn WebhookEventRepository>,
        webhook_delivery_repo: Arc<dyn WebhookDeliveryRepository>
    )]
    async fn prepare_delivery(
        &self,
        task_id: ts::TaskID,
        webhook_subscription_id: WebhookSubscriptionID,
        webhook_event_id: WebhookEventID,
    ) -> Result<WebhookDeliveryData, InternalError> {
        let event = webhook_event_repo
            .get_event_by_id(webhook_event_id)
            .await
            .int_err()?;

        let subscription = WebhookSubscription::load(
            webhook_subscription_id,
            webhook_subscription_event_store.as_ref(),
        )
        .await
        .int_err()?;

        let payload_raw_bytes = serde_json::to_vec(&event.payload).int_err()?;
        let payload_bytes = bytes::Bytes::from(payload_raw_bytes);

        let created_at = Utc::now();

        let headers = self.generate_headers(&subscription, &event, &payload_bytes, created_at)?;

        tracing::debug!(?headers, "Webhook delivery headers generated");

        let delivery = WebhookDelivery::new(
            task_id,
            subscription.id(),
            event.id,
            WebhookRequest {
                headers: headers.clone(),
                started_at: created_at,
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
        event: &WebhookEvent,
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
            http::header::HeaderName::from_static(HEADER_WEBHOOK_EVENT_ID),
            http::HeaderValue::from_str(&event.id.to_string()).int_err()?,
        );
        headers.insert(
            http::header::HeaderName::from_static(HEADER_WEBHOOK_SUBSCRIPTION_ID),
            http::HeaderValue::from_str(&subscription.id().to_string()).int_err()?,
        );
        headers.insert(
            http::header::HeaderName::from_static(HEADER_WEBHOOK_EVENT_TYPE),
            http::HeaderValue::from_str(event.event_type.as_ref()).int_err()?,
        );
        headers.insert(
            http::header::HeaderName::from_static(HEADER_WEBHOOK_DELIVERY_ATTEMPT),
            http::HeaderValue::from_str("1").int_err()?,
        );

        Ok(headers)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%task_id))]
    #[transactional_method1(webhook_delivery_repo: Arc<dyn WebhookDeliveryRepository>)]
    async fn write_delivery_response(
        &self,
        task_id: ts::TaskID,
        webhook_response: WebhookResponse,
    ) -> Result<(), InternalError> {
        webhook_delivery_repo
            .update_response(task_id, webhook_response)
            .await
            .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookDeliveryWorker for WebhookDeliveryWorkerImpl {
    async fn deliver_webhook(
        &self,
        task_id: ts::TaskID,
        webhook_subscription_id: uuid::Uuid,
        webhook_event_id: uuid::Uuid,
    ) -> Result<(), internal_error::InternalError> {
        let delivery_data = self
            .prepare_delivery(
                task_id,
                WebhookSubscriptionID::new(webhook_subscription_id),
                WebhookEventID::new(webhook_event_id),
            )
            .await?;

        let webhook_response = self
            .webhook_sender
            .send_webhook(
                delivery_data.target_url,
                delivery_data.payload_bytes,
                delivery_data.headers,
            )
            .await
            .int_err()?;

        self.write_delivery_response(task_id, webhook_response)
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct WebhookDeliveryData {
    target_url: url::Url,
    headers: http::HeaderMap,
    payload_bytes: bytes::Bytes,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
