// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::{PaginationOpts, TransactionRefT};
use dill::*;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_webhooks::*;
use sqlx::types::uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn WebhookDeliveryRepository)]
pub struct SqliteWebhookDeliveryRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookDeliveryRepository for SqliteWebhookDeliveryRepository {
    async fn create(&self, delivery: WebhookDelivery) -> Result<(), CreateWebhookDeliveryError> {
        assert!(delivery.response.is_none());

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let delivery_id = delivery.webhook_delivery_id.as_ref();
        let subscription_id = delivery.webhook_subscription_id.as_ref();
        let event_type_str = delivery.event_type.to_string();

        let request_headers =
            WebhookDeliveryRecord::serialize_http_headers(&delivery.request.headers)
                .map_err(|e| CreateWebhookDeliveryError::Internal(e.int_err()))?;

        let request_payload = delivery.request.payload;

        let requested_at = delivery.request.started_at;

        sqlx::query!(
            r#"
            INSERT into webhook_deliveries(delivery_id, subscription_id, event_type, request_payload, request_headers, requested_at)
                VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            delivery_id,
            subscription_id,
            event_type_str,
            request_payload,
            request_headers,
            requested_at,
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                CreateWebhookDeliveryError::DeliveryExists(WebhookDeliveryAlreadyExistsError {
                    webhook_delivery_id: delivery.webhook_delivery_id,
                })
            }
            _ => CreateWebhookDeliveryError::Internal(e.int_err()),
        })?;

        Ok(())
    }

    async fn update_response(
        &self,
        delivery_id: WebhookDeliveryID,
        response: WebhookResponse,
    ) -> Result<(), UpdateWebhookDeliveryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let delivery_id = delivery_id.as_ref();

        let response_status = i16::try_from(response.status_code.as_u16()).unwrap();

        let response_headers = WebhookDeliveryRecord::serialize_http_headers(&response.headers)
            .map_err(|e| UpdateWebhookDeliveryError::Internal(e.int_err()))?;

        sqlx::query!(
            r#"
            UPDATE webhook_deliveries
                SET response_code = $1,
                    response_headers = $2,
                    response_body = $3,
                    response_at = $4
                WHERE delivery_id = $5
            "#,
            response_status,
            response_headers,
            response.body,
            response.finished_at,
            delivery_id,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn get_by_webhook_delivery_id(
        &self,
        delivery_id: WebhookDeliveryID,
    ) -> Result<Option<WebhookDelivery>, GetWebhookDeliveryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let delivery_id = delivery_id.as_ref();

        let record: Option<WebhookDeliveryRecord> = sqlx::query_as!(
            WebhookDeliveryRecord,
            r#"
            SELECT
                delivery_id as "delivery_id!: uuid::Uuid",
                subscription_id as "subscription_id!: uuid::Uuid",
                event_type,
                request_payload as "request_payload!: serde_json::Value",
                request_headers as "request_headers!: serde_json::Value",
                requested_at as "requested_at!: DateTime<Utc>",
                response_code as "response_code: i16",
                response_body,
                response_headers as "response_headers: serde_json::Value",
                response_at as "response_at: DateTime<Utc>"
            FROM webhook_deliveries
                WHERE delivery_id = $1
            "#,
            delivery_id,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        record
            .map(WebhookDeliveryRecord::try_into_webhook_delivery)
            .transpose()
            .map_err(GetWebhookDeliveryError::Internal)
    }

    async fn list_by_subscription_id(
        &self,
        subscription_id: WebhookSubscriptionID,
        pagination: PaginationOpts,
    ) -> Result<Vec<WebhookDelivery>, ListWebhookDeliveriesError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let limit = i64::try_from(pagination.limit).unwrap();
        let offset = i64::try_from(pagination.offset).unwrap();

        let subscription_id = subscription_id.as_ref();

        let records = sqlx::query_as!(
            WebhookDeliveryRecord,
            r#"
            SELECT
                delivery_id as "delivery_id!: uuid::Uuid",
                subscription_id as "subscription_id!: uuid::Uuid",
                event_type,
                request_payload as "request_payload!: serde_json::Value",
                request_headers as "request_headers!: serde_json::Value",
                requested_at as "requested_at!: DateTime<Utc>",
                response_code as "response_code: i16",
                response_body,
                response_headers as "response_headers: serde_json::Value",
                response_at as "response_at: DateTime<Utc>"
            FROM webhook_deliveries
                WHERE subscription_id = $1
            ORDER BY requested_at DESC
            LIMIT $2 OFFSET $3
            "#,
            subscription_id,
            limit,
            offset,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        records
            .into_iter()
            .map(|record| {
                record
                    .try_into_webhook_delivery()
                    .map_err(|e| ListWebhookDeliveriesError::Internal(e.int_err()))
            })
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
