// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{PaginationOpts, TransactionRef, TransactionRefT};
use dill::*;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_webhooks::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresWebhookDeliveryRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn WebhookDeliveryRepository)]
impl PostgresWebhookDeliveryRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookDeliveryRepository for PostgresWebhookDeliveryRepository {
    async fn create(&self, delivery: WebhookDelivery) -> Result<(), CreateWebhookDeliveryError> {
        assert!(delivery.response.is_none());

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let delivery_id = delivery.webhook_delivery_id.as_ref();
        let subscription_id = delivery.webhook_subscription_id.as_ref();
        let event_type_str = delivery.event_type.to_string();

        let request_payload = delivery.request.payload;

        let request_headers =
            WebhookDeliveryRecord::serialize_http_headers(&delivery.request.headers)
                .map_err(|e| CreateWebhookDeliveryError::Internal(e.int_err()))?;

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
        webhook_delivery_id: WebhookDeliveryID,
        response: WebhookResponse,
    ) -> Result<(), UpdateWebhookDeliveryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let delivery_id = webhook_delivery_id.as_ref();

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
            i16::try_from(response.status_code.as_u16()).unwrap(),
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
        webhook_delivery_id: WebhookDeliveryID,
    ) -> Result<Option<WebhookDelivery>, GetWebhookDeliveryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let delivery_id = webhook_delivery_id.as_ref();

        let record: Option<WebhookDeliveryRecord> = sqlx::query_as!(
            WebhookDeliveryRecord,
            r#"
            SELECT
                delivery_id,
                subscription_id,
                event_type,
                request_payload,
                request_headers,
                requested_at,
                response_code,
                response_body,
                response_headers,
                response_at
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
        event_id: WebhookSubscriptionID,
        pagination: PaginationOpts,
    ) -> Result<Vec<WebhookDelivery>, ListWebhookDeliveriesError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let event_id = event_id.as_ref();

        let records = sqlx::query_as!(
            WebhookDeliveryRecord,
            r#"
            SELECT
                delivery_id,
                subscription_id,
                event_type,
                request_payload,
                request_headers,
                requested_at,
                response_code,
                response_body,
                response_headers,
                response_at
            FROM webhook_deliveries
                WHERE subscription_id = $1
            ORDER BY requested_at DESC
            LIMIT $2 OFFSET $3
            "#,
            event_id,
            i64::try_from(pagination.limit).unwrap(),
            i64::try_from(pagination.offset).unwrap(),
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
