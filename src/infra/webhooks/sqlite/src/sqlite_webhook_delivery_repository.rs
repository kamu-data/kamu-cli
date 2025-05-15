// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::{PaginationOpts, TransactionRef, TransactionRefT};
use dill::*;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_task_system as ts;
use kamu_webhooks::*;
use sqlx::types::uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteWebhookDeliveryRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn WebhookDeliveryRepository)]
impl SqliteWebhookDeliveryRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookDeliveryRepository for SqliteWebhookDeliveryRepository {
    async fn create(&self, delivery: WebhookDelivery) -> Result<(), CreateWebhookDeliveryError> {
        assert!(delivery.response.is_none());

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let task_id: i64 = delivery.task_attempt_id.task_id.try_into().unwrap();
        let task_attempt_number: i32 =
            i32::try_from(delivery.task_attempt_id.attempt_number).unwrap();
        let event_id = delivery.webhook_event_id.as_ref();
        let subscription_id = delivery.webhook_subscription_id.as_ref();

        let request_headers =
            WebhookDeliveryRecord::serialize_http_headers(&delivery.request.headers)
                .map_err(|e| CreateWebhookDeliveryError::Internal(e.int_err()))?;

        let requested_at = delivery.request.started_at;

        sqlx::query!(
            r#"
            INSERT into webhook_deliveries(task_id, attempt_number, event_id, subscription_id, request_headers, requested_at)
                VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            task_id,
            task_attempt_number,
            event_id,
            subscription_id,
            request_headers,
            requested_at,
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                CreateWebhookDeliveryError::DeliveryExists(WebhookDeliveryAlreadyExistsError {
                    task_attempt_id: delivery.task_attempt_id,
                })
            }
            _ => CreateWebhookDeliveryError::Internal(e.int_err()),
        })?;

        Ok(())
    }

    async fn update_response(
        &self,
        task_attempt_id: ts::TaskAttemptID,
        response: WebhookResponse,
    ) -> Result<(), UpdateWebhookDeliveryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let task_id: i64 = task_attempt_id.task_id.try_into().unwrap();
        let task_attempt_number: i32 = i32::try_from(task_attempt_id.attempt_number).unwrap();

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
                WHERE task_id = $5 AND attempt_number = $6
            "#,
            response_status,
            response_headers,
            response.body,
            response.finished_at,
            task_id,
            task_attempt_number,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn get_by_task_attempt_id(
        &self,
        task_attempt_id: ts::TaskAttemptID,
    ) -> Result<Option<WebhookDelivery>, GetWebhookDeliveryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let task_id: i64 = task_attempt_id.task_id.try_into().unwrap();
        let task_attempt_number: i32 = i32::try_from(task_attempt_id.attempt_number).unwrap();

        let record: Option<WebhookDeliveryRecord> = sqlx::query_as!(
            WebhookDeliveryRecord,
            r#"
            SELECT
                task_id,
                attempt_number as "attempt_number!: i32",
                event_id as "event_id!: uuid::Uuid",
                subscription_id as "subscription_id!: uuid::Uuid",
                request_headers as "request_headers!: serde_json::Value",
                requested_at as "requested_at!: DateTime<Utc>",
                response_code as "response_code: i16",
                response_body,
                response_headers as "response_headers: serde_json::Value",
                response_at as "response_at: DateTime<Utc>"
            FROM webhook_deliveries
                WHERE task_id = $1 AND attempt_number = $2
            "#,
            task_id,
            task_attempt_number,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        record
            .map(WebhookDeliveryRecord::try_into_webhook_delivery)
            .transpose()
            .map_err(GetWebhookDeliveryError::Internal)
    }

    async fn list_by_task_id(
        &self,
        task_id: ts::TaskID,
    ) -> Result<Vec<WebhookDelivery>, ListWebhookDeliveriesError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let task_id: i64 = task_id.try_into().unwrap();

        let records = sqlx::query_as!(
            WebhookDeliveryRecord,
            r#"
            SELECT
                task_id,
                attempt_number as "attempt_number!: i32",
                event_id as "event_id!: uuid::Uuid",
                subscription_id as "subscription_id!: uuid::Uuid",
                request_headers as "request_headers!: serde_json::Value",
                requested_at as "requested_at!: DateTime<Utc>",
                response_code as "response_code: i16",
                response_body,
                response_headers as "response_headers: serde_json::Value",
                response_at as "response_at: DateTime<Utc>"
            FROM webhook_deliveries
                WHERE task_id = $1
            "#,
            task_id,
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

    async fn list_by_event_id(
        &self,
        event_id: WebhookEventID,
    ) -> Result<Vec<WebhookDelivery>, ListWebhookDeliveriesError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let event_id = event_id.as_ref();

        let records = sqlx::query_as!(
            WebhookDeliveryRecord,
            r#"
            SELECT
                task_id,
                attempt_number as "attempt_number!: i32",
                event_id as "event_id!: uuid::Uuid",
                subscription_id as "subscription_id!: uuid::Uuid",
                request_headers as "request_headers!: serde_json::Value",
                requested_at as "requested_at!: DateTime<Utc>",
                response_code as "response_code: i16",
                response_body,
                response_headers as "response_headers: serde_json::Value",
                response_at as "response_at: DateTime<Utc>"
            FROM webhook_deliveries
                WHERE event_id = $1
            "#,
            event_id,
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

    async fn list_by_subscription_id(
        &self,
        event_id: WebhookSubscriptionID,
        pagination: PaginationOpts,
    ) -> Result<Vec<WebhookDelivery>, ListWebhookDeliveriesError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let event_id = event_id.as_ref();

        let limit = i64::try_from(pagination.limit).unwrap();
        let offset = i64::try_from(pagination.offset).unwrap();

        let records = sqlx::query_as!(
            WebhookDeliveryRecord,
            r#"
            SELECT
                task_id,
                attempt_number as "attempt_number!: i32",
                event_id as "event_id!: uuid::Uuid",
                subscription_id as "subscription_id!: uuid::Uuid",
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
            event_id,
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
