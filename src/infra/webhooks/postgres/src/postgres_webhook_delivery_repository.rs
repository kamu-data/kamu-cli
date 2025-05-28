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
use kamu_task_system as ts;
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

        let task_id: i64 = delivery.task_id.try_into().unwrap();
        let event_id = delivery.webhook_event_id.as_ref();
        let subscription_id = delivery.webhook_subscription_id.as_ref();

        let request_headers =
            WebhookDeliveryRecord::serialize_http_headers(&delivery.request.headers)
                .map_err(|e| CreateWebhookDeliveryError::Internal(e.int_err()))?;

        let requested_at = delivery.request.started_at;

        sqlx::query!(
            r#"
            INSERT into webhook_deliveries(task_id, event_id, subscription_id, request_headers, requested_at)
                VALUES ($1, $2, $3, $4, $5)
            "#,
            task_id,
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
                    task_id: delivery.task_id,
                })
            }
            _ => CreateWebhookDeliveryError::Internal(e.int_err()),
        })?;

        Ok(())
    }

    async fn update_response(
        &self,
        task_id: ts::TaskID,
        response: WebhookResponse,
    ) -> Result<(), UpdateWebhookDeliveryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let task_id: i64 = task_id.try_into().unwrap();

        let response_headers = WebhookDeliveryRecord::serialize_http_headers(&response.headers)
            .map_err(|e| UpdateWebhookDeliveryError::Internal(e.int_err()))?;

        sqlx::query!(
            r#"
            UPDATE webhook_deliveries
                SET response_code = $1,
                    response_headers = $2,
                    response_body = $3,
                    response_at = $4
                WHERE task_id = $5
            "#,
            i16::try_from(response.status_code.as_u16()).unwrap(),
            response_headers,
            response.body,
            response.finished_at,
            task_id,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn get_by_task_id(
        &self,
        task_id: ts::TaskID,
    ) -> Result<Option<WebhookDelivery>, GetWebhookDeliveryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let task_id: i64 = task_id.try_into().unwrap();

        let record: Option<WebhookDeliveryRecord> = sqlx::query_as!(
            WebhookDeliveryRecord,
            r#"
            SELECT
                task_id,
                event_id,
                subscription_id,
                request_headers,
                requested_at,
                response_code,
                response_body,
                response_headers,
                response_at
            FROM webhook_deliveries
                WHERE task_id = $1
            "#,
            task_id,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        record
            .map(WebhookDeliveryRecord::try_into_webhook_delivery)
            .transpose()
            .map_err(GetWebhookDeliveryError::Internal)
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
                event_id,
                subscription_id,
                request_headers,
                requested_at,
                response_code,
                response_body,
                response_headers,
                response_at
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

        let records = sqlx::query_as!(
            WebhookDeliveryRecord,
            r#"
            SELECT
                task_id,
                event_id,
                subscription_id,
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
