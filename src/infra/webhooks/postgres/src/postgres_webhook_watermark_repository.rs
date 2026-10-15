// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::TransactionRefT;
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_webhooks::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn WebhookWatermarkRepository)]
pub struct PostgresWebhookWatermarkRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookWatermarkRepository for PostgresWebhookWatermarkRepository {
    async fn find_watermark(
        &self,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
    ) -> Result<Option<WebhookWatermark>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let subscription_id: &uuid::Uuid = subscription_id.as_ref();
        let event_type = event_type.as_ref();

        let maybe_watermark = sqlx::query_scalar!(
            r#"
            SELECT watermark as "watermark!: serde_json::Value"
                FROM webhook_watermarks
                WHERE subscription_id = $1 AND event_type = $2
            "#,
            subscription_id,
            event_type,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_watermark.map(WebhookWatermark::from))
    }

    async fn set_watermark(
        &self,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
        watermark: WebhookWatermark,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let subscription_id: &uuid::Uuid = subscription_id.as_ref();
        let event_type = event_type.as_ref();
        let watermark = serde_json::Value::from(watermark);

        sqlx::query!(
            r#"
            INSERT INTO webhook_watermarks (subscription_id, event_type, watermark)
                VALUES ($1, $2, $3)
                ON CONFLICT (subscription_id, event_type)
                DO UPDATE SET watermark = EXCLUDED.watermark
            "#,
            subscription_id,
            event_type,
            watermark,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn remove_watermark(
        &self,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let subscription_id: &uuid::Uuid = subscription_id.as_ref();
        let event_type = event_type.as_ref();

        sqlx::query!(
            r#"
            DELETE FROM webhook_watermarks
                WHERE subscription_id = $1 AND event_type = $2
            "#,
            subscription_id,
            event_type,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn remove_watermarks(
        &self,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let subscription_id: &uuid::Uuid = subscription_id.as_ref();

        sqlx::query!(
            r#"
            DELETE FROM webhook_watermarks
                WHERE subscription_id = $1
            "#,
            subscription_id,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
