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

pub struct PostgresWebhookEventRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn WebhookEventRepository)]
impl PostgresWebhookEventRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookEventRepository for PostgresWebhookEventRepository {
    async fn create_event(&self, event: &WebhookEvent) -> Result<(), CreateWebhookEventError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
            INSERT INTO webhook_events (id, event_type, payload, created_at)
                VALUES ($1, $2, $3, $4)
            "#,
            event.id.as_ref(),
            event.event_type.as_ref(),
            event.payload,
            event.created_at,
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                CreateWebhookEventError::DuplicateId(WebhookEventDuplicateIdError {
                    event_id: event.id,
                })
            }
            _ => CreateWebhookEventError::Internal(e.int_err()),
        })?;

        Ok(())
    }

    async fn get_event_by_id(
        &self,
        event_id: WebhookEventId,
    ) -> Result<WebhookEvent, GetWebhookEventError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let record = sqlx::query!(
            r#"
            SELECT id, event_type, payload, created_at
                FROM webhook_events
                WHERE id = $1
            "#,
            event_id.as_ref(),
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(record) = record {
            Ok(WebhookEvent {
                id: WebhookEventId::new(record.id),
                event_type: WebhookEventType::try_new(record.event_type).unwrap(),
                payload: record.payload,
                created_at: record.created_at,
            })
        } else {
            Err(GetWebhookEventError::NotFound(WebhookEventNotFoundError {
                event_id,
            }))
        }
    }

    async fn list_recent_events(
        &self,
        pagination: PaginationOpts,
    ) -> Result<Vec<WebhookEvent>, ListRecentWebhookEventsError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let records = sqlx::query!(
            r#"
            SELECT id, event_type, payload, created_at
                FROM webhook_events
                ORDER BY created_at DESC
                LIMIT $1 OFFSET $2
            "#,
            i64::try_from(pagination.limit).unwrap(),
            i64::try_from(pagination.offset).unwrap(),
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let events = records
            .into_iter()
            .map(|record| WebhookEvent {
                id: WebhookEventId::new(record.id),
                event_type: WebhookEventType::try_new(record.event_type).unwrap(),
                payload: record.payload,
                created_at: record.created_at,
            })
            .collect();

        Ok(events)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
