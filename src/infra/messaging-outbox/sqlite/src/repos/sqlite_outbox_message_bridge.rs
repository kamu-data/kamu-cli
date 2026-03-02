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
use database_common::TransactionRefT;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use messaging_outbox::*;
use sqlx::Sqlite;

use crate::SqliteMessageStoreWakeupDetector;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteOutboxMessageBridge {
    wakeup_detector: SqliteMessageStoreWakeupDetector,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::scope(dill::scopes::Agnostic)]
#[dill::interface(dyn OutboxMessageBridge)]
impl SqliteOutboxMessageBridge {
    pub fn new(pool: Arc<sqlx::SqlitePool>) -> Self {
        Self {
            wakeup_detector: SqliteMessageStoreWakeupDetector::new(
                pool,
                "SELECT MAX(message_id) FROM outbox_messages",
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl OutboxMessageBridge for SqliteOutboxMessageBridge {
    /// Provides event store wakeup detector instance
    fn wakeup_detector(&self) -> &dyn MessageStoreWakeupDetector {
        &self.wakeup_detector
    }

    /// Fetch next batch for the given producer-consumer pair;
    ///  order by global id.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(producer_name, consumer_name, batch_size)
    )]
    async fn fetch_next_batch(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_name: &str,
        consumer_name: &str,
        batch_size: usize,
    ) -> Result<Vec<OutboxMessage>, InternalError> {
        let transaction: Arc<TransactionRefT<Sqlite>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        let limit = i64::try_from(batch_size).unwrap();

        let rows = sqlx::query!(
            r#"
            SELECT
                o.message_id          AS "message_id!",
                o.producer_name       AS "producer_name!: String",
                o.occurred_on         AS "occurred_on!: DateTime<Utc>",
                o.content_json        AS "content_json!: serde_json::Value",
                o.version             AS "version!"

            FROM outbox_messages o
            WHERE o.message_id > COALESCE(
                (
                    SELECT last_consumed_message_id FROM outbox_message_consumptions
                    WHERE consumer_name = $1 AND producer_name = $2
                ),
                0
            )
            ORDER BY o.message_id
            LIMIT $3;
            "#,
            consumer_name,
            producer_name,
            limit
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let events = rows
            .into_iter()
            .map(|r| OutboxMessage {
                message_id: OutboxMessageID::new(r.message_id),
                tx_id: 0, // tx_id not tracked in SQLite impl
                producer_name: r.producer_name,
                occurred_on: r.occurred_on,
                content_json: r.content_json,
                version: u32::try_from(r.version).unwrap(),
            })
            .collect();

        Ok(events)
    }

    fn list_consumption_boundaries(
        &self,
        transactional_catalog: &dill::Catalog,
    ) -> OutboxMessageConsumptionBoundariesStream<'_> {
        let transaction: Arc<TransactionRefT<Sqlite>> = transactional_catalog.get_one().unwrap();

        Box::pin(async_stream::stream! {
            let mut tr = transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query_as!(
                OutboxMessageConsumptionBoundary,
                r#"
                SELECT
                    consumer_name,
                    producer_name,
                    last_consumed_message_id,
                    0 AS "last_tx_id!: i64"
                FROM outbox_message_consumptions
                "#,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            use futures::TryStreamExt;
            while let Some(consumption) = query_stream.try_next().await? {
                yield Ok(consumption);
            }
        })
    }

    /// Mark these messages as applied for this producer-consumer pair
    /// (should be idempotent!).
    #[tracing::instrument(level = "debug", skip_all, fields(producer_name, consumer_name))]
    async fn mark_applied(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_name: &str,
        consumer_name: &str,
        message_ids_with_tx_ids: &[(OutboxMessageID, i64)],
    ) -> Result<(), InternalError> {
        if message_ids_with_tx_ids.is_empty() {
            return Ok(());
        }

        let transaction: Arc<TransactionRefT<Sqlite>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        // Use the maximum message_id value from the array to update the boundary
        // Note: in SQLite we ignore the tx_ids as they are not needed
        let last_message_id = message_ids_with_tx_ids
            .iter()
            .map(|(message_id, _)| message_id.into_inner())
            .max()
            .unwrap();

        sqlx::query!(
            r#"
            INSERT INTO outbox_message_consumptions (consumer_name, producer_name, last_consumed_message_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (consumer_name, producer_name) DO UPDATE
                SET
                    last_consumed_message_id = EXCLUDED.last_consumed_message_id
                WHERE (EXCLUDED.last_consumed_message_id)
                    > (outbox_message_consumptions.last_consumed_message_id);
            "#,
            consumer_name,
            producer_name,
            last_message_id
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
