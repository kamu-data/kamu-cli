// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::TransactionRefT;
use internal_error::{InternalError, ResultIntoInternal};
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
    fn wakeup_detector(&self) -> &dyn MessageStoreWakeupDetector {
        &self.wakeup_detector
    }

    async fn push_message(
        &self,
        transaction_catalog: &dill::Catalog,
        message: NewOutboxMessage,
    ) -> Result<(), InternalError> {
        let transaction: Arc<TransactionRefT<Sqlite>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        let message_content_json = message.content_json;
        let message_version: i32 = message
            .version
            .try_into()
            .expect("Version out of range for i32");

        sqlx::query!(
            r#"
            INSERT INTO outbox_messages (producer_name, content_json, occurred_on, version)
                VALUES ($1, $2, $3, $4)
            "#,
            message.producer_name,
            message_content_json,
            message.occurred_on,
            message_version,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn get_unprocessed_messages(
        &self,
        transaction_catalog: &dill::Catalog,
        above_boundaries_by_producer: Vec<(String, OutboxMessageBoundary)>,
        batch_size: usize,
    ) -> Result<Vec<OutboxMessage>, InternalError> {
        assert!(
            !above_boundaries_by_producer.is_empty(),
            "get_unprocessed_messages requires non-empty boundaries"
        );

        let transaction: Arc<TransactionRefT<Sqlite>> = transaction_catalog.get_one().unwrap();

        let json_bounds = serde_json::to_string(
            &above_boundaries_by_producer
                .into_iter()
                // Note: ignore tx_id for SQLite implementation
                .map(|(p, boundary)| {
                    serde_json::json!({"p": p, "id": boundary.message_id.into_inner()})
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();

        let mut tr = transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;
        let batch_size = i64::try_from(batch_size).unwrap();

        let rows = sqlx::query_as!(
            OutboxMessageRow,
            r#"
            WITH bounds AS (
                SELECT
                    json_extract(value, '$.p')  AS producer_name,
                    json_extract(value, '$.id') AS above_id
                FROM json_each($1)
            )
            SELECT
                m.message_id,
                0 AS "tx_id!: i64",
                m.producer_name,
                m.content_json as "content_json: _",
                m.occurred_on as "occurred_on: _",
                m.version as "version!"
            FROM outbox_messages AS m
            JOIN bounds AS b
                ON m.producer_name = b.producer_name AND m.message_id > b.above_id
            ORDER BY m.message_id
            LIMIT $2
            "#,
            json_bounds,
            batch_size
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows.into_iter().map(OutboxMessage::from).collect())
    }

    async fn get_messages_by_producer(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_name: &str,
        above_boundary: OutboxMessageBoundary,
        batch_size: usize,
    ) -> Result<Vec<OutboxMessage>, InternalError> {
        let transaction: Arc<TransactionRefT<Sqlite>> = transaction_catalog.get_one().unwrap();
        let producer_name = producer_name.to_string();
        let above_message_id = above_boundary.message_id.into_inner();

        let mut tr = transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;
        let batch_size = i64::try_from(batch_size).unwrap();

        let rows = sqlx::query_as!(
            OutboxMessageRow,
            r#"
            SELECT
                message_id,
                0 AS "tx_id!: i64",
                producer_name,
                content_json as "content_json: _",
                occurred_on as "occurred_on: _",
                version as "version!"
            FROM outbox_messages
            WHERE producer_name = $1 AND message_id > $2
            ORDER BY message_id
            LIMIT $3
            "#,
            producer_name,
            above_message_id,
            batch_size
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows.into_iter().map(OutboxMessage::from).collect())
    }

    async fn get_latest_message_boundaries_by_producer(
        &self,
        transaction_catalog: &dill::Catalog,
    ) -> Result<Vec<(String, OutboxMessageBoundary)>, InternalError> {
        let transaction: Arc<TransactionRefT<Sqlite>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        let records = sqlx::query!(
            r#"
            SELECT
                producer_name,
                IFNULL(MAX(message_id), 0) AS max_message_id
            FROM outbox_messages
            GROUP BY producer_name
            "#,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(records
            .into_iter()
            .map(|r| {
                (
                    r.producer_name.unwrap(),
                    OutboxMessageBoundary {
                        message_id: OutboxMessageID::new(r.max_message_id),
                        tx_id: 0, // ignore tx_id for SQLite implementation
                    },
                )
            })
            .collect())
    }

    async fn list_consumption_boundaries(
        &self,
        transactional_catalog: &dill::Catalog,
    ) -> Result<Vec<OutboxMessageConsumptionBoundary>, InternalError> {
        let transaction: Arc<TransactionRefT<Sqlite>> = transactional_catalog.get_one().unwrap();

        let mut tr = transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let consumptions = sqlx::query_as!(
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
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(consumptions)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(producer_name, consumer_name, boundary = ?boundary))]
    async fn mark_consumed(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_name: &str,
        consumer_name: &str,
        boundary: OutboxMessageBoundary,
    ) -> Result<(), InternalError> {
        let transaction: Arc<TransactionRefT<Sqlite>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        // Note: in SQLite we ignore the tx_ids as they are not needed
        let last_message_id = boundary.message_id.into_inner();

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
