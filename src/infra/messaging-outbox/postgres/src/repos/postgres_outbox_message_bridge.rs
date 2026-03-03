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
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use messaging_outbox::*;
use sqlx::Postgres;

use crate::PostgresMessageStoreWakeupDetector;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const NOTIFY_CHANNEL_NAME: &str = "outbox_messages_ready";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresOutboxMessageBridge {
    wakeup_detector: PostgresMessageStoreWakeupDetector,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::scope(dill::scopes::Agnostic)]
#[dill::interface(dyn OutboxMessageBridge)]
impl PostgresOutboxMessageBridge {
    pub fn new(pool: Arc<sqlx::PgPool>) -> Self {
        Self {
            wakeup_detector: PostgresMessageStoreWakeupDetector::new(pool, NOTIFY_CHANNEL_NAME),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl OutboxMessageBridge for PostgresOutboxMessageBridge {
    fn wakeup_detector(&self) -> &dyn MessageStoreWakeupDetector {
        &self.wakeup_detector
    }

    async fn push_message(
        &self,
        transaction_catalog: &dill::Catalog,
        message: NewOutboxMessage,
    ) -> Result<(), InternalError> {
        let transaction: Arc<TransactionRefT<Postgres>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        let message_version: i32 = message.version.try_into().unwrap();

        sqlx::query!(
            r#"
            INSERT INTO outbox_messages (producer_name, content_json, occurred_on, version)
                VALUES ($1, $2, $3, $4)
            "#,
            message.producer_name,
            &message.content_json,
            message.occurred_on,
            message_version,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    fn get_unprocessed_messages(
        &self,
        transaction_catalog: &dill::Catalog,
        above_boundaries_by_producer: Vec<(String, OutboxMessageBoundary)>,
        batch_size: usize,
    ) -> OutboxMessageStream<'_> {
        assert!(
            !above_boundaries_by_producer.is_empty(),
            "get_unprocessed_messages requires non-empty boundaries"
        );

        let transaction: Arc<TransactionRefT<Postgres>> = transaction_catalog.get_one().unwrap();

        let mut producers = Vec::with_capacity(above_boundaries_by_producer.len());
        let mut above_tx_ids = Vec::with_capacity(above_boundaries_by_producer.len());
        let mut above_message_ids = Vec::with_capacity(above_boundaries_by_producer.len());

        for (producer_name, boundary) in above_boundaries_by_producer {
            producers.push(producer_name);
            above_tx_ids.push(boundary.tx_id);
            above_message_ids.push(boundary.message_id.into_inner());
        }

        Box::pin(async_stream::stream! {
            let mut tr = transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut stream = sqlx::query_as!(
                OutboxMessageRow,
                r#"
                WITH bounds AS (
                    SELECT *
                    FROM UNNEST($1::text[], $2::bigint[], $3::bigint[]) AS b(
                        producer_name,
                        above_tx_id,
                        above_message_id
                    )
                )
                SELECT
                    m.message_id,
                    m.tx_id::text::bigint AS "tx_id!: i64",
                    m.producer_name,
                    m.content_json,
                    m.occurred_on,
                    m.version as "version!"
                FROM outbox_messages AS m
                JOIN bounds AS b
                    ON m.producer_name = b.producer_name
                    AND (
                        -- tx_id = 0 is a special case, meaning that we want to get messages
                        -- with message_id > above_message_id regardless of tx_id
                        (b.above_tx_id = 0 AND m.message_id > b.above_message_id)
                        OR (
                            b.above_tx_id <> 0
                            AND (
                                m.tx_id::text::bigint > b.above_tx_id
                                OR (
                                    m.tx_id::text::bigint = b.above_tx_id
                                    AND m.message_id > b.above_message_id
                                )
                            )
                        )
                    )
                WHERE
                    -- Ignore rows from txns that might still be in flight ("Usain Bolt")
                    -- while still allowing rows from current transaction.
                    m.tx_id < pg_snapshot_xmin(pg_current_snapshot())
                    OR m.tx_id = pg_current_xact_id()
                ORDER BY m.tx_id, m.message_id
                LIMIT $4
                "#,
                &producers,
                &above_tx_ids,
                &above_message_ids,
                i64::try_from(batch_size).unwrap()
            )
            .fetch(connection_mut);

            while let Some(message_row) =
                stream.try_next().await.map_err(ErrorIntoInternal::int_err)?
            {
                let message: OutboxMessage = message_row.into();
                yield Ok(message);
            }
        })
    }

    fn get_messages_by_producer(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_name: &str,
        above_boundary: OutboxMessageBoundary,
        batch_size: usize,
    ) -> OutboxMessageStream<'_> {
        let transaction: Arc<TransactionRefT<Postgres>> = transaction_catalog.get_one().unwrap();

        let producer_name = producer_name.to_string();
        let above_tx_id = above_boundary.tx_id;
        let above_message_id = above_boundary.message_id.into_inner();

        Box::pin(async_stream::stream! {
            let mut tr = transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut stream = sqlx::query_as!(
                OutboxMessageRow,
                r#"
                SELECT
                    message_id,
                    tx_id::text::bigint AS "tx_id!: i64",
                    producer_name,
                    content_json,
                    occurred_on,
                    version as "version!"
                FROM outbox_messages
                WHERE
                    producer_name = $1
                    AND (
                        ($2::bigint = 0 AND message_id > $3)
                        OR (
                            $2::bigint <> 0
                            AND (
                                tx_id::text::bigint > $2::bigint
                                OR (tx_id::text::bigint = $2::bigint AND message_id > $3)
                            )
                        )
                    )
                ORDER BY tx_id, message_id
                LIMIT $4
                "#,
                producer_name,
                above_tx_id,
                above_message_id,
                i64::try_from(batch_size).unwrap()
            )
            .fetch(connection_mut);

            while let Some(message_row) =
                stream.try_next().await.map_err(ErrorIntoInternal::int_err)?
            {
                let message: OutboxMessage = message_row.into();
                yield Ok(message);
            }
        })
    }

    async fn get_latest_message_boundaries_by_producer(
        &self,
        transaction_catalog: &dill::Catalog,
    ) -> Result<Vec<(String, OutboxMessageBoundary)>, InternalError> {
        let transaction: Arc<TransactionRefT<Postgres>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        let records = sqlx::query!(
            r#"
            SELECT
                producer_name,
                message_id AS "message_id!",
                tx_id::text::bigint AS "tx_id!: i64"
            FROM (
                SELECT
                    producer_name,
                    message_id,
                    tx_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY producer_name
                        ORDER BY tx_id DESC, message_id DESC
                    ) AS rn
                FROM outbox_messages
                WHERE
                    -- Ignore rows from txns that might still be in flight ("Usain Bolt")
                    -- while still allowing rows from current transaction.
                    tx_id < pg_snapshot_xmin(pg_current_snapshot())
                    OR tx_id = pg_current_xact_id()
            ) ranked
            WHERE rn = 1
            "#,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(records
            .into_iter()
            .map(|r| {
                (
                    r.producer_name,
                    OutboxMessageBoundary {
                        message_id: OutboxMessageID::new(r.message_id),
                        tx_id: r.tx_id,
                    },
                )
            })
            .collect())
    }

    fn list_consumption_boundaries(
        &self,
        transactional_catalog: &dill::Catalog,
    ) -> OutboxMessageConsumptionBoundariesStream<'_> {
        let transaction: Arc<TransactionRefT<Postgres>> = transactional_catalog.get_one().unwrap();

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
                    last_tx_id::text::bigint AS "last_tx_id!: i64"
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

    #[tracing::instrument(level = "debug", skip_all, fields(producer_name, consumer_name, boundary = ?boundary))]
    async fn mark_consumed(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_name: &str,
        consumer_name: &str,
        boundary: OutboxMessageBoundary,
    ) -> Result<(), InternalError> {
        let transaction: Arc<TransactionRefT<Postgres>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        let (last_tx_id, last_message_id) = (boundary.tx_id, boundary.message_id.into_inner());

        sqlx::query!(
            r#"
            INSERT INTO outbox_message_consumptions (consumer_name, producer_name, last_tx_id, last_consumed_message_id)
                VALUES ($1, $2, ($3)::text::xid8, $4)
                ON CONFLICT (consumer_name, producer_name) DO UPDATE
                SET
                    last_tx_id               = EXCLUDED.last_tx_id,
                    last_consumed_message_id = EXCLUDED.last_consumed_message_id
                WHERE (EXCLUDED.last_tx_id, EXCLUDED.last_consumed_message_id)
                    > (outbox_message_consumptions.last_tx_id, outbox_message_consumptions.last_consumed_message_id);
            "#,
            consumer_name,
            producer_name,
            last_tx_id.to_string(),
            last_message_id,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
