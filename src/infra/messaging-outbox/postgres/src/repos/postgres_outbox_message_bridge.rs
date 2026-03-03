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
        let transaction: Arc<TransactionRefT<Postgres>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        let rows = sqlx::query!(
            r#"
            WITH consumed_offsets AS (
                SELECT
                    COALESCE(
                        ( SELECT last_tx_id FROM outbox_message_consumptions WHERE consumer_name = $1 AND producer_name = $2),
                        '0'::xid8
                    ) AS last_tx_id,
                    COALESCE(
                        ( SELECT last_consumed_message_id FROM outbox_message_consumptions WHERE consumer_name = $1 AND producer_name = $2),
                        0::bigint
                    ) AS last_message_id
            )

            SELECT
                message_id          AS "message_id!",
                tx_id::text::bigint AS "tx_id!: i64",
                producer_name       AS "producer_name!: String",
                occurred_on         AS "occurred_on!: DateTime<Utc>",
                content_json        AS "content_json!",
                version             AS "version!"
            FROM outbox_messages o, consumed_offsets
            WHERE
                -- Ignore rows from txns that might still be in flight ("Usain Bolt")
                o.tx_id < pg_snapshot_xmin(pg_current_snapshot()) AND (
                    (
                        -- Same transaction as last projected event, but higher message id
                        o.tx_id = consumed_offsets.last_tx_id AND
                        o.message_id > consumed_offsets.last_message_id
                    ) OR
                    (
                        -- Later transaction than last projected event
                        o.tx_id > consumed_offsets.last_tx_id
                    )
                )
            ORDER BY o.tx_id ASC, o.message_id ASC
            LIMIT $3
            "#,
            consumer_name,
            producer_name,
            i64::try_from(batch_size).unwrap()
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let events: Vec<OutboxMessage> = rows
            .into_iter()
            .map(|r| OutboxMessage {
                message_id: OutboxMessageID::new(r.message_id),
                tx_id: r.tx_id,
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
