// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::TransactionRefT;
use dill::{component, interface};
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn OutboxMessageRepository)]
pub struct PostgresOutboxMessageRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[async_trait::async_trait]
impl OutboxMessageRepository for PostgresOutboxMessageRepository {
    async fn push_message(&self, message: NewOutboxMessage) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;
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

    fn get_messages(
        &self,
        above_boundaries_by_producer: Vec<(String, OutboxMessageBoundary)>,
        batch_size: usize,
    ) -> OutboxMessageStream<'_> {
        let mut producers = Vec::with_capacity(above_boundaries_by_producer.len());
        let mut above_tx_ids = Vec::with_capacity(above_boundaries_by_producer.len());
        let mut above_message_ids = Vec::with_capacity(above_boundaries_by_producer.len());

        for (producer_name, boundary) in above_boundaries_by_producer {
            producers.push(producer_name);
            above_tx_ids.push(boundary.tx_id);
            above_message_ids.push(boundary.message_id.into_inner());
        }

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut stream = if producers.is_empty() {
                sqlx::query_as!(
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
                    ORDER BY tx_id, message_id
                    LIMIT $1
                    "#,
                    i64::try_from(batch_size).unwrap()
                )
                .fetch(connection_mut)
            } else {
                sqlx::query_as!(
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
                    ORDER BY m.tx_id, m.message_id
                    LIMIT $4
                    "#,
                    &producers,
                    &above_tx_ids,
                    &above_message_ids,
                    i64::try_from(batch_size).unwrap()
                )
                .fetch(connection_mut)
            };

            while let Some(message_row) = stream.try_next().await.map_err(ErrorIntoInternal::int_err)? {
                let message: OutboxMessage = message_row.into();
                yield Ok(message);
            }
        })
    }

    async fn get_latest_message_boundaries_by_producer(
        &self,
    ) -> Result<Vec<(String, OutboxMessageBoundary)>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
