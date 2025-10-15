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
        above_boundaries_by_producer: Vec<(String, OutboxMessageID)>,
        batch_size: usize,
    ) -> OutboxMessageStream {
        let (producers, above_ids): (Vec<String>, Vec<i64>) = above_boundaries_by_producer
            .into_iter()
            .map(|(p, v)| (p, v.into_inner()))
            .unzip();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut stream = if producers.is_empty() {
                sqlx::query_as!(
                    OutboxMessageRow,
                    r#"
                    SELECT
                        message_id,
                        producer_name,
                        content_json,
                        occurred_on,
                        version as "version!"
                    FROM outbox_messages
                    ORDER BY message_id
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
                        FROM UNNEST($1::text[], $2::bigint[]) AS b(producer_name, above_id)
                    )
                    SELECT
                        m.message_id,
                        m.producer_name,
                        m.content_json,
                        m.occurred_on,
                        m.version as "version!"
                    FROM outbox_messages AS m
                    JOIN bounds AS b
                        ON m.producer_name = b.producer_name AND m.message_id > b.above_id
                    ORDER BY m.message_id
                    LIMIT $3
                    "#,
                    &producers,
                    &above_ids,
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

    async fn get_latest_message_ids_by_producer(
        &self,
    ) -> Result<Vec<(String, OutboxMessageID)>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let records = sqlx::query!(
            r#"
            SELECT
                producer_name,
                max(message_id) AS max_message_id
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
                    r.producer_name,
                    OutboxMessageID::new(r.max_message_id.unwrap_or(0)),
                )
            })
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
