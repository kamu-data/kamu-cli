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
pub struct SqliteOutboxMessageRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

#[async_trait::async_trait]
impl OutboxMessageRepository for SqliteOutboxMessageRepository {
    async fn push_message(&self, message: NewOutboxMessage) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

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

    fn get_messages(
        &self,
        above_boundaries_by_producer: Vec<(String, OutboxMessageID)>,
        batch_size: usize,
    ) -> OutboxMessageStream<'_> {
        let unfiltered = above_boundaries_by_producer.is_empty();

        let json_bounds = serde_json::to_string(
            &above_boundaries_by_producer
                .into_iter()
                .map(|(p, id)| serde_json::json!({"p": p, "id": id.into_inner()}))
                .collect::<Vec<_>>(),
        )
        .unwrap();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let batch_size = i64::try_from(batch_size).unwrap();

            let mut stream = if unfiltered {
                sqlx::query_as!(
                    OutboxMessageRow,
                    r#"
                    SELECT
                        message_id,
                        producer_name,
                        content_json as "content_json: _",
                        occurred_on as "occurred_on: _",
                        version as "version!"
                    FROM outbox_messages
                    ORDER BY message_id
                    LIMIT $1
                    "#,
                    batch_size,
                ).fetch(connection_mut)
            } else {
                sqlx::query_as!(
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
                ).fetch(connection_mut)
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
                    OutboxMessageID::new(r.max_message_id),
                )
            })
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
