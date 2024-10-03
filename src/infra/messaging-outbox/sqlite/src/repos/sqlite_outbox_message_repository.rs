// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{TransactionRef, TransactionRefT};
use dill::{component, interface};
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use sqlx::sqlite::SqliteRow;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteOutboxMessageRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

#[component(pub)]
#[interface(dyn OutboxMessageRepository)]
impl SqliteOutboxMessageRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

#[async_trait::async_trait]
impl OutboxMessageRepository for SqliteOutboxMessageRepository {
    async fn push_message(&self, message: NewOutboxMessage) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let message_content_json = message.content_json;

        sqlx::query!(
            r#"
                INSERT INTO outbox_messages (producer_name, content_json, occurred_on)
                    VALUES ($1, $2, $3)
            "#,
            message.producer_name,
            message_content_json,
            message.occurred_on
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
        let producer_filters = if above_boundaries_by_producer.is_empty() {
            "true".to_string()
        } else {
            above_boundaries_by_producer
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    format!(
                        "producer_name = ${} AND message_id > ${}",
                        i * 2 + 2, // $2, $4, $6, ...
                        i * 2 + 3, // $3, $5, $7, ...
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR ")
        };

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let query_str = format!(
                r#"
                SELECT
                    message_id,
                    producer_name,
                    content_json,
                    occurred_on
                FROM outbox_messages
                WHERE {producer_filters}
                ORDER BY message_id
                LIMIT $1
                "#,
            );

            let mut query = sqlx::query(&query_str)
                .bind(i64::try_from(batch_size).unwrap());

            for (producer_name, above_id) in above_boundaries_by_producer {
                query = query.bind(producer_name).bind(above_id.into_inner());
            }


            use sqlx::Row;
            let mut query_stream = query.try_map(|event_row: SqliteRow| {
                Ok(OutboxMessage {
                    message_id: OutboxMessageID::new(event_row.get(0)),
                    producer_name: event_row.get(1),
                    content_json: event_row.get(2),
                    occurred_on: event_row.get(3),
                })
            })
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(message) = query_stream.try_next().await? {
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
        .map_err(ErrorIntoInternal::int_err)?;

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
