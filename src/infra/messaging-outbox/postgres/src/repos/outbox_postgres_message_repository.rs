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
use sqlx::postgres::PgRow;

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
                    occurred_on,
                    version
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
            let mut query_stream = query.map(|event_row: PgRow| {
                let version: i32 = event_row.get(4);
                OutboxMessage{
                    message_id: OutboxMessageID::new(event_row.get(0)),
                    producer_name: event_row.get(1),
                    content_json: event_row.get(2),
                    occurred_on: event_row.get(3),
                    version: version.try_into().unwrap(),
                }
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
