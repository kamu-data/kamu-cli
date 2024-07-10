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

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresOutboxMessageRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[component(pub)]
#[interface(dyn OutboxMessageRepository)]
impl PostgresOutboxMessageRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

#[async_trait::async_trait]
impl OutboxMessageRepository for PostgresOutboxMessageRepository {
    async fn push_message(&self, message: NewOutboxMessage) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await.int_err()?;

        sqlx::query!(
            r#"
                INSERT INTO outbox_messages (producer_name, content_json, occurred_on)
                    VALUES ($1, $2, $3)
            "#,
            message.producer_name,
            &message.content_json,
            message.occurred_on
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn get_producer_messages(
        &self,
        producer_name: &str,
        above_id: OutboxMessageID,
        batch_size: usize,
    ) -> Result<OutboxMessageStream, InternalError> {
        let mut tr = self.transaction.lock().await;

        let producer_name = producer_name.to_string();

        Ok(Box::pin(async_stream::stream! {
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query_as!(
                OutboxMessage,
                r#"
                    SELECT
                        message_id,
                        producer_name,
                        content_json,
                        occurred_on
                    FROM outbox_messages
                    WHERE producer_name = $1 and message_id > $2
                    ORDER BY message_id
                    LIMIT $3
                "#,
                producer_name,
                above_id.into_inner(),
                i64::try_from(batch_size).unwrap(),
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(message) = query_stream.try_next().await? {
                yield Ok(message);
            }
        }))
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
                    max(message_id) as max_message_id
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
                    r.producer_name,
                    OutboxMessageID::new(r.max_message_id.unwrap_or(0)),
                )
            })
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
