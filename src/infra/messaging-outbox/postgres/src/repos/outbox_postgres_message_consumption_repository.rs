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
use internal_error::{ErrorIntoInternal, InternalError};

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresOutboxMessageConsumptionRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[component(pub)]
#[interface(dyn OutboxMessageConsumptionRepository)]
impl PostgresOutboxMessageConsumptionRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

#[async_trait::async_trait]
impl OutboxMessageConsumptionRepository for PostgresOutboxMessageConsumptionRepository {
    fn list_consumption_boundaries(&self) -> OutboxMessageConsumptionBoundariesStream {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query_as!(
                OutboxMessageConsumptionBoundary,
                r#"
                    SELECT
                        consumer_name, producer_name, last_consumed_message_id
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

    async fn find_consumption_boundary(
        &self,
        consumer_name: &str,
        producer_name: &str,
    ) -> Result<Option<OutboxMessageConsumptionBoundary>, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        sqlx::query_as!(
            OutboxMessageConsumptionBoundary,
            r#"
                SELECT
                    consumer_name, producer_name, last_consumed_message_id
                FROM outbox_message_consumptions
                WHERE consumer_name = $1 and producer_name = $2
            "#,
            consumer_name,
            producer_name
        )
        .fetch_optional(connection_mut)
        .await
        .map_err(ErrorIntoInternal::int_err)
    }

    async fn create_consumption_boundary(
        &self,
        boundary: OutboxMessageConsumptionBoundary,
    ) -> Result<(), CreateConsumptionBoundaryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(|e| CreateConsumptionBoundaryError::Internal(e.int_err()))?;

        sqlx::query!(
            r#"
                INSERT INTO outbox_message_consumptions (consumer_name, producer_name, last_consumed_message_id)
                    VALUES ($1, $2, $3)
            "#,
            boundary.consumer_name,
            boundary.producer_name,
            boundary.last_consumed_message_id.into_inner(),
        )
        .execute(connection_mut)
        .await
        .map_err(|e| {
            if let sqlx::Error::Database(e) = &e
                && e.is_unique_violation()
            {
                CreateConsumptionBoundaryError::DuplicateConsumptionBoundary(
                    DuplicateConsumptionBoundaryError {
                        consumer_name: boundary.consumer_name,
                        producer_name: boundary.producer_name,
                    },
                )
            } else {
                CreateConsumptionBoundaryError::Internal(e.int_err())
            }
        })?;

        Ok(())
    }

    async fn update_consumption_boundary(
        &self,
        boundary: OutboxMessageConsumptionBoundary,
    ) -> Result<(), UpdateConsumptionBoundaryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(|e| UpdateConsumptionBoundaryError::Internal(e.int_err()))?;

        let res = sqlx::query!(
            r#"
                UPDATE outbox_message_consumptions SET last_consumed_message_id = $3
                    WHERE consumer_name = $1 and producer_name = $2 and last_consumed_message_id < $3
            "#,
            boundary.consumer_name,
            boundary.producer_name,
            boundary.last_consumed_message_id.into_inner(),
        )
        .execute(connection_mut)
        .await
        .map_err(|e| UpdateConsumptionBoundaryError::Internal(e.int_err()))?;

        if res.rows_affected() != 1 {
            Err(UpdateConsumptionBoundaryError::ConsumptionBoundaryNotFound(
                ConsumptionBoundaryNotFoundError {
                    consumer_name: boundary.consumer_name,
                    producer_name: boundary.producer_name,
                },
            ))
        } else {
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
