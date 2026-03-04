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
use internal_error::{InternalError, ResultIntoInternal};
use kamu_flow_system::{
    EventID,
    FlowSystemEvent,
    FlowSystemEventBridge,
    FlowSystemEventSourceType,
};
use kamu_messaging_outbox_sqlite::SqliteMessageStoreWakeupDetector;
use messaging_outbox::MessageStoreWakeupDetector;
use sqlx::Sqlite;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteFlowSystemEventBridge {
    wakeup_detector: SqliteMessageStoreWakeupDetector,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::scope(dill::scopes::Agnostic)]
#[dill::interface(dyn FlowSystemEventBridge)]
impl SqliteFlowSystemEventBridge {
    pub fn new(pool: Arc<sqlx::SqlitePool>) -> Self {
        Self {
            wakeup_detector: SqliteMessageStoreWakeupDetector::new(
                pool,
                "SELECT MAX(event_id) FROM flow_system_events",
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventBridge for SqliteFlowSystemEventBridge {
    /// Provides event store wakeup detector instance
    fn wakeup_detector(&self) -> &dyn MessageStoreWakeupDetector {
        &self.wakeup_detector
    }

    /// Fetch next batch for the given projector; order by global id.
    #[tracing::instrument(level = "debug", skip_all, fields(projector_name))]
    async fn fetch_next_batch(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        batch_size: usize,
    ) -> Result<Vec<FlowSystemEvent>, InternalError> {
        let transaction: Arc<TransactionRefT<Sqlite>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        let limit = i64::try_from(batch_size).unwrap();

        let rows = sqlx::query!(
            r#"
            SELECT
                e.event_id                AS "event_id!",
                e.source_stream           AS "source_stream!: String",
                e.event_time              AS "occurred_at!: DateTime<Utc>",
                e.event_payload           AS "event_payload!: serde_json::Value"
            FROM flow_system_events e
            WHERE e.event_id > COALESCE(
                (
                    SELECT last_event_id
                    FROM flow_system_projected_offsets
                    WHERE projector = $1
                ),
                0
            )
            ORDER BY e.event_id
            LIMIT $2;
            "#,
            projector_name,
            limit
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let events = rows
            .into_iter()
            .map(|r| FlowSystemEvent {
                event_id: EventID::new(r.event_id),
                tx_id: 0, // tx_id not tracked in SQLite impl
                source_type: match r.source_stream.as_str() {
                    "flows" => FlowSystemEventSourceType::Flow,
                    "triggers" => FlowSystemEventSourceType::FlowTrigger,
                    "configurations" => FlowSystemEventSourceType::FlowConfiguration,
                    _ => panic!("Unknown source_stream type"),
                },
                occurred_at: r.occurred_at,
                payload: r.event_payload,
            })
            .collect();

        Ok(events)
    }

    /// Mark these events as applied for this projector (idempotent).
    #[tracing::instrument(level = "debug", skip_all, fields(projector_name))]
    async fn mark_applied(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        event_ids_with_tx_ids: &[(EventID, i64)],
    ) -> Result<(), InternalError> {
        if event_ids_with_tx_ids.is_empty() {
            return Ok(());
        }

        let transaction: Arc<TransactionRefT<Sqlite>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        // Use the maximum event_id value from the array to update the boundary
        // Note: in SQLite we ignore the tx_ids as they are not needed
        let last_event_id = event_ids_with_tx_ids
            .iter()
            .map(|(event_id, _)| event_id.into_inner())
            .max()
            .unwrap();

        sqlx::query!(
            r#"
            INSERT INTO flow_system_projected_offsets(projector, last_event_id, updated_at)
                VALUES ($1, $2, datetime('now'))
                ON CONFLICT(projector) DO UPDATE
                    SET
                        last_event_id = MAX(last_event_id, excluded.last_event_id),
                        updated_at    = excluded.updated_at
            "#,
            projector_name,
            last_event_id
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
