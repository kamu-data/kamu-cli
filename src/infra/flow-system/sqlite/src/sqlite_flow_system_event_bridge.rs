// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use database_common::TransactionRefT;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_flow_system::{
    EventID,
    FlowSystemEvent,
    FlowSystemEventBridge,
    FlowSystemEventSourceType,
    FlowSystemEventStoreWakeHint,
};
use sqlx::Sqlite;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteFlowSystemEventBridge {
    pool: Arc<sqlx::SqlitePool>,
    max_seen_event_id: Mutex<EventID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn FlowSystemEventBridge)]
impl SqliteFlowSystemEventBridge {
    pub fn new(pool: Arc<sqlx::SqlitePool>) -> Self {
        Self {
            pool,
            max_seen_event_id: Mutex::new(EventID::new(0)),
        }
    }

    async fn check_for_new_events(&self) -> Result<Option<EventID>, InternalError> {
        let max_present_event_id = {
            let (max_present_event_id,): (Option<i64>,) =
                sqlx::query_as("SELECT MAX(event_id) FROM flow_system_events")
                    .fetch_one(self.pool.as_ref())
                    .await
                    .unwrap_or((None,));

            Ok(EventID::new(max_present_event_id.unwrap_or_default()))
        }?;

        let mut max_seen_event_id = self.max_seen_event_id.lock().unwrap();
        if max_present_event_id > *max_seen_event_id {
            *max_seen_event_id = max_present_event_id;
            Ok(Some(max_present_event_id))
        } else {
            Ok(None)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventBridge for SqliteFlowSystemEventBridge {
    #[tracing::instrument(level = "debug", skip_all, fields(timeout, min_debounce_interval))]
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<FlowSystemEventStoreWakeHint, InternalError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut poll_interval = min_debounce_interval;

        loop {
            // Check for new events
            if let Some(_max_event_id) = self.check_for_new_events().await? {
                return Ok(FlowSystemEventStoreWakeHint::NewEvents);
            }

            // Calculate remaining time
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                // Timeout elapsed, no new work
                return Ok(FlowSystemEventStoreWakeHint::Timeout);
            }

            // Sleep for the shorter of poll_interval or remaining time
            let sleep_duration = std::cmp::min(poll_interval, remaining);
            tokio::time::sleep(sleep_duration).await;

            // Increase poll interval for next iteration
            // (exponential backoff with max limit of timeout)
            poll_interval = std::cmp::min(poll_interval * 2, timeout);
        }
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
