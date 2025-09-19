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

use database_common::TransactionRefT;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_flow_system::{
    EventID,
    FlowSystemEvent,
    FlowSystemEventSourceType,
    FlowSystemEventStore,
    FlowSystemEventStoreWakeHint,
};
use sqlx::Sqlite;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteFlowSystemEventStore {
    pool: Arc<sqlx::SqlitePool>,
    max_seen_event_id: Mutex<EventID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn FlowSystemEventStore)]
impl SqliteFlowSystemEventStore {
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
impl FlowSystemEventStore for SqliteFlowSystemEventStore {
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<FlowSystemEventStoreWakeHint, InternalError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut poll_interval = min_debounce_interval;

        loop {
            // Check for new events
            if let Some(event_id) = self.check_for_new_events().await? {
                return Ok(FlowSystemEventStoreWakeHint::NewEvents {
                    upper_event_id_bound: event_id,
                });
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
    async fn fetch_next_batch(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        limit: usize,
        maybe_upper_event_id_bound: Option<EventID>,
    ) -> Result<Vec<FlowSystemEvent>, InternalError> {
        let transaction: Arc<TransactionRefT<Sqlite>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        let upper_bound = maybe_upper_event_id_bound
            .map(EventID::into_inner)
            .unwrap_or(i64::MAX);
        let limit = i64::try_from(limit).unwrap();

        let rows = sqlx::query!(
            r#"
            WITH next AS (
                SELECT e.event_id
                FROM flow_system_events e
                LEFT JOIN flow_system_projected_events a
                    ON a.projector = $1 AND a.event_id = e.event_id
                WHERE a.event_id IS NULL AND e.event_id <= $2
                ORDER BY e.event_id
                LIMIT $3
            )
                SELECT
                    e.event_id,
                    e.source_stream as "source_stream: String",
                    e.source_event_id,
                    e.occurred_at as "occurred_at: chrono::DateTime<chrono::Utc>",
                    e.inserted_at as "inserted_at: chrono::DateTime<chrono::Utc>"
                FROM flow_system_events e
                JOIN next n ON n.event_id = e.event_id
                ORDER BY e.event_id
                "#,
            projector_name,
            upper_bound,
            limit
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let events = rows
            .into_iter()
            .map(|r| FlowSystemEvent {
                event_id: EventID::new(r.event_id),
                source_type: match r.source_stream.as_str() {
                    "flows" => FlowSystemEventSourceType::Flow,
                    "triggers" => FlowSystemEventSourceType::FlowTrigger,
                    _ => FlowSystemEventSourceType::FlowConfiguration,
                },
                source_event_id: EventID::new(r.source_event_id),
                occurred_at: r.occurred_at,
                inserted_at: r.inserted_at,
            })
            .collect();

        Ok(events)
    }

    /// Mark these events as applied for this projector (idempotent).
    async fn mark_applied(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        event_ids: &[EventID],
    ) -> Result<(), InternalError> {
        if event_ids.is_empty() {
            return Ok(());
        }

        let transaction: Arc<TransactionRefT<Sqlite>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        // Convert event IDs to JSON array for SQLite
        let event_id_values: Vec<i64> = event_ids.iter().map(|id| id.into_inner()).collect();
        let json_array = serde_json::to_string(&event_id_values).int_err()?;

        // Use json_each for bulk insert in SQLite
        sqlx::query!(
            r#"
            INSERT OR IGNORE INTO flow_system_projected_events(projector, event_id)
                SELECT $1, CAST(value AS INTEGER)
                FROM json_each($2)
            "#,
            projector_name,
            json_array
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
