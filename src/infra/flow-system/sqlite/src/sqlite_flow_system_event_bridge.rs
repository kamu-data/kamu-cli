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
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<FlowSystemEventStoreWakeHint, InternalError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut poll_interval = min_debounce_interval;

        loop {
            // Check for new events
            if let Some(max_event_id) = self.check_for_new_events().await? {
                return Ok(FlowSystemEventStoreWakeHint::NewEvents {
                    lower_event_id_bound: EventID::new(0), // can't provide this hint in SQLite
                    upper_event_id_bound: max_event_id,
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
        batch_size: usize,
        _loopback_offset: usize, // Ignored by SQLite implementation
        _maybe_event_id_bounds_hint: Option<(EventID, EventID)>, // Ignored by SQLite implementation
    ) -> Result<Vec<FlowSystemEvent>, InternalError> {
        let transaction: Arc<TransactionRefT<Sqlite>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        let limit = i64::try_from(batch_size).unwrap();

        let rows = sqlx::query!(
            r#"
            WITH next AS (
                SELECT e.*
                FROM flow_system_events AS e
                WHERE e.event_id >
                    COALESCE(
                        (
                            SELECT last_event_id FROM flow_system_projected_offsets WHERE projector = $1
                        ),
                        0
                    )
                ORDER BY e.event_id
                LIMIT $2
            ),
            merged as (
                SELECT
                    fse.event_id,
                    fse.source_stream,
                    fse.source_event_id,
                    fse.occurred_at,
                    fse.inserted_at,
                    fe.event_payload
                FROM flow_system_events fse
                JOIN next n
                    ON n.event_id = fse.event_id
                JOIN flow_events fe
                    ON fse.source_stream = 'flows' AND fe.event_id = fse.source_event_id

                UNION ALL

                SELECT
                    fse.event_id,
                    fse.source_stream,
                    fse.source_event_id,
                    fse.occurred_at,
                    fse.inserted_at,
                    fte.event_payload
                FROM flow_system_events fse
                JOIN next n
                    ON n.event_id = fse.event_id
                JOIN flow_trigger_events fte
                    ON fse.source_stream = 'triggers' AND fte.event_id = fse.source_event_id

                UNION ALL

                SELECT
                    fse.event_id,
                    fse.source_stream,
                    fse.source_event_id,
                    fse.occurred_at,
                    fse.inserted_at,
                    fce.event_payload
                FROM flow_system_events fse
                JOIN next n
                    ON n.event_id = fse.event_id
                JOIN flow_configuration_events fce
                    ON fse.source_stream = 'configurations' AND fce.event_id = fse.source_event_id
            )
            SELECT
                event_id as "event_id!",
                source_stream as "source_stream!: String",
                source_event_id as "source_event_id!",
                occurred_at as "occurred_at!: DateTime<Utc>",
                inserted_at as "inserted_at!: DateTime<Utc>",
                event_payload as "event_payload!: serde_json::Value"
            FROM merged
            ORDER BY event_id
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
                source_type: match r.source_stream.as_str() {
                    "flows" => FlowSystemEventSourceType::Flow,
                    "triggers" => FlowSystemEventSourceType::FlowTrigger,
                    _ => FlowSystemEventSourceType::FlowConfiguration,
                },
                source_event_id: EventID::new(r.source_event_id),
                occurred_at: r.occurred_at,
                inserted_at: r.inserted_at,
                payload: r.event_payload,
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

        // Use the maximum event_id value from the array to update the boundary
        let last_event_id = event_ids.iter().map(|id| id.into_inner()).max().unwrap();

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
