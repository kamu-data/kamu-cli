// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
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
use sqlx::Postgres;
use sqlx::postgres::PgListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const NOTIFY_CHANNEL_NAME: &str = "flow_system_events_ready";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresFlowSystemEventBridge {
    pool: Arc<sqlx::PgPool>,
    listener: tokio::sync::Mutex<Option<PgListener>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn FlowSystemEventBridge)]
impl PostgresFlowSystemEventBridge {
    pub fn new(pool: Arc<sqlx::PgPool>) -> Self {
        Self {
            pool,
            listener: tokio::sync::Mutex::new(None),
        }
    }

    async fn try_create_listener(&self) -> Option<PgListener> {
        match PgListener::connect_with(&self.pool).await {
            Ok(mut l) => match l.listen(NOTIFY_CHANNEL_NAME).await {
                Ok(_) => Some(l),
                Err(e) => {
                    tracing::error!(
                        error = ?e,
                        error_msg = %e,
                        "Failed to listen on channel '{NOTIFY_CHANNEL_NAME}'",
                    );
                    None
                }
            },
            Err(e) => {
                tracing::error!(
                    error = ?e,
                    error_msg = %e,
                    "Failed to connect to PgListener"
                );
                None
            }
        }
    }

    fn calculate_retry_delay(
        &self,
        deadline: tokio::time::Instant,
        min_debounce_interval: Duration,
    ) -> Duration {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        std::cmp::min(min_debounce_interval, remaining)
    }

    async fn buffer_notifications(
        &self,
        listener: &mut PgListener,
        initial_payload: Option<PgNotifyPayload>,
        elapsed: Duration,
        remaining_timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Option<(EventID, EventID)> {
        use tokio::time::{Instant, timeout};

        // Seed from the first payload (if any)
        let mut bounds = initial_payload.map(|p| (p.min, p.max));

        // Only buffer if first NOTIFY arrived "too fast"
        if elapsed < min_debounce_interval {
            // Calculate how much time we have left in the debounce window
            // also respect the outer wait_wake timeout
            let budget = std::cmp::min(
                min_debounce_interval.saturating_sub(elapsed),
                remaining_timeout,
            );
            if !budget.is_zero() {
                let end = Instant::now() + budget;
                loop {
                    let remaining = end.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }

                    match timeout(remaining, listener.recv()).await {
                        Ok(Ok(n)) => {
                            if let Ok(p) = serde_json::from_str::<PgNotifyPayload>(n.payload()) {
                                bounds = Some(bounds.map_or((p.min, p.max), |old| {
                                    (old.0.min(p.min), old.1.max(p.max))
                                }));
                            }
                            // keep draining until time budget is up
                        }
                        Ok(Err(_)) | Err(_) => break, /* connection error → caller handles
                                                       * reconnect, or time budget elapsed */
                    }
                }
            }
        }

        bounds
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventBridge for PostgresFlowSystemEventBridge {
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<FlowSystemEventStoreWakeHint, InternalError> {
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            // Try to get or create a listener
            let mut listener = match self.listener.lock().await.take() {
                Some(existing_listener) => existing_listener,
                None => {
                    // No listener available, try to create one
                    match self.try_create_listener().await {
                        Some(new_listener) => new_listener,
                        None => {
                            // Failed to create listener, wait for min_debounce_interval and then
                            // sleep for remaining timeout if any
                            let delay = self.calculate_retry_delay(deadline, min_debounce_interval);
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }

                            // Check if we still have time left after the delay
                            let remaining_after_delay =
                                deadline.saturating_duration_since(tokio::time::Instant::now());
                            if remaining_after_delay.is_zero() {
                                return Ok(FlowSystemEventStoreWakeHint::Timeout);
                            }

                            // Continue to next iteration to try again
                            continue;
                        }
                    }
                }
            };

            // Calculate remaining timeout for this iteration
            let remaining_timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining_timeout.is_zero() {
                // Timeout exceeded, put listener back and return
                *self.listener.lock().await = Some(listener);
                return Ok(FlowSystemEventStoreWakeHint::Timeout);
            }

            // Wait for notification with remaining timeout
            match tokio::time::timeout(remaining_timeout, listener.recv()).await {
                // Got a NOTIFY — check if we should buffer for more events
                Ok(Ok(notification)) => {
                    // Parse initial payload
                    let initial_payload: Option<PgNotifyPayload> =
                        serde_json::from_str(notification.payload()).ok();

                    // Calculate how long we've been waiting since the start
                    let elapsed = deadline
                        .saturating_duration_since(tokio::time::Instant::now())
                        .saturating_sub(remaining_timeout);

                    // Buffer additional notifications if appropriate
                    let bounds = self
                        .buffer_notifications(
                            &mut listener,
                            initial_payload,
                            elapsed,
                            remaining_timeout,
                            min_debounce_interval,
                        )
                        .await;

                    // Stash the listener back
                    *self.listener.lock().await = Some(listener);

                    // Return the (potentially updated) bounds
                    return match bounds {
                        Some((min_event_id, max_event_id)) => {
                            Ok(FlowSystemEventStoreWakeHint::NewEvents {
                                lower_event_id_bound: min_event_id,
                                upper_event_id_bound: max_event_id,
                            })
                        }
                        None => Ok(FlowSystemEventStoreWakeHint::Timeout),
                    };
                }

                // Socket/conn error — drop listener and try to reconnect after delay
                Ok(Err(conn_err)) => {
                    tracing::error!(
                        error = ?conn_err,
                        error_msg = %conn_err,
                        "PgListener connection error, will attempt to reconnect after delay",
                    );

                    // Wait for min_debounce_interval before retrying, if we have remaining time
                    let delay = self.calculate_retry_delay(deadline, min_debounce_interval);
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    // Loop will try to reconnect
                }

                // Timed out waiting — stash listener back and return
                Err(_elapsed) => {
                    *self.listener.lock().await = Some(listener);
                    return Ok(FlowSystemEventStoreWakeHint::Timeout);
                }
            }
        }
    }

    /// Fetch next batch for the given projector; order by global id.
    async fn fetch_next_batch(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        batch_size: usize,
    ) -> Result<Vec<FlowSystemEvent>, InternalError> {
        let transaction: Arc<TransactionRefT<Postgres>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        let rows = sqlx::query!(
            r#"
            WITH projected_offsets AS (
                SELECT
                    COALESCE(
                        ( SELECT last_tx_id FROM flow_system_projected_offsets WHERE projector = $1),
                        '0'::xid8
                    ) AS last_tx_id,
                    COALESCE(
                        ( SELECT last_event_id FROM flow_system_projected_offsets WHERE projector = $1),
                        0::bigint
                    ) AS last_event_id
            )

            SELECT
                event_id            AS "event_id!",
                tx_id::text::bigint AS "tx_id!: i64",
                source_stream       AS "source_stream!: String",
                event_time          AS "occurred_at!: DateTime<Utc>",
                event_payload       AS "event_payload!"
            FROM flow_system_events e, projected_offsets
            WHERE
                -- Ignore rows from txns that might still be in flight ("Usain Bolt")
                e.tx_id < pg_snapshot_xmin(pg_current_snapshot()) AND (
                    (
                        -- Same transaction as last projected event, but higher event id
                        e.tx_id = projected_offsets.last_tx_id AND
                        e.event_id > projected_offsets.last_event_id
                    ) OR
                    (
                        -- Later transaction than last projected event
                        e.tx_id > projected_offsets.last_tx_id
                    )
                )
            ORDER BY e.tx_id ASC, e.event_id ASC
            LIMIT $2
            "#,
            projector_name,
            i64::try_from(batch_size).unwrap()
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let events: Vec<FlowSystemEvent> = rows
            .into_iter()
            .map(|r| FlowSystemEvent {
                event_id: EventID::new(r.event_id),
                tx_id: r.tx_id,
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
    async fn mark_applied(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        event_ids_with_tx_ids: &[(EventID, i64)],
    ) -> Result<(), InternalError> {
        if event_ids_with_tx_ids.is_empty() {
            return Ok(());
        }

        let transaction: Arc<TransactionRefT<Postgres>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        let (last_event_id, last_tx_id) = event_ids_with_tx_ids
            .iter()
            .map(|(event_id, tx_id)| (event_id.into_inner(), *tx_id))
            .max()
            .unwrap();

        sqlx::query!(
            r#"
            INSERT INTO flow_system_projected_offsets (projector, last_tx_id, last_event_id, updated_at)
                VALUES ($1, ($2)::text::xid8, $3, now())
                ON CONFLICT (projector) DO UPDATE
                SET
                    last_tx_id    = EXCLUDED.last_tx_id,
                    last_event_id = EXCLUDED.last_event_id,
                    updated_at    = now()
                WHERE (EXCLUDED.last_tx_id, EXCLUDED.last_event_id)
                    > (flow_system_projected_offsets.last_tx_id, flow_system_projected_offsets.last_event_id);
            "#,
            projector_name,
            last_tx_id.to_string(),
            last_event_id,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize)]
struct PgNotifyPayload {
    #[allow(dead_code)]
    min: EventID,
    max: EventID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
