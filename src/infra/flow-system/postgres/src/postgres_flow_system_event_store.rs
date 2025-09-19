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

use database_common::TransactionRefT;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_flow_system::{
    EventID,
    FlowSystemEvent,
    FlowSystemEventSourceType,
    FlowSystemEventStore,
    FlowSystemEventStoreWakeHint,
};
use sqlx::Postgres;
use sqlx::postgres::PgListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const NOTIFY_CHANNEL_NAME: &str = "flow_system_events_ready";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresFlowSystemEventStore {
    pool: Arc<sqlx::PgPool>,
    listener: tokio::sync::Mutex<Option<PgListener>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn FlowSystemEventStore)]
impl PostgresFlowSystemEventStore {
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
    ) -> Option<EventID> {
        use tokio::time::{Instant, timeout};

        // Seed from the first payload (if any)
        let mut upper_bound = initial_payload.map(|p| p.max);

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
                                upper_bound = Some(upper_bound.map_or(p.max, |old| old.max(p.max)));
                            }
                            // keep draining until time budget is up
                        }
                        Ok(Err(_)) | Err(_) => break, /* connection error → caller handles
                                                       * reconnect, or time budget elapsed */
                    }
                }
            }
        }

        upper_bound
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventStore for PostgresFlowSystemEventStore {
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
                    let upper_bound = self
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

                    // Return the (potentially updated) upper bound
                    return match upper_bound {
                        Some(event_id) => Ok(FlowSystemEventStoreWakeHint::NewEvents {
                            upper_event_id_bound: event_id,
                        }),
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
        limit: usize,
        maybe_upper_event_id_bound: Option<EventID>,
    ) -> Result<Vec<FlowSystemEvent>, InternalError> {
        let transaction: Arc<TransactionRefT<Postgres>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

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
                    e.occurred_at,
                    e.inserted_at
                FROM flow_system_events e
                JOIN next n ON n.event_id = e.event_id
                ORDER BY e.event_id
                "#,
            projector_name,
            maybe_upper_event_id_bound
                .map(EventID::into_inner)
                .unwrap_or(i64::MAX),
            i64::try_from(limit).unwrap()
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

        let transaction: Arc<TransactionRefT<Postgres>> = transaction_catalog.get_one().unwrap();

        let mut guard = transaction.lock().await;
        let connection_mut = guard.connection_mut().await?;

        // Convert event IDs to a vector of i64 for bulk insert
        let event_id_values: Vec<i64> = event_ids.iter().map(|id| id.into_inner()).collect();

        sqlx::query!(
            r#"
            INSERT INTO flow_system_projected_events(projector, event_id)
                SELECT $1, UNNEST($2::BIGINT[]) ON CONFLICT DO NOTHING
            "#,
            projector_name,
            &event_id_values
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
