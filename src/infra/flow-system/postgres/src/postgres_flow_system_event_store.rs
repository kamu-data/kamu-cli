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

use internal_error::InternalError;
use kamu_flow_system::{EventID, FlowSystemEventStore, FlowSystemEventStoreWakeHint};
use sqlx::postgres::PgListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const NOTIFY_CHANNEL_NAME: &str = "flow_system_events_ready";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresFlowSystemEventStore {
    pool: Arc<sqlx::PgPool>,
    listener: tokio::sync::Mutex<Option<PgListener>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl PostgresFlowSystemEventStore {
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
        min_polling_interval: Duration,
    ) -> Duration {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        std::cmp::min(min_polling_interval, remaining)
    }

    async fn buffer_notifications(
        &self,
        listener: &mut PgListener,
        initial_payload: Option<serde_json::Value>,
        elapsed: Duration,
        remaining_timeout: Duration,
        min_polling_interval: Duration,
    ) -> Option<EventID> {
        #[derive(Debug, serde::Deserialize)]
        struct Payload {
            #[allow(dead_code)]
            min: i64,
            max: i64,
        }

        let mut upper_bound = initial_payload
            .and_then(|p| serde_json::from_value::<Payload>(p).ok())
            .map(|p| EventID::new(p.max));

        // If we got notification quickly (before min_polling_interval),
        // try to buffer for a bit more to accumulate additional events
        if elapsed < min_polling_interval {
            let buffer_timeout = min_polling_interval.saturating_sub(elapsed);
            let remaining_after_buffer = remaining_timeout.saturating_sub(buffer_timeout);

            // Only buffer if we have enough remaining time
            if !remaining_after_buffer.is_zero() {
                // Try to collect more notifications during buffering period
                while let Ok(Ok(additional_notification)) =
                    tokio::time::timeout(buffer_timeout, listener.recv()).await
                {
                    if let Ok(additional_payload) =
                        serde_json::from_str::<Payload>(additional_notification.payload())
                    {
                        // Update upper bound to the latest max value
                        upper_bound = Some(EventID::new(additional_payload.max));
                    }
                    // Continue collecting until buffer timeout
                }
            }
        }

        upper_bound
    }
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventStore for PostgresFlowSystemEventStore {
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_polling_interval: Duration,
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
                            // Failed to create listener, wait for min_listening_timeout and then
                            // sleep for remaining timeout if any
                            let delay = self.calculate_retry_delay(deadline, min_polling_interval);
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }

                            // Check if we still have time left after the delay
                            let remaining_after_delay =
                                deadline.saturating_duration_since(tokio::time::Instant::now());
                            if remaining_after_delay.is_zero() {
                                return Ok(FlowSystemEventStoreWakeHint::default());
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
                return Ok(FlowSystemEventStoreWakeHint::default());
            }

            // Wait for notification with remaining timeout
            match tokio::time::timeout(remaining_timeout, listener.recv()).await {
                // Got a NOTIFY — check if we should buffer for more events
                Ok(Ok(notification)) => {
                    // Parse initial payload
                    let initial_payload = serde_json::from_str(notification.payload()).ok();

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
                            min_polling_interval,
                        )
                        .await;

                    // Stash the listener back
                    *self.listener.lock().await = Some(listener);

                    // Return the (potentially updated) upper bound
                    return Ok(FlowSystemEventStoreWakeHint {
                        upper_event_id_bound: upper_bound,
                    });
                }

                // Socket/conn error — drop listener and try to reconnect after delay
                Ok(Err(conn_err)) => {
                    tracing::error!(
                        error = ?conn_err,
                        error_msg = %conn_err,
                        "PgListener connection error, will attempt to reconnect after delay",
                    );

                    // Wait for min_listening_timeout before retrying, if we have remaining time
                    let delay = self.calculate_retry_delay(deadline, min_polling_interval);
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    // Loop will try to reconnect
                }

                // Timed out waiting — stash listener back and return
                Err(_elapsed) => {
                    *self.listener.lock().await = Some(listener);
                    return Ok(FlowSystemEventStoreWakeHint::default());
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
