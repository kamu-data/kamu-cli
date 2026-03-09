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
use messaging_outbox::{MessageStoreWakeHint, MessageStoreWakeupDetector};
use sqlx::postgres::PgListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresMessageStoreWakeupDetector {
    pool: Arc<sqlx::PgPool>,
    listener: tokio::sync::Mutex<Option<PgListener>>,
    channel_name: &'static str,
}

impl PostgresMessageStoreWakeupDetector {
    pub fn new(pool: Arc<sqlx::PgPool>, channel_name: &'static str) -> Self {
        Self {
            pool,
            listener: Default::default(),
            channel_name,
        }
    }

    async fn try_create_listener(&self) -> Option<PgListener> {
        match PgListener::connect_with(&self.pool).await {
            Ok(mut l) => match l.listen(self.channel_name).await {
                Ok(_) => Some(l),
                Err(e) => {
                    tracing::error!(
                        error = ?e,
                        error_msg = %e,
                        "Failed to listen on channel '{}'", self.channel_name,
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageStoreWakeupDetector for PostgresMessageStoreWakeupDetector {
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<MessageStoreWakeHint, InternalError> {
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
                                return Ok(MessageStoreWakeHint::Timeout);
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
                return Ok(MessageStoreWakeHint::Timeout);
            }

            // Wait for notification with remaining timeout
            match tokio::time::timeout(remaining_timeout, listener.recv()).await {
                // Got a NOTIFY - new messages are available
                Ok(Ok(_notification)) => {
                    // Optionally debounce by waiting a bit to collect more notifications
                    if !min_debounce_interval.is_zero() {
                        let remaining_after_debounce = deadline
                            .saturating_duration_since(tokio::time::Instant::now())
                            .saturating_sub(min_debounce_interval);

                        if !remaining_after_debounce.is_zero() {
                            // Drain additional notifications during debounce period
                            let _ = tokio::time::timeout(min_debounce_interval, async {
                                while (listener.recv().await).is_ok() {
                                    // Just drain, we don't need the payload
                                    // anymore
                                }
                            })
                            .await;
                        }
                    }

                    // Stash the listener back
                    *self.listener.lock().await = Some(listener);

                    return Ok(MessageStoreWakeHint::NewMessages);
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
                    return Ok(MessageStoreWakeHint::Timeout);
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
