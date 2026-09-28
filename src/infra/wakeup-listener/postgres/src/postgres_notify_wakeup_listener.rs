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
use sqlx::postgres::PgListener;
use wakeup_listener::{WakeHint, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Independent of the debounce interval, so a short one doesn't flood a database
// that is down
const RECONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Waits for Postgres `NOTIFY` signals on the given channel via `LISTEN`.
/// The channel is expected to be notified by triggers on the watched tables.
pub struct PostgresNotifyWakeupListener {
    pool: Arc<sqlx::PgPool>,
    listener: tokio::sync::Mutex<Option<PgListener>>,
    channel_name: &'static str,
}

impl PostgresNotifyWakeupListener {
    pub fn new(pool: Arc<sqlx::PgPool>, channel_name: &'static str) -> Self {
        Self {
            pool,
            listener: Default::default(),
            channel_name,
        }
    }

    async fn try_create_listener(&self) -> Option<PgListener> {
        match PgListener::connect_with(&self.pool).await {
            Ok(mut l) => {
                // Reconnects must go through `wait_wake()`, which reports a possible change
                l.eager_reconnect(false);
                match l.listen(self.channel_name).await {
                    Ok(_) => Some(l),
                    Err(e) => {
                        tracing::error!(
                            error = ?e,
                            error_msg = %e,
                            "Failed to listen on channel '{}'", self.channel_name,
                        );
                        None
                    }
                }
            }
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

    fn calculate_retry_delay(deadline: tokio::time::Instant) -> Duration {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        std::cmp::min(RECONNECT_RETRY_INTERVAL, remaining)
    }

    /// Returns `false` if the connection was lost while draining
    async fn drain_notifications(
        listener: &mut PgListener,
        deadline: tokio::time::Instant,
        min_debounce_interval: Duration,
    ) -> bool {
        let remaining_after_debounce = deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .saturating_sub(min_debounce_interval);
        if min_debounce_interval.is_zero() || remaining_after_debounce.is_zero() {
            return true;
        }

        tokio::time::timeout(min_debounce_interval, async {
            while let Ok(Some(_notification)) = listener.try_recv().await {}
            false
        })
        .await
        .unwrap_or(true)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WakeupListener for PostgresNotifyWakeupListener {
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<WakeHint, InternalError> {
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let Some(mut listener) = self.listener.lock().await.take() else {
                match self.try_create_listener().await {
                    // Notifications sent before LISTEN was active are lost,
                    // so let the caller re-check the storage
                    Some(new_listener) => {
                        *self.listener.lock().await = Some(new_listener);
                        return Ok(WakeHint::Signaled);
                    }
                    None => {
                        let delay = Self::calculate_retry_delay(deadline);
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        if deadline <= tokio::time::Instant::now() {
                            return Ok(WakeHint::Timeout);
                        }
                        continue;
                    }
                }
            };

            let remaining_timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining_timeout.is_zero() {
                *self.listener.lock().await = Some(listener);
                return Ok(WakeHint::Timeout);
            }

            match tokio::time::timeout(remaining_timeout, listener.try_recv()).await {
                // Got a NOTIFY - new data might be available
                Ok(Ok(Some(_notification))) => {
                    if Self::drain_notifications(&mut listener, deadline, min_debounce_interval)
                        .await
                    {
                        *self.listener.lock().await = Some(listener);
                    }
                    return Ok(WakeHint::Signaled);
                }

                // Connection lost: drop the listener, re-subscribing will report a possible change
                Ok(Ok(None)) => {
                    tracing::warn!("PgListener connection was lost, re-subscribing");
                }

                Ok(Err(conn_err)) => {
                    tracing::error!(
                        error = ?conn_err,
                        error_msg = %conn_err,
                        "PgListener connection error, will attempt to reconnect after delay",
                    );

                    let delay = Self::calculate_retry_delay(deadline);
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                }

                Err(_elapsed) => {
                    *self.listener.lock().await = Some(listener);
                    return Ok(WakeHint::Timeout);
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
