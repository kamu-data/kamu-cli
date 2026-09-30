// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use sqlx::postgres::PgListener;
use tokio::sync::Notify;
use wakeup_listener::{WakeupHub, WakeupListenerMetrics, WakeupSubscribers};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Independent of the debounce interval, so a short one doesn't flood a database
// that is down
const RECONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Multiplexes all `LISTEN` channels over a single pooled connection, so the
/// number of listening agents doesn't eat into the pool.
///
/// Guarantee towards subscribers: a notification can only be missed while
/// `LISTEN` is not active (before the first connect, or during a reconnect),
/// and every (re)connect signals all subscribers, so they re-check storage.
/// See `docs/internal/wakeup-listeners.md`.
pub struct PostgresNotificationHub {
    // Shared with the background task. Kept apart from the hub itself, so the
    // task doesn't keep the hub alive and `Drop` below can stop it.
    inner: Arc<HubInner>,
    // Spawned on the first subscription: DI may build the hub outside a runtime
    task: OnceLock<tokio::task::AbortHandle>,
    metrics: Arc<WakeupListenerMetrics>,
}

struct HubInner {
    pool: Arc<sqlx::PgPool>,
    // One slot per waiting listener handle, keyed by channel name. A new
    // channel makes the task reconnect with the extended channel set.
    subscribers: WakeupSubscribers<&'static str>,
}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
impl PostgresNotificationHub {
    pub fn new(pool: Arc<sqlx::PgPool>, metrics: Arc<WakeupListenerMetrics>) -> Self {
        Self {
            inner: Arc::new(HubInner {
                pool,
                subscribers: WakeupSubscribers::new(),
            }),
            task: OnceLock::new(),
            metrics,
        }
    }
}

impl WakeupHub for PostgresNotificationHub {
    type Channel = &'static str;

    // The slot is signaled once `LISTEN` on the channel is active, and on every
    // notification afterwards
    fn subscribe(&self, channel: &'static str) -> Arc<Notify> {
        let slot = self.inner.subscribers.add(channel);
        self.task
            .get_or_init(|| tokio::spawn(self.inner.clone().run()).abort_handle());
        slot
    }

    fn metrics(&self) -> &WakeupListenerMetrics {
        &self.metrics
    }
}

impl Drop for PostgresNotificationHub {
    // Aborting drops the task's `PgListener`, which returns the connection to the
    // pool
    fn drop(&mut self) {
        if let Some(task) = self.task.get() {
            task.abort();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl HubInner {
    /// Outer loop: one iteration per connection. Inner loop: routes
    /// notifications until the connection has to be replaced.
    async fn run(self: Arc<Self>) {
        let mut lost_connection = false;
        loop {
            // A server or proxy dropping sessions at once must not make every agent
            // re-check in a tight loop
            if lost_connection {
                tokio::time::sleep(RECONNECT_RETRY_INTERVAL).await;
                lost_connection = false;
            }

            // The snapshot below already covers any pending subscription change.
            // A subscription racing with it leaves a permit, costing one extra reconnect.
            self.subscribers.take_changed();
            let channels = self.subscribers.channels();

            let mut listener = match self.connect(&channels).await {
                Ok(listener) => listener,
                // Shutdown (or the end of a `sqlx::test`): exiting releases the
                // connection, otherwise `pool.close()` would wait for it
                Err(sqlx::Error::PoolClosed) => return,
                Err(e) => {
                    tracing::error!(
                        error = ?e,
                        error_msg = %e,
                        ?channels,
                        "Failed to listen on Postgres channels, will retry after delay",
                    );
                    tokio::time::sleep(RECONNECT_RETRY_INTERVAL).await;
                    continue;
                }
            };

            tracing::debug!(?channels, "Listening on Postgres channels");

            // Anything notified while LISTEN wasn't active is lost: let everyone re-check.
            // This is also what makes a fresh subscriber's first wait return `Signaled`.
            self.subscribers.signal_all();

            loop {
                tokio::select! {
                    // A new channel: rather than `listen()` on this connection after
                    // cancelling `try_recv()` (sqlx doesn't document it as cancel-safe),
                    // drop it and reconnect with the full channel set. Subscriptions happen
                    // at startup, so this costs one spurious wakeup to the other channels.
                    () = self.subscribers.changed() => break,
                    res = listener.try_recv() => match res {
                        Ok(Some(notification)) => self.subscribers.signal(notification.channel()),
                        // Connection lost. With eager reconnect off, sqlx reports it
                        // instead of silently reconnecting and hiding lost notifications.
                        Ok(None) => {
                            tracing::warn!("PgListener connection was lost, reconnecting");
                            lost_connection = true;
                            break;
                        }
                        Err(sqlx::Error::PoolClosed) => return,
                        Err(e) => {
                            tracing::error!(
                                error = ?e,
                                error_msg = %e,
                                "PgListener error, reconnecting",
                            );
                            lost_connection = true;
                            break;
                        }
                    },
                }
            }
        }
    }

    async fn connect(&self, channels: &[&'static str]) -> Result<PgListener, sqlx::Error> {
        // Checked out of the shared pool, and held for the lifetime of this connection
        let mut listener = PgListener::connect_with(&self.pool).await?;
        // Reconnects must go through `run()`, which signals the subscribers
        listener.eager_reconnect(false);
        listener.listen_all(channels.iter().copied()).await?;
        Ok(listener)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
