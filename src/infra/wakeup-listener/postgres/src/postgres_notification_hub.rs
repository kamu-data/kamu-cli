// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use futures::FutureExt as _;
use sqlx::postgres::PgListener;
use tokio::sync::Notify;

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
}

struct HubInner {
    pool: Arc<sqlx::PgPool>,
    // One slot per waiting listener handle. Weak, so a dropped handle simply
    // stops receiving signals and is pruned on the next routing.
    subscribers: Mutex<HashMap<&'static str, Vec<Weak<Notify>>>>,
    // Tells the task to reconnect with an extended channel set. `Notify`
    // stores a permit, so a subscription made while the task is busy
    // connecting is not lost.
    subscriptions_changed: Notify,
}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
impl PostgresNotificationHub {
    pub fn new(pool: Arc<sqlx::PgPool>) -> Self {
        Self {
            inner: Arc::new(HubInner {
                pool,
                subscribers: Mutex::default(),
                subscriptions_changed: Notify::new(),
            }),
            task: OnceLock::new(),
        }
    }

    /// Registers a subscriber slot, which is signaled once `LISTEN` on the
    /// channel is active and on every notification afterwards.
    /// Must be called within a Tokio runtime.
    pub(crate) fn subscribe(&self, channel: &'static str) -> Arc<Notify> {
        let slot = Arc::new(Notify::new());
        self.inner
            .subscribers
            .lock()
            .unwrap()
            .entry(channel)
            .or_default()
            .push(Arc::downgrade(&slot));

        self.inner.subscriptions_changed.notify_one();
        self.task
            .get_or_init(|| tokio::spawn(self.inner.clone().run()).abort_handle());

        slot
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
        loop {
            // The snapshot below already covers any pending subscription change.
            // A subscription racing with it leaves a permit, costing one extra reconnect.
            let _ = self.subscriptions_changed.notified().now_or_never();
            let channels = self.channels();

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
            self.signal_all();

            loop {
                tokio::select! {
                    // A new channel: rather than `listen()` on this connection after
                    // cancelling `try_recv()` (sqlx doesn't document it as cancel-safe),
                    // drop it and reconnect with the full channel set. Subscriptions happen
                    // at startup, so this costs one spurious wakeup to the other channels.
                    () = self.subscriptions_changed.notified() => break,
                    res = listener.try_recv() => match res {
                        Ok(Some(notification)) => self.signal(notification.channel()),
                        // Connection lost. With eager reconnect off, sqlx reports it
                        // instead of silently reconnecting and hiding lost notifications.
                        Ok(None) => {
                            tracing::warn!("PgListener connection was lost, reconnecting");
                            break;
                        }
                        Err(sqlx::Error::PoolClosed) => return,
                        Err(e) => {
                            tracing::error!(
                                error = ?e,
                                error_msg = %e,
                                "PgListener error, reconnecting",
                            );
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

    fn channels(&self) -> Vec<&'static str> {
        self.subscribers.lock().unwrap().keys().copied().collect()
    }

    fn signal(&self, channel: &str) {
        let mut subscribers = self.subscribers.lock().unwrap();
        if let Some(slots) = subscribers.get_mut(channel) {
            Self::signal_slots(slots);
        }
    }

    fn signal_all(&self) {
        let mut subscribers = self.subscribers.lock().unwrap();
        for slots in subscribers.values_mut() {
            Self::signal_slots(slots);
        }
    }

    // `notify_one` stores a permit if the handle isn't waiting right now, so the
    // signal is picked up by its next `wait_wake`
    fn signal_slots(slots: &mut Vec<Weak<Notify>>) {
        slots.retain(|slot| match slot.upgrade() {
            Some(slot) => {
                slot.notify_one();
                true
            }
            None => false,
        });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
