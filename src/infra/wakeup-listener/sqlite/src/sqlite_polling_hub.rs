// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::Notify;
use wakeup_listener::{WakeupHub, WakeupListenerConfig, WakeupSubscribers};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Guards against busy polling when the debounce interval is zero
const MIN_POLL_INTERVAL: Duration = Duration::from_millis(10);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A table watched by polling a cheap query returning its maximum id.
/// The id must only grow, so that a bigger value means new rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SqlitePollingChannel {
    /// Used in logs only
    pub name: &'static str,
    pub max_id_query: &'static str,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Polls all channels in a single loop, as Sqlite has no notifications, and
/// other processes may write to the same database file.
///
/// Compared to polling per listener, each tick acquires the pool's only
/// connection once instead of once per channel, and one shared backoff means a
/// change on any channel makes the next hop of a chain (outbox message, flow
/// event, task) visible quickly.
///
/// Guarantee towards subscribers: a new slot is signaled right away, and after
/// that on every poll which sees its channel's maximum id grow.
/// See `docs/internal/wakeup-listeners.md`.
pub struct SqlitePollingHub {
    // Shared with the background task. Kept apart from the hub itself, so the
    // task doesn't keep the hub alive and `Drop` below can stop it.
    inner: Arc<HubInner>,
    // Spawned on the first subscription: DI may build the hub outside a runtime
    task: OnceLock<tokio::task::AbortHandle>,
}

struct HubInner {
    pool: Arc<sqlx::SqlitePool>,
    config: Arc<WakeupListenerConfig>,
    // One slot per waiting listener handle, keyed by channel. A new channel
    // makes the task poll right away, to take its first reading.
    subscribers: WakeupSubscribers<SqlitePollingChannel>,
}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
impl SqlitePollingHub {
    pub fn new(pool: Arc<sqlx::SqlitePool>, config: Arc<WakeupListenerConfig>) -> Self {
        Self {
            inner: Arc::new(HubInner {
                pool,
                config,
                subscribers: WakeupSubscribers::new(),
            }),
            task: OnceLock::new(),
        }
    }
}

impl WakeupHub for SqlitePollingHub {
    type Channel = SqlitePollingChannel;

    fn subscribe(&self, channel: SqlitePollingChannel) -> Arc<Notify> {
        let slot = self.inner.subscribers.add(channel);

        // The channel may already be polled by another subscriber, and its latest
        // change consumed before this one joined. The subscriber has no earlier
        // reading of its own, so it must re-check once.
        slot.notify_one();

        self.task
            .get_or_init(|| tokio::spawn(self.inner.clone().run()).abort_handle());
        slot
    }
}

impl Drop for SqlitePollingHub {
    fn drop(&mut self) {
        if let Some(task) = self.task.get() {
            task.abort();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl HubInner {
    async fn run(self: Arc<Self>) {
        let min_interval = self.config.min_debounce_interval.max(MIN_POLL_INTERVAL);
        let max_interval = self.config.max_listening_timeout.max(min_interval);
        let mut interval = min_interval;

        // Maximum ids seen so far. Missing entries count as 0, so rows existing
        // before the first reading signal the channel: the agent may have drained
        // before they were written.
        let mut watermarks = HashMap::new();

        loop {
            match self.poll(&mut watermarks).await {
                // Changes come in bursts, so poll often while things are moving,
                // and back off exponentially while idle
                PollOutcome::Changed => interval = min_interval,
                PollOutcome::Unchanged => interval = (interval * 2).min(max_interval),
                // Shutdown (or the end of a `sqlx::test`)
                PollOutcome::PoolClosed => return,
            }

            tokio::select! {
                // A new channel: take its first reading without waiting out the backoff
                () = self.subscribers.changed() => {}
                () = tokio::time::sleep(interval) => {}
            }
        }
    }

    // Errors are logged and retried on the next tick
    async fn poll(&self, watermarks: &mut HashMap<SqlitePollingChannel, i64>) -> PollOutcome {
        let channels = self.subscribers.channels();

        let mut connection = match self.pool.acquire().await {
            Ok(connection) => connection,
            Err(sqlx::Error::PoolClosed) => return PollOutcome::PoolClosed,
            Err(e) => {
                tracing::error!(
                    error = ?e,
                    error_msg = %e,
                    "Failed to acquire Sqlite connection for polling, will retry",
                );
                return PollOutcome::Unchanged;
            }
        };

        let mut changed = false;

        // One query per channel rather than a combined one, so that a failing
        // query only affects its own channel
        for channel in channels {
            let max_id: Option<i64> = match sqlx::query_scalar(channel.max_id_query)
                .fetch_one(&mut *connection)
                .await
            {
                Ok(max_id) => max_id,
                Err(e) => {
                    tracing::error!(
                        error = ?e,
                        error_msg = %e,
                        channel = channel.name,
                        "Failed to poll Sqlite channel, will retry",
                    );
                    continue;
                }
            };

            let seen_id = watermarks.entry(channel).or_insert(0);
            if let Some(max_id) = max_id
                && max_id > *seen_id
            {
                *seen_id = max_id;
                self.subscribers.signal(&channel);
                changed = true;
            }
        }

        if changed {
            PollOutcome::Changed
        } else {
            PollOutcome::Unchanged
        }
    }
}

enum PollOutcome {
    Changed,
    Unchanged,
    PoolClosed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
