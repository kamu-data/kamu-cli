// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;
use std::time::Duration;

use kamu_wakeup_listener_sqlite::{SqlitePollingChannel, SqlitePollingHub};
use sqlx::SqlitePool;
use wakeup_listener::{HubWakeupListener, WakeHint, WakeupListener, WakeupListenerConfig};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const CHANNEL: SqlitePollingChannel = SqlitePollingChannel {
    name: "records",
    max_id_query: "SELECT MAX(record_id) FROM records",
};
pub(crate) const OTHER_CHANNEL: SqlitePollingChannel = SqlitePollingChannel {
    name: "other_records",
    max_id_query: "SELECT MAX(record_id) FROM other_records",
};
pub(crate) const BROKEN_CHANNEL: SqlitePollingChannel = SqlitePollingChannel {
    name: "missing_records",
    max_id_query: "SELECT MAX(record_id) FROM missing_records",
};

pub(crate) const SHORT_TIMEOUT: Duration = Duration::from_millis(300);
pub(crate) const LONG_TIMEOUT: Duration = Duration::from_secs(10);
const DEBOUNCE_INTERVAL: Duration = Duration::from_millis(20);
// Longer than the tests' timeouts, so that an idle hub backs off far enough to
// tell a reset backoff apart
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) type Listener = HubWakeupListener<SqlitePollingHub>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Owns a hub and a default listener on [`CHANNEL`]; extra listeners share the
/// hub
pub(crate) struct SqliteWakeupHarness {
    sqlite_pool: SqlitePool,
    hub: Arc<SqlitePollingHub>,
    listener: Listener,
}

impl SqliteWakeupHarness {
    pub(crate) async fn new(sqlite_pool: SqlitePool) -> Self {
        for table in ["records", "other_records"] {
            sqlx::query(&format!(
                "CREATE TABLE {table} (record_id INTEGER PRIMARY KEY AUTOINCREMENT)"
            ))
            .execute(&sqlite_pool)
            .await
            .unwrap();
        }

        let hub = Arc::new(SqlitePollingHub::new(
            Arc::new(sqlite_pool.clone()),
            Arc::new(WakeupListenerConfig {
                min_debounce_interval: DEBOUNCE_INTERVAL,
                max_listening_timeout: MAX_POLL_INTERVAL,
            }),
        ));
        let listener = HubWakeupListener::new(hub.clone(), CHANNEL);

        Self {
            sqlite_pool,
            hub,
            listener,
        }
    }

    /// The default listener subscribes lazily on its first wait, which reports
    /// a possible change
    pub(crate) async fn start_listening(&self) {
        assert_matches!(self.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
    }

    pub(crate) async fn wait_wake(&self, timeout: Duration) -> WakeHint {
        Self::wait_wake_on(&self.listener, timeout).await
    }

    /// Creates another listener on the shared hub, not subscribed yet
    pub(crate) fn new_listener(&self, channel: SqlitePollingChannel) -> Listener {
        HubWakeupListener::new(self.hub.clone(), channel)
    }

    /// Creates another listener on the shared hub, and subscribes it
    pub(crate) async fn subscribe(&self, channel: SqlitePollingChannel) -> Listener {
        let listener = self.new_listener(channel);
        assert_matches!(
            Self::wait_wake_on(&listener, LONG_TIMEOUT).await,
            WakeHint::Signaled
        );
        listener
    }

    pub(crate) async fn wait_wake_on(listener: &Listener, timeout: Duration) -> WakeHint {
        listener
            .wait_wake(timeout, DEBOUNCE_INTERVAL)
            .await
            .unwrap()
    }

    pub(crate) async fn settle_default(&self) {
        Self::settle(&self.listener).await;
    }

    /// Consumes spurious wakeups, e.g. the hub's first reading of existing
    /// records
    pub(crate) async fn settle(listener: &Listener) {
        for _ in 0..5 {
            if let WakeHint::Timeout = Self::wait_wake_on(listener, SHORT_TIMEOUT).await {
                return;
            }
        }
        panic!("Listener did not settle");
    }

    pub(crate) async fn insert_record(&self) {
        self.insert_into("records").await;
    }

    pub(crate) async fn insert_other_record(&self) {
        self.insert_into("other_records").await;
    }

    async fn insert_into(&self, table: &str) {
        sqlx::query(&format!("INSERT INTO {table} DEFAULT VALUES"))
            .execute(&self.sqlite_pool)
            .await
            .unwrap();
    }

    async fn insert_record_after(&self, delay: Duration) {
        tokio::time::sleep(delay).await;
        self.insert_record().await;
    }

    pub(crate) async fn wait_wake_while_inserting_after(
        &self,
        delay: Duration,
    ) -> (WakeHint, Duration) {
        let started_at = tokio::time::Instant::now();
        let (hint, ()) = tokio::join!(
            self.wait_wake(LONG_TIMEOUT),
            self.insert_record_after(delay)
        );
        (hint, started_at.elapsed())
    }

    pub(crate) async fn timed_wait_wake_on(listener: &Listener) -> (WakeHint, Duration) {
        let started_at = tokio::time::Instant::now();
        let hint = Self::wait_wake_on(listener, LONG_TIMEOUT).await;
        (hint, started_at.elapsed())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
