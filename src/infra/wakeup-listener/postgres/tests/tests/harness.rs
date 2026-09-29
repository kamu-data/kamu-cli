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

use kamu_wakeup_listener_postgres::PostgresNotificationHub;
use sqlx::PgPool;
use wakeup_listener::{HubWakeupListener, WakeHint, WakeupListener, WakeupListenerMetrics};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const CHANNEL: &str = "test_wakeup_channel";
pub(crate) const OTHER_CHANNEL: &str = "test_other_wakeup_channel";
pub(crate) const SHORT_TIMEOUT: Duration = Duration::from_millis(300);
pub(crate) const LONG_TIMEOUT: Duration = Duration::from_secs(10);
const AGENT: &str = "test_agent";
const DEBOUNCE_INTERVAL: Duration = Duration::from_millis(50);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Owns a hub and a default listener on [`CHANNEL`]; extra listeners share the
/// hub
pub(crate) struct PostgresWakeupHarness {
    pg_pool: PgPool,
    hub: Arc<PostgresNotificationHub>,
    listener: HubWakeupListener<PostgresNotificationHub>,
}

impl PostgresWakeupHarness {
    pub(crate) fn new(pg_pool: PgPool) -> Self {
        let hub = Arc::new(PostgresNotificationHub::new(
            Arc::new(pg_pool.clone()),
            Arc::new(WakeupListenerMetrics::new()),
        ));
        let listener = HubWakeupListener::new(hub.clone(), CHANNEL, AGENT);
        Self {
            pg_pool,
            hub,
            listener,
        }
    }

    /// The default listener subscribes lazily on its first wait
    pub(crate) async fn start_listening(&self) {
        assert_matches!(self.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
    }

    pub(crate) async fn wait_wake(&self, timeout: Duration) -> WakeHint {
        Self::wait_wake_on(&self.listener, timeout).await
    }

    /// Creates another listener on the shared hub, and waits until it is
    /// subscribed
    pub(crate) async fn subscribe(
        &self,
        channel: &'static str,
    ) -> HubWakeupListener<PostgresNotificationHub> {
        let listener = HubWakeupListener::new(self.hub.clone(), channel, AGENT);
        assert_matches!(
            Self::wait_wake_on(&listener, LONG_TIMEOUT).await,
            WakeHint::Signaled
        );
        listener
    }

    pub(crate) async fn wait_wake_on(
        listener: &HubWakeupListener<PostgresNotificationHub>,
        timeout: Duration,
    ) -> WakeHint {
        listener
            .wait_wake(timeout, DEBOUNCE_INTERVAL)
            .await
            .unwrap()
    }

    pub(crate) async fn settle_default(&self) {
        Self::settle(&self.listener).await;
    }

    /// Consumes spurious wakeups, e.g. from a reconnect caused by a later
    /// subscription
    pub(crate) async fn settle(listener: &HubWakeupListener<PostgresNotificationHub>) {
        for _ in 0..5 {
            if let WakeHint::Timeout = Self::wait_wake_on(listener, SHORT_TIMEOUT).await {
                return;
            }
        }
        panic!("Listener did not settle");
    }

    pub(crate) async fn notify(&self, channel: &str) {
        sqlx::query("SELECT pg_notify($1, '')")
            .bind(channel)
            .execute(&self.pg_pool)
            .await
            .unwrap();
    }

    pub(crate) async fn notify_in_rolled_back_transaction(&self, channel: &str) {
        let mut tx = self.pg_pool.begin().await.unwrap();
        sqlx::query("SELECT pg_notify($1, '')")
            .bind(channel)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
    }

    async fn notify_after(&self, delay: Duration) {
        tokio::time::sleep(delay).await;
        self.notify(CHANNEL).await;
    }

    pub(crate) async fn wait_wake_while_notifying_after(
        &self,
        delay: Duration,
    ) -> (WakeHint, Duration) {
        let started_at = tokio::time::Instant::now();
        let (hint, ()) = tokio::join!(self.wait_wake(LONG_TIMEOUT), self.notify_after(delay));
        (hint, started_at.elapsed())
    }

    pub(crate) async fn terminate_listener_connection(&self) {
        let (terminated,): (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(pg_terminate_backend(pid))
                FROM pg_stat_activity
                WHERE datname = current_database() AND query LIKE 'LISTEN%'
            "#,
        )
        .fetch_one(&self.pg_pool)
        .await
        .unwrap();
        assert!(terminated >= 1);
    }

    /// Previous connections are released asynchronously after a reconnect, so
    /// poll a bit
    pub(crate) async fn assert_single_listen_connection(&self) {
        let mut listen_connections = 0;
        for _ in 0..50 {
            let (count,): (i64,) = sqlx::query_as(
                r#"
                SELECT COUNT(*)
                    FROM pg_stat_activity
                    WHERE datname = current_database() AND query LIKE 'LISTEN%'
                "#,
            )
            .fetch_one(&self.pg_pool)
            .await
            .unwrap();

            listen_connections = count;
            if listen_connections == 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("Expected a single LISTEN connection, found {listen_connections}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
