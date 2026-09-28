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

use kamu_wakeup_listener_postgres::PostgresNotifyWakeupListener;
use sqlx::PgPool;
use wakeup_listener::{WakeHint, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Note: each test runs in its own database, and LISTEN/NOTIFY channels are
// scoped to a database, so tests don't interfere with each other

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_times_out_without_notifications(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);

    assert_matches!(harness.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_wakes_up_on_notification(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);
    harness.start_listening().await;

    harness.notify(CHANNEL).await;

    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_wakes_up_on_notification_while_waiting(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);
    harness.start_listening().await;

    let (hint, elapsed) = harness
        .wait_wake_while_notifying_after(Duration::from_millis(100))
        .await;

    assert_matches!(hint, WakeHint::Signaled);
    assert!(elapsed < LONG_TIMEOUT, "Woke up by timeout: {elapsed:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_ignores_notifications_on_other_channels(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);
    harness.start_listening().await;

    harness.notify("some_other_channel").await;

    assert_matches!(harness.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_ignores_rolled_back_notifications(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);
    harness.start_listening().await;

    // NOTIFY is only delivered when the transaction commits
    harness.notify_in_rolled_back_transaction(CHANNEL).await;

    assert_matches!(harness.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_debounce_coalesces_notification_burst(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);
    harness.start_listening().await;

    for _ in 0..5 {
        harness.notify(CHANNEL).await;
    }

    // The whole burst is drained by a single wakeup
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
    assert_matches!(harness.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_recovers_after_listener_connection_loss(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);
    harness.start_listening().await;

    harness.terminate_listener_connection().await;

    // Notifications sent while disconnected may be lost, but the listener
    // must reconnect and observe the following ones
    harness.wait_wake(SHORT_TIMEOUT).await;
    harness.notify(CHANNEL).await;

    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const CHANNEL: &str = "test_wakeup_channel";
const SHORT_TIMEOUT: Duration = Duration::from_millis(300);
const LONG_TIMEOUT: Duration = Duration::from_secs(10);
const DEBOUNCE_INTERVAL: Duration = Duration::from_millis(50);

struct PostgresWakeupHarness {
    pg_pool: PgPool,
    listener: PostgresNotifyWakeupListener,
}

impl PostgresWakeupHarness {
    fn new(pg_pool: PgPool) -> Self {
        let listener = PostgresNotifyWakeupListener::new(Arc::new(pg_pool.clone()), CHANNEL);
        Self { pg_pool, listener }
    }

    /// The listening connection is established lazily on the first wait,
    /// notifications sent before that are not observed
    async fn start_listening(&self) {
        assert_matches!(self.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);
    }

    async fn wait_wake(&self, timeout: Duration) -> WakeHint {
        self.listener
            .wait_wake(timeout, DEBOUNCE_INTERVAL)
            .await
            .unwrap()
    }

    async fn notify(&self, channel: &str) {
        sqlx::query("SELECT pg_notify($1, '')")
            .bind(channel)
            .execute(&self.pg_pool)
            .await
            .unwrap();
    }

    async fn notify_in_rolled_back_transaction(&self, channel: &str) {
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

    async fn wait_wake_while_notifying_after(&self, delay: Duration) -> (WakeHint, Duration) {
        let started_at = tokio::time::Instant::now();
        let (hint, ()) = tokio::join!(self.wait_wake(LONG_TIMEOUT), self.notify_after(delay));
        (hint, started_at.elapsed())
    }

    async fn terminate_listener_connection(&self) {
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
        assert_eq!(terminated, 1);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
