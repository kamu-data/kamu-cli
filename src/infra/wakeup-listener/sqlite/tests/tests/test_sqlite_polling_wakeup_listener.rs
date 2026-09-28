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

use kamu_wakeup_listener_sqlite::SqlitePollingWakeupListener;
use sqlx::SqlitePool;
use wakeup_listener::{WakeHint, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_times_out_on_empty_table(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;

    assert_matches!(harness.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_wakes_up_immediately_on_existing_records(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;
    harness.insert_record().await;

    // Records existing before the first wait are treated as new
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_times_out_when_nothing_new_since_last_wakeup(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;
    harness.insert_record().await;
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);

    assert_matches!(harness.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_wakes_up_on_record_inserted_while_waiting(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;

    let (hint, elapsed) = harness
        .wait_wake_while_inserting_after(Duration::from_millis(150))
        .await;

    assert_matches!(hint, WakeHint::Signaled);
    assert!(elapsed < LONG_TIMEOUT, "Woke up by timeout: {elapsed:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_wakes_up_once_per_batch_of_new_records(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;
    harness.insert_record().await;
    harness.insert_record().await;
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
    assert_matches!(harness.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);

    harness.insert_record().await;
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
    assert_matches!(harness.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SHORT_TIMEOUT: Duration = Duration::from_millis(300);
const LONG_TIMEOUT: Duration = Duration::from_secs(10);
const DEBOUNCE_INTERVAL: Duration = Duration::from_millis(20);

struct SqliteWakeupHarness {
    sqlite_pool: SqlitePool,
    listener: SqlitePollingWakeupListener,
}

impl SqliteWakeupHarness {
    async fn new(sqlite_pool: SqlitePool) -> Self {
        sqlx::query("CREATE TABLE records (record_id INTEGER PRIMARY KEY AUTOINCREMENT)")
            .execute(&sqlite_pool)
            .await
            .unwrap();

        let listener = SqlitePollingWakeupListener::new(
            Arc::new(sqlite_pool.clone()),
            "SELECT MAX(record_id) FROM records",
        );

        Self {
            sqlite_pool,
            listener,
        }
    }

    async fn wait_wake(&self, timeout: Duration) -> WakeHint {
        self.listener
            .wait_wake(timeout, DEBOUNCE_INTERVAL)
            .await
            .unwrap()
    }

    async fn insert_record(&self) {
        sqlx::query("INSERT INTO records DEFAULT VALUES")
            .execute(&self.sqlite_pool)
            .await
            .unwrap();
    }

    async fn insert_record_after(&self, delay: Duration) {
        tokio::time::sleep(delay).await;
        self.insert_record().await;
    }

    async fn wait_wake_while_inserting_after(&self, delay: Duration) -> (WakeHint, Duration) {
        let started_at = tokio::time::Instant::now();
        let (hint, ()) = tokio::join!(
            self.wait_wake(LONG_TIMEOUT),
            self.insert_record_after(delay)
        );
        (hint, started_at.elapsed())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
