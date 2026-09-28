// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::time::Duration;

use sqlx::SqlitePool;
use wakeup_listener::WakeHint;

use super::harness::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_subscription_signals_then_times_out_on_empty_table(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;

    harness.start_listening().await;

    assert_matches!(harness.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_wakes_up_on_records_existing_before_subscription(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;
    harness.insert_record().await;

    harness.start_listening().await;

    // The records are reported at most once more, by the hub's first reading
    harness.settle_default().await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_times_out_when_nothing_new_since_last_wakeup(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;
    harness.start_listening().await;
    harness.settle_default().await;

    harness.insert_record().await;
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);

    assert_matches!(harness.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_wakes_up_on_record_inserted_while_waiting(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;
    harness.start_listening().await;
    harness.settle_default().await;

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
    harness.start_listening().await;
    harness.settle_default().await;

    harness.insert_record().await;
    harness.insert_record().await;
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
    assert_matches!(harness.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);

    harness.insert_record().await;
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
    assert_matches!(harness.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
