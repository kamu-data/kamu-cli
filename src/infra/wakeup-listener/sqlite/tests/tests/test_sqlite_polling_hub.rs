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
async fn test_routes_changes_per_channel(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;
    let listener = harness.subscribe(CHANNEL).await;
    let other = harness.subscribe(OTHER_CHANNEL).await;
    SqliteWakeupHarness::settle(&listener).await;
    SqliteWakeupHarness::settle(&other).await;

    harness.insert_other_record().await;

    assert_matches!(
        SqliteWakeupHarness::wait_wake_on(&other, LONG_TIMEOUT).await,
        WakeHint::Signaled
    );
    assert_matches!(
        SqliteWakeupHarness::wait_wake_on(&listener, SHORT_TIMEOUT).await,
        WakeHint::Timeout
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_change_wakes_up_all_listeners_of_channel(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;
    let listener_1 = harness.subscribe(CHANNEL).await;
    let listener_2 = harness.subscribe(CHANNEL).await;
    SqliteWakeupHarness::settle(&listener_1).await;
    SqliteWakeupHarness::settle(&listener_2).await;

    harness.insert_record().await;

    assert_matches!(
        SqliteWakeupHarness::wait_wake_on(&listener_1, LONG_TIMEOUT).await,
        WakeHint::Signaled
    );
    assert_matches!(
        SqliteWakeupHarness::wait_wake_on(&listener_2, LONG_TIMEOUT).await,
        WakeHint::Signaled
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_late_subscriber_is_signaled_immediately(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;
    harness.start_listening().await;
    harness.settle_default().await;

    // The hub has already consumed this change for the default listener
    harness.insert_record().await;
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);

    let late = harness.new_listener(CHANNEL);
    assert_matches!(
        SqliteWakeupHarness::wait_wake_on(&late, SHORT_TIMEOUT).await,
        WakeHint::Signaled
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_change_resets_backoff(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;
    let other = harness.subscribe(OTHER_CHANNEL).await;
    harness.start_listening().await;
    harness.settle_default().await;
    SqliteWakeupHarness::settle(&other).await;

    // Idle long enough for the poll interval to grow past a second
    assert_matches!(
        harness.wait_wake(Duration::from_secs(2)).await,
        WakeHint::Timeout
    );

    harness.insert_record().await;
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);

    // Right after a change the hub polls often again, for every channel
    harness.insert_other_record().await;
    let (hint, elapsed) = SqliteWakeupHarness::timed_wait_wake_on(&other).await;
    assert_matches!(hint, WakeHint::Signaled);
    assert!(
        elapsed < Duration::from_millis(500),
        "Backoff was not reset: {elapsed:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_failing_channel_query_does_not_block_others(sqlite_pool: SqlitePool) {
    let harness = SqliteWakeupHarness::new(sqlite_pool).await;
    let _broken = harness.subscribe(BROKEN_CHANNEL).await;
    harness.start_listening().await;
    harness.settle_default().await;

    harness.insert_record().await;

    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
