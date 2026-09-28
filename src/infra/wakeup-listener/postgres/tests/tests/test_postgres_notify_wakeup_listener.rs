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

use sqlx::PgPool;
use wakeup_listener::WakeHint;

use super::harness::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Note: each test runs in its own database, and LISTEN/NOTIFY channels are
// scoped to a database, so tests don't interfere with each other

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_signals_when_subscribed(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);

    // Notifications sent before LISTEN was active are lost, so subscribing
    // reports a possible change
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_times_out_without_notifications(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);
    harness.start_listening().await;

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
async fn test_signals_and_recovers_after_connection_loss(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);
    harness.start_listening().await;

    // Notifications sent while disconnected are lost, so reconnecting
    // reports a possible change
    harness.terminate_listener_connection().await;
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);

    assert_matches!(harness.wait_wake(SHORT_TIMEOUT).await, WakeHint::Timeout);
    harness.notify(CHANNEL).await;
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
