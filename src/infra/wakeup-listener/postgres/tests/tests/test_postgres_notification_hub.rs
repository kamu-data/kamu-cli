// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use sqlx::PgPool;
use wakeup_listener::WakeHint;

use super::harness::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_channels_share_single_connection(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);
    harness.start_listening().await;
    let _other = harness.subscribe(OTHER_CHANNEL).await;

    harness.assert_single_listen_connection().await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_routes_notifications_per_channel(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);
    let listener = harness.subscribe(CHANNEL).await;
    let other = harness.subscribe(OTHER_CHANNEL).await;
    PostgresWakeupHarness::settle(&listener).await;

    harness.notify(OTHER_CHANNEL).await;

    assert_matches!(
        PostgresWakeupHarness::wait_wake_on(&other, LONG_TIMEOUT).await,
        WakeHint::Signaled
    );
    assert_matches!(
        PostgresWakeupHarness::wait_wake_on(&listener, SHORT_TIMEOUT).await,
        WakeHint::Timeout
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_notification_wakes_up_all_listeners_of_channel(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);
    let listener_1 = harness.subscribe(CHANNEL).await;
    let listener_2 = harness.subscribe(CHANNEL).await;
    PostgresWakeupHarness::settle(&listener_1).await;

    harness.notify(CHANNEL).await;

    assert_matches!(
        PostgresWakeupHarness::wait_wake_on(&listener_1, LONG_TIMEOUT).await,
        WakeHint::Signaled
    );
    assert_matches!(
        PostgresWakeupHarness::wait_wake_on(&listener_2, LONG_TIMEOUT).await,
        WakeHint::Signaled
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_late_subscription_joins_shared_connection(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);
    harness.start_listening().await;

    // Subscribing re-establishes the shared connection with the extended channel
    // set
    let other = harness.subscribe(OTHER_CHANNEL).await;
    harness.settle_default().await;

    harness.notify(CHANNEL).await;
    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);

    harness.notify(OTHER_CHANNEL).await;
    assert_matches!(
        PostgresWakeupHarness::wait_wake_on(&other, LONG_TIMEOUT).await,
        WakeHint::Signaled
    );

    harness.assert_single_listen_connection().await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = false))]
async fn test_connection_loss_signals_all_channels(pg_pool: PgPool) {
    let harness = PostgresWakeupHarness::new(pg_pool);
    harness.start_listening().await;
    let other = harness.subscribe(OTHER_CHANNEL).await;
    harness.settle_default().await;
    PostgresWakeupHarness::settle(&other).await;

    harness.terminate_listener_connection().await;

    assert_matches!(harness.wait_wake(LONG_TIMEOUT).await, WakeHint::Signaled);
    assert_matches!(
        PostgresWakeupHarness::wait_wake_on(&other, LONG_TIMEOUT).await,
        WakeHint::Signaled
    );

    // Both channels keep working over the new connection
    PostgresWakeupHarness::settle(&other).await;
    harness.notify(OTHER_CHANNEL).await;
    assert_matches!(
        PostgresWakeupHarness::wait_wake_on(&other, LONG_TIMEOUT).await,
        WakeHint::Signaled
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
