// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::TimeDelta;
use pretty_assertions::assert_eq;

use crate::tests::controller_runtime::utils::{
    ControllerRuntimeHarness,
    FAILURE_BACKOFF,
    LOOP_RESTART_BACKOFF,
    OTHER_TEST_CONTROLLER_NAME,
    TEST_CONTROLLER_NAME,
    TestControllerBehavior,
    TestControllers,
    TestListingFailure,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// The agent runs on the fake clock, so a restart backoff passes only when a
// test moves the clock

#[test_log::test(tokio::test)]
async fn test_failing_loop_restarts_with_backoff_while_another_keeps_processing() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::MainAndOther,
        TestControllerBehavior::FinishAtCurrentVersion,
    )
    .await;
    harness
        .script()
        .fail_next_listings(OTHER_TEST_CONTROLLER_NAME, TestListingFailure::Error, 2);
    let a = harness.apply("res-a").await;
    let _agent = harness.start_agent();

    harness.script().wait_for_total_calls(1).await;
    harness.let_agent_settle().await;
    assert!(harness.state(a).await.is_some());
    assert_eq!(harness.script().listings_of(OTHER_TEST_CONTROLLER_NAME), 1);
    assert_eq!(harness.loop_restarts_metric(OTHER_TEST_CONTROLLER_NAME), 1);

    // The main controller's loop keeps processing meanwhile
    let b = harness.apply("res-b").await;
    harness.deliver_messages().await;
    harness.script().wait_for_total_calls(2).await;
    harness.let_agent_settle().await;
    assert!(harness.state(b).await.is_some());

    // The restarted loop fails again, and waits twice as long
    harness.advance_time(LOOP_RESTART_BACKOFF);
    harness.let_agent_settle().await;
    assert_eq!(harness.script().listings_of(OTHER_TEST_CONTROLLER_NAME), 2);
    assert_eq!(harness.loop_restarts_metric(OTHER_TEST_CONTROLLER_NAME), 2);

    harness.advance_time(LOOP_RESTART_BACKOFF * 2 - TimeDelta::seconds(1));
    harness.let_agent_settle().await;
    assert_eq!(harness.script().listings_of(OTHER_TEST_CONTROLLER_NAME), 2);

    // A drain, then another: the first wait subscribes to hints, which wakes
    // it once, so that no change made before is missed
    harness.advance_time(TimeDelta::seconds(1));
    harness.let_agent_settle().await;
    assert_eq!(harness.script().listings_of(OTHER_TEST_CONTROLLER_NAME), 4);
    assert_eq!(harness.loop_restarts_metric(OTHER_TEST_CONTROLLER_NAME), 2);
    assert_eq!(harness.loop_restarts_metric(TEST_CONTROLLER_NAME), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_panicking_loop_restarts_and_a_completed_drain_resets_its_backoff() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::MainAndOther,
        TestControllerBehavior::FinishAtCurrentVersion,
    )
    .await;
    harness
        .script()
        .fail_next_listings(OTHER_TEST_CONTROLLER_NAME, TestListingFailure::Panic, 1);
    let _agent = harness.start_agent();

    harness.let_agent_settle().await;
    assert_eq!(harness.loop_restarts_metric(OTHER_TEST_CONTROLLER_NAME), 1);

    // A drain, then another: the first wait subscribes to hints, which wakes
    // it once, so that no change made before is missed
    harness.advance_time(LOOP_RESTART_BACKOFF);
    harness.let_agent_settle().await;
    assert_eq!(harness.script().listings_of(OTHER_TEST_CONTROLLER_NAME), 3);

    // Its drain completed, so the next failure waits the first delay again
    harness
        .script()
        .fail_next_listings(OTHER_TEST_CONTROLLER_NAME, TestListingFailure::Panic, 1);
    harness.hint_other_schema();
    harness.let_agent_settle().await;
    assert_eq!(harness.script().listings_of(OTHER_TEST_CONTROLLER_NAME), 4);
    assert_eq!(harness.loop_restarts_metric(OTHER_TEST_CONTROLLER_NAME), 2);

    harness.advance_time(LOOP_RESTART_BACKOFF - TimeDelta::seconds(1));
    harness.let_agent_settle().await;
    assert_eq!(harness.script().listings_of(OTHER_TEST_CONTROLLER_NAME), 4);

    harness.advance_time(TimeDelta::seconds(1));
    harness.let_agent_settle().await;
    assert_eq!(harness.script().listings_of(OTHER_TEST_CONTROLLER_NAME), 5);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_metrics_count_outcomes_and_failing_resources() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::FinishAtCurrentVersion,
    )
    .await;
    let _done = harness.apply("res-done").await;
    let failing = harness.apply("res-failing").await;
    let retrying = harness.apply("res-retrying").await;
    harness
        .script()
        .set_behavior_of(failing, TestControllerBehavior::Fail);
    harness.script().set_behavior_of(
        retrying,
        TestControllerBehavior::RetryAfter(TimeDelta::hours(1)),
    );

    harness.run_outbox_and_controllers_until_idle().await;
    assert_eq!(
        (
            harness.processed_metric("done"),
            harness.processed_metric("retry_at"),
            harness.processed_metric("failed"),
            harness.failing_resources_metric(),
        ),
        (1, 1, 1, 1)
    );

    // Recovered after its backoff
    harness
        .script()
        .set_behavior_of(failing, TestControllerBehavior::FinishAtCurrentVersion);
    harness.advance_time(FAILURE_BACKOFF);
    harness.run_outbox_and_controllers_until_idle().await;
    assert_eq!(
        (
            harness.processed_metric("done"),
            harness.processed_metric("retry_at"),
            harness.processed_metric("failed"),
            harness.failing_resources_metric(),
        ),
        (2, 1, 1, 0)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
