// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::TimeDelta;
use kamu_resources::ResourcePhase;
use pretty_assertions::assert_eq;

use crate::tests::controller_runtime::utils::{
    ControllerRuntimeHarness,
    TEST_CONTROLLER_NAME,
    TestControllerBehavior,
    TestControllers,
    TestListingFailure,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// The agent runs on the fake clock: its waits and restart backoffs end only
// when a test moves the clock, so a returning agent was ended by the shutdown

#[test_log::test(tokio::test)]
async fn test_shutdown_ends_the_wait_for_the_next_step() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::Stepwise {
            step_wait: TimeDelta::minutes(1),
        },
    )
    .await;
    let id = harness.apply("res-a").await;
    let mut agent = harness.start_agent();
    harness.script().wait_for_total_calls(1).await;
    harness.let_agent_settle().await;

    agent.shutdown().await.unwrap();

    assert_eq!(harness.script().total_calls(), 1);
    assert_eq!(harness.phase(id).await, ResourcePhase::Reconciling);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_shutdown_lets_the_current_drain_finish() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::FinishWhenReleased,
    )
    .await;
    harness.apply("res-a").await;
    let mut agent = harness.start_agent();
    harness.script().wait_for_total_calls(1).await;

    agent.request_shutdown();
    harness.let_agent_settle().await;
    assert!(!agent.has_returned());

    harness.script().release_processing();
    agent.shutdown().await.unwrap();

    assert_eq!(harness.processed_metric("done"), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_loop_failing_during_a_shutdown_is_not_restarted() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::FinishWhenReleased,
    )
    .await;
    harness.apply("res-a").await;
    let mut agent = harness.start_agent();
    harness.script().wait_for_total_calls(1).await;

    // The drain's next pass fails, after the shutdown was requested
    harness
        .script()
        .fail_next_listings(TEST_CONTROLLER_NAME, TestListingFailure::Error, 1);
    agent.request_shutdown();
    harness.let_agent_settle().await;
    harness.script().release_processing();

    agent.shutdown().await.unwrap();
    assert_eq!(harness.loop_restarts_metric(TEST_CONTROLLER_NAME), 0);
    assert_eq!(harness.script().listings_of(TEST_CONTROLLER_NAME), 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_shutdown_ends_a_loop_restart_backoff() {
    let harness = ControllerRuntimeHarness::finishing().await;
    harness
        .script()
        .fail_next_listings(TEST_CONTROLLER_NAME, TestListingFailure::Error, 1);
    let mut agent = harness.start_agent();
    harness.let_agent_settle().await;
    assert_eq!(harness.loop_restarts_metric(TEST_CONTROLLER_NAME), 1);

    agent.shutdown().await.unwrap();

    assert_eq!(harness.script().listings_of(TEST_CONTROLLER_NAME), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
