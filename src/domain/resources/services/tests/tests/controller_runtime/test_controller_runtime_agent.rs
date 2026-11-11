// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::TimeDelta;
use kamu_resources::{ResourceID, ResourcePhase};
use pretty_assertions::assert_eq;

use crate::tests::controller_runtime::utils::{
    ControllerRuntimeHarness,
    FAILURE_BACKOFF,
    TestControllerBehavior,
    TestControllers,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// The agent runs on the fake clock: it sleeps until the deadlines the tests
// move the clock to, and never on its own

#[test_log::test(tokio::test)]
async fn test_agent_wakes_for_a_retry_at_its_time() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::Stepwise {
            step_wait: TimeDelta::minutes(1),
        },
    )
    .await;
    let id = harness.apply("res-a").await;
    let _agent = harness.start_agent();

    harness.script().wait_for_total_calls(1).await;
    harness.let_agent_settle().await;
    assert_eq!(harness.phase(id).await, ResourcePhase::Reconciling);

    harness.advance_time(TimeDelta::seconds(59));
    harness.let_agent_settle().await;
    assert_eq!(harness.script().total_calls(), 1);

    harness.advance_time(TimeDelta::seconds(1));
    harness.script().wait_for_total_calls(2).await;
    harness.let_agent_settle().await;
    assert_eq!(harness.phase(id).await, ResourcePhase::Ready);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failing_extra_resource_neither_spins_nor_wakes_the_agent_during_its_backoff() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::Stepwise {
            step_wait: TimeDelta::zero(),
        },
    )
    .await;
    // Reconciled, so only the extra set makes it actionable
    let id = harness.apply("res-a").await;
    harness.run_until_idle().await;
    assert_eq!(harness.phase(id).await, ResourcePhase::Ready);
    let calls_before = harness.script().total_calls();

    // Due long ago, like a claim that expired an hour ago, and failing
    make_failing_extra_due_long_ago(&harness, id);
    let _agent = harness.start_agent();

    harness
        .script()
        .wait_for_total_calls(calls_before + 1)
        .await;
    harness.let_agent_settle().await;
    let waits_before = harness.script().extra_deadline_queries();

    // During the backoff: no processing, and no wakeup either
    harness.advance_time(FAILURE_BACKOFF - TimeDelta::seconds(1));
    harness.let_agent_settle().await;
    assert_eq!(harness.script().total_calls(), calls_before + 1);
    assert_eq!(harness.script().extra_deadline_queries(), waits_before);

    // Its backoff over, the second failure doubles it
    harness.advance_time(TimeDelta::seconds(1));
    harness
        .script()
        .wait_for_total_calls(calls_before + 2)
        .await;
    harness.let_agent_settle().await;
    assert_eq!(harness.state(id).await.unwrap().attempts, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_future_extra_deadline_wakes_the_agent() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::FinishAtCurrentVersion,
    )
    .await;
    let id = harness.apply("res-a").await;
    harness.run_until_idle().await;
    let calls_before = harness.script().total_calls();

    harness
        .script()
        .set_extra_deadlines(vec![harness.time_source().now() + TimeDelta::minutes(1)]);
    let _agent = harness.start_agent();
    harness.let_agent_settle().await;
    assert_eq!(harness.script().total_calls(), calls_before);

    // The resource joins the extra set at the deadline, without any message
    harness.script().set_extras(vec![id]);
    harness.advance_time(TimeDelta::minutes(1));

    harness
        .script()
        .wait_for_total_calls(calls_before + 1)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_extra_deadline_passing_between_the_drain_and_the_wait_wakes_the_agent() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::FinishAtCurrentVersion,
    )
    .await;
    let id = harness.apply("res-a").await;
    harness.run_until_idle().await;
    let calls_before = harness.script().total_calls();

    let _agent = harness.start_agent();
    harness.let_agent_settle().await;

    // A claim expiring a minute from now does so after the next drain listed
    // the extra resources, but before that drain's wait starts
    harness
        .script()
        .set_extra_deadlines(vec![harness.time_source().now() + TimeDelta::minutes(1)]);
    harness
        .script()
        .after_next_listing(TimeDelta::minutes(1), vec![id]);
    harness.hint();

    harness
        .script()
        .wait_for_total_calls(calls_before + 1)
        .await;
}

#[test_log::test(tokio::test)]
async fn test_overdue_extra_backing_off_does_not_hide_a_later_extra_deadline() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::FinishAtCurrentVersion,
    )
    .await;
    let backing_off = harness.apply("res-a").await;
    let expiring = harness.apply("res-b").await;
    harness.run_until_idle().await;

    // The first claim expired an hour ago and keeps failing, so it backs off
    // longer than the second claim takes to expire
    make_failing_extra_due_long_ago(&harness, backing_off);
    let now = harness.time_source().now();
    harness
        .script()
        .set_extra_deadlines(vec![now - TimeDelta::hours(1), now + TimeDelta::seconds(1)]);
    let _agent = harness.start_agent();
    harness.script().wait_for_total_calls(3).await;
    harness.let_agent_settle().await;

    harness.script().set_extras(vec![backing_off, expiring]);
    harness.advance_time(TimeDelta::seconds(1));

    harness.script().wait_for_total_calls(4).await;
    assert_eq!(harness.script().calls_of(&expiring), 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_failing_extra_due_long_ago(harness: &ControllerRuntimeHarness, id: ResourceID) {
    let script = harness.script();
    script.set_extras(vec![id]);
    script.set_extra_deadlines(vec![harness.time_source().now() - TimeDelta::hours(1)]);
    script.set_behavior_of(id, TestControllerBehavior::Fail);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
