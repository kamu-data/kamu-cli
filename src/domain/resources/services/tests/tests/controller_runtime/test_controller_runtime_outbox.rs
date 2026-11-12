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
    TestControllerBehavior,
    TestControllers,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_applied_message_wakes_the_agent() {
    let harness = ControllerRuntimeHarness::finishing().await;
    let _agent = harness.start_agent();
    harness.let_agent_settle().await;

    let id = harness.apply("res-a").await;
    harness.let_agent_settle().await;
    assert_eq!(harness.script().total_calls(), 0);

    // Well within the fallback timeout, so the hint is what woke the agent
    harness.deliver_messages().await;
    harness.let_agent_settle().await;
    assert_eq!(harness.script().calls_of(&id), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_draining_delivers_the_messages_of_the_steps() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::Stepwise {
            step_wait: TimeDelta::zero(),
        },
    )
    .await;
    let id = harness.apply("res-a").await;

    harness.run_outbox_and_controllers_until_idle().await;

    assert_eq!(harness.phase(id).await, ResourcePhase::Ready);
    assert!(!harness.has_undelivered_lifecycle_messages().await);
}

#[test_log::test(tokio::test)]
async fn test_draining_returns_while_a_step_waits_for_its_time() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::Stepwise {
            step_wait: TimeDelta::minutes(1),
        },
    )
    .await;
    let id = harness.apply("res-a").await;

    harness.run_outbox_and_controllers_until_idle().await;

    assert_eq!(harness.script().calls_of(&id), 1);
    assert_eq!(harness.phase(id).await, ResourcePhase::Reconciling);
    assert!(!harness.has_undelivered_lifecycle_messages().await);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
