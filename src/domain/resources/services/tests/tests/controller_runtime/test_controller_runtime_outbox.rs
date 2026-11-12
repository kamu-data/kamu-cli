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
async fn test_steps_due_at_once_run_in_one_drain_and_their_messages_are_delivered() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::Stepwise {
            step_wait: TimeDelta::zero(),
        },
    )
    .await;
    let id = harness.apply("res-a").await;

    let mut session = harness.new_drain_session();
    assert_eq!(harness.deliver_and_drain(&mut session).await, 2);
    assert_eq!(harness.phase(id).await, ResourcePhase::Ready);

    // Draining until idle delivers the messages the steps posted
    assert_eq!(harness.process_calls_until_idle().await, 0);
    assert!(!harness.has_undelivered_lifecycle_messages().await);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
