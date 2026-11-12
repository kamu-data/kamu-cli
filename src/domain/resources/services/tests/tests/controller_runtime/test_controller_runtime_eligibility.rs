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
    FAILURE_BACKOFF,
    TestControllerBehavior,
    TestControllers,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_done_suppresses_until_the_generation_changes() {
    let harness = ControllerRuntimeHarness::finishing().await;
    let id = harness.apply("res-a").await;

    assert_eq!(harness.process_calls_until_idle().await, 1);
    assert_eq!(harness.process_calls_until_idle().await, 0);

    harness.update_spec(id, "changed").await;

    assert_eq!(harness.process_calls_until_idle().await, 1);
    assert_eq!(harness.process_calls_until_idle().await, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_done_suppresses_until_the_status_changes() {
    let harness = ControllerRuntimeHarness::finishing().await;
    let id = harness.apply("res-a").await;
    assert_eq!(harness.process_calls_until_idle().await, 1);

    // The resource is still not reconciled, so the predicate selects it again
    // once the status write lifts the suppression
    harness.write_stage_as_other_writer(id, "external").await;

    assert_eq!(harness.process_calls_until_idle().await, 1);
    assert_eq!(harness.process_calls_until_idle().await, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_status_change_of_reconciled_resource_wakes_only_a_status_reacting_controller() {
    for (controllers, expected_calls) in [
        (TestControllers::Main, 0),
        (TestControllers::StatusReacting, 1),
    ] {
        let harness = ControllerRuntimeHarness::new(
            controllers,
            TestControllerBehavior::Stepwise {
                step_wait: TimeDelta::zero(),
            },
        )
        .await;
        let id = harness.apply("res-a").await;
        harness.run_outbox_and_controllers_until_idle().await;
        assert_eq!(harness.phase(id).await, ResourcePhase::Ready);

        harness.write_stage_as_other_writer(id, "external").await;

        assert_eq!(
            harness.process_calls_until_idle().await,
            expected_calls,
            "{controllers:?}"
        );
        assert_eq!(
            harness.process_calls_until_idle().await,
            0,
            "{controllers:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_revision_bump_reconsiders_finished_resources_a_predicate_selects() {
    let mut harness = ControllerRuntimeHarness::finishing().await;
    // Finished with but never reconciled: the predicate keeps selecting it
    let finished = harness.apply("res-finished").await;
    // Reconciled: no predicate selects it
    let ready = harness.apply("res-ready").await;
    harness.script().set_behavior_of(
        ready,
        TestControllerBehavior::Stepwise {
            step_wait: TimeDelta::zero(),
        },
    );
    harness.run_outbox_and_controllers_until_idle().await;
    assert_eq!(harness.phase(ready).await, ResourcePhase::Ready);
    let ready_calls = harness.script().calls_of(&ready);

    harness.restart(TestControllers::MainRevision2);

    assert_eq!(harness.process_calls_until_idle().await, 1);
    assert_eq!(harness.script().calls_of(&finished), 2);
    assert_eq!(harness.script().calls_of(&ready), ready_calls);
    assert_eq!(
        harness.state(finished).await.unwrap().controller_revision,
        2
    );
    assert_eq!(harness.process_calls_until_idle().await, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_steps_due_at_once_run_back_to_back_in_one_drain() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::Stepwise {
            step_wait: TimeDelta::zero(),
        },
    )
    .await;
    let id = harness.apply("res-a").await;

    assert_eq!(harness.process_calls_until_idle().await, 2);
    assert_eq!(harness.phase(id).await, ResourcePhase::Ready);
    assert_eq!(harness.process_calls_until_idle().await, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_retry_at_waits_until_due() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::RetryAfter(TimeDelta::minutes(1)),
    )
    .await;
    let id = harness.apply("res-a").await;

    assert_eq!(harness.process_calls_until_idle().await, 1);
    assert_eq!(harness.process_calls_until_idle().await, 0);

    harness.advance_time(TimeDelta::minutes(1));

    assert_eq!(harness.process_calls_until_idle().await, 1);
    assert_eq!(harness.script().calls_of(&id), 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_drain_goes_through_more_resources_than_one_batch() {
    let harness = ControllerRuntimeHarness::finishing().await;
    // The harness batch holds 3
    for i in 0..7 {
        harness.apply(&format!("res-{i}")).await;
    }

    assert_eq!(harness.process_calls_until_idle().await, 7);
    assert_eq!(harness.process_calls_until_idle().await, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_extra_resource_lifts_done_but_not_backoff() {
    let harness = ControllerRuntimeHarness::finishing().await;
    let id = harness.apply("res-a").await;
    assert_eq!(harness.process_calls_until_idle().await, 1);

    // Still listed after `Done`: asked once per drain, never twice in one
    harness.script().set_extras(vec![id]);
    assert_eq!(harness.process_calls_until_idle().await, 1);
    assert_eq!(harness.process_calls_until_idle().await, 1);

    // A failing extra resource waits for its backoff
    harness
        .script()
        .set_behavior_of(id, TestControllerBehavior::Fail);
    assert_eq!(harness.process_calls_until_idle().await, 1);
    assert_eq!(harness.process_calls_until_idle().await, 0);

    harness.advance_time(FAILURE_BACKOFF);
    assert_eq!(harness.process_calls_until_idle().await, 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_backing_off_extra_resources_do_not_hide_the_ones_listed_after_them() {
    let harness = ControllerRuntimeHarness::finishing().await;
    // The harness batch holds 3: as many failing extra resources as that,
    // listed first, then a healthy one
    let mut ids = Vec::new();
    for i in 0..4 {
        ids.push(harness.apply(&format!("res-{i}")).await);
    }
    harness.run_outbox_and_controllers_until_idle().await;

    let (failing, healthy) = (&ids[..3], ids[3]);
    for id in failing {
        harness
            .script()
            .set_behavior_of(*id, TestControllerBehavior::Fail);
    }
    harness.script().set_extras(ids.clone());

    harness.run_outbox_and_controllers_until_idle().await;
    assert_eq!(harness.script().calls_of(&healthy), 2);

    // Reached again while the failing ones back off
    harness.run_outbox_and_controllers_until_idle().await;
    assert_eq!(harness.script().calls_of(&healthy), 3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_finished_extra_resource_that_changes_is_processed_again_in_one_session() {
    let harness = ControllerRuntimeHarness::finishing().await;
    let id = harness.apply("res-a").await;
    harness.run_outbox_and_controllers_until_idle().await;

    let mut session = harness.new_drain_session();
    harness.script().set_extras(vec![id]);
    assert_eq!(harness.deliver_and_drain(&mut session).await, 1);

    // The controller took it out of its extra set, and then it changed
    harness.script().set_extras(vec![]);
    harness.update_spec(id, "v2").await;

    assert_eq!(harness.deliver_and_drain(&mut session).await, 1);
    assert_eq!(harness.deliver_and_drain(&mut session).await, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
