// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::ResourceControllerState;
use pretty_assertions::assert_eq;

use crate::tests::controller_runtime::utils::{
    ControllerRuntimeHarness,
    FAILURE_BACKOFF,
    TestControllerBehavior,
    TestControllers,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failure_backs_off_and_success_resets_it() {
    let harness =
        ControllerRuntimeHarness::new(TestControllers::Main, TestControllerBehavior::Fail).await;
    let id = harness.apply("res-a").await;
    let first_failure_at = harness.time_source().now();

    assert_eq!(harness.process_calls_until_idle().await, 1);
    let state = harness.state(id).await.unwrap();
    assert_eq!(
        (
            state.attempts,
            state.last_error.as_deref(),
            state.not_before
        ),
        (
            1,
            Some("Internal error: scripted failure"),
            Some(first_failure_at + FAILURE_BACKOFF)
        )
    );
    assert_eq!(harness.process_calls_until_idle().await, 0);

    // The second consecutive failure waits twice as long
    harness.advance_time(FAILURE_BACKOFF);
    let second_failure_at = harness.time_source().now();
    assert_eq!(harness.process_calls_until_idle().await, 1);
    let state = harness.state(id).await.unwrap();
    assert_eq!(
        (state.attempts, state.not_before),
        (2, Some(second_failure_at + FAILURE_BACKOFF * 2))
    );

    harness
        .script()
        .set_behavior_of(id, TestControllerBehavior::FinishAtCurrentVersion);
    harness.advance_time(FAILURE_BACKOFF * 2);
    assert_eq!(harness.process_calls_until_idle().await, 1);

    let version = harness.version(id).await;
    assert_eq!(
        harness.state(id).await.unwrap(),
        ResourceControllerState {
            controller: state.controller,
            resource_id: id,
            controller_revision: 1,
            handled_generation: Some(version.generation),
            handled_status_generation: Some(version.status_generation),
            not_before: None,
            attempts: 0,
            last_error: None,
        }
    );
}

#[test_log::test(tokio::test)]
async fn test_panic_fails_only_its_resource() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::FinishAtCurrentVersion,
    )
    .await;
    let panicking = harness.apply("res-panicking").await;
    let healthy = harness.apply("res-healthy").await;
    harness
        .script()
        .set_behavior_of(panicking, TestControllerBehavior::Panic);

    assert_eq!(harness.process_calls_until_idle().await, 2);

    let panicked_state = harness.state(panicking).await.unwrap();
    assert_eq!(panicked_state.attempts, 1);
    assert!(
        panicked_state
            .last_error
            .as_deref()
            .unwrap()
            .contains("scripted panic"),
        "{panicked_state:?}"
    );
    let healthy_state = harness.state(healthy).await.unwrap();
    assert_eq!(
        (healthy_state.attempts, healthy_state.not_before),
        (0, None)
    );

    assert_eq!(harness.process_calls_until_idle().await, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
