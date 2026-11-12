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
    TEST_STAGE_FINALIZING,
    TEST_STAGE_PREPARED,
    TestControllerBehavior,
    TestControllers,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const STEP_WAIT: TimeDelta = TimeDelta::minutes(1);

#[test_log::test(tokio::test)]
async fn test_multi_step_reconciliation_resumes_after_restart() {
    let mut harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::Stepwise {
            step_wait: STEP_WAIT,
        },
    )
    .await;
    let id = harness.apply("res-a").await;

    assert_eq!(harness.process_calls_until_idle().await, 1);
    assert_eq!(harness.phase(id).await, ResourcePhase::Reconciling);
    assert_eq!(
        harness.stage(id).await.as_deref(),
        Some(TEST_STAGE_PREPARED)
    );

    harness.restart(TestControllers::Main);

    // The restarted runtime knows from storage that the step is not due yet
    assert_eq!(harness.process_calls_until_idle().await, 0);

    harness.advance_time(STEP_WAIT);

    assert_eq!(harness.process_calls_until_idle().await, 1);
    assert_eq!(harness.phase(id).await, ResourcePhase::Ready);
    assert_eq!(harness.stage(id).await, None);
    assert_eq!(harness.process_calls_until_idle().await, 0);
}

#[test_log::test(tokio::test)]
async fn test_delayed_deletion_finalizes_after_restart() {
    let mut harness = ControllerRuntimeHarness::new(
        TestControllers::Main,
        TestControllerBehavior::Stepwise {
            step_wait: STEP_WAIT,
        },
    )
    .await;
    let id = harness.apply("res-a").await;
    harness.request_deletion(id).await;

    assert_eq!(harness.process_calls_until_idle().await, 1);
    assert_eq!(harness.phase(id).await, ResourcePhase::Deleting);
    assert_eq!(
        harness.stage(id).await.as_deref(),
        Some(TEST_STAGE_FINALIZING)
    );

    harness.restart(TestControllers::Main);
    assert_eq!(harness.process_calls_until_idle().await, 0);

    harness.advance_time(STEP_WAIT);

    assert_eq!(harness.process_calls_until_idle().await, 1);
    assert!(!harness.exists(id).await);
    assert_eq!(harness.process_calls_until_idle().await, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
