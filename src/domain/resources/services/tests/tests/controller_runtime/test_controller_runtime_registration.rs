// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::tests::controller_runtime::utils::{
    ControllerRuntimeHarness,
    TestControllerBehavior,
    TestControllers,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_two_controllers_for_one_schema_are_rejected() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::TwoForOneSchema,
        TestControllerBehavior::FinishAtCurrentVersion,
    )
    .await;

    let error = harness
        .try_run_outbox_and_controllers_until_idle()
        .await
        .unwrap_err();

    assert!(
        error
            .reason()
            .contains("Duplicate resource controller registered for schema"),
        "{}",
        error.reason()
    );
}

#[test_log::test(tokio::test)]
async fn test_two_controllers_with_one_name_are_rejected() {
    let harness = ControllerRuntimeHarness::new(
        TestControllers::TwoWithOneName,
        TestControllerBehavior::FinishAtCurrentVersion,
    )
    .await;

    let error = harness
        .try_run_outbox_and_controllers_until_idle()
        .await
        .unwrap_err();

    assert!(
        error
            .reason()
            .contains("Duplicate resource controller registered with name"),
        "{}",
        error.reason()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
