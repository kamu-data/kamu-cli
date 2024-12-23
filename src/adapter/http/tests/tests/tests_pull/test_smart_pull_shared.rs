// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
use kamu::testing::DatasetTestHelper;

use crate::harness::{await_client_server_flow, ClientSideHarness, ServerSideHarness};
use crate::tests::tests_pull::scenarios::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_pull_new_dataset<TServerHarness: ServerSideHarness>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario = SmartPullNewDatasetScenario::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();
    let client_handle = async {
        let pull_result = scenario
            .client_harness
            .pull_dataset_result(odf::DatasetRefAny::from(scenario.server_dataset_ref), false)
            .await;

        assert_eq!(
            PullResult::Updated {
                old_head: None,
                new_head: scenario.server_commit_result.new_head,
            },
            pull_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_pull_new_empty_dataset<TServerHarness: ServerSideHarness>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario =
        SmartPullNewEmptyDatasetScenario::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();
    let client_handle = async {
        let pull_result = scenario
            .client_harness
            .pull_dataset_result(odf::DatasetRefAny::from(scenario.server_dataset_ref), false)
            .await;

        assert_eq!(
            PullResult::Updated {
                old_head: None,
                new_head: scenario.server_create_result.head,
            },
            pull_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_pull_existing_up_to_date_dataset<
    TServerHarness: ServerSideHarness,
>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario =
        SmartPullExistingUpToDateDatasetScenario::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();
    let client_handle = async {
        let pull_result = scenario
            .client_harness
            .pull_dataset_result(odf::DatasetRefAny::from(scenario.server_dataset_ref), false)
            .await;

        assert_eq!(PullResult::UpToDate(PullResultUpToDate::Sync), pull_result);

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_pull_existing_evolved_dataset<TServerHarness: ServerSideHarness>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario =
        SmartPullExistingEvolvedDatasetScenario::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();
    let client_handle = async {
        let pull_result = scenario
            .client_harness
            .pull_dataset_result(odf::DatasetRefAny::from(scenario.server_dataset_ref), false)
            .await;

        assert_eq!(
            PullResult::Updated {
                old_head: Some(scenario.server_create_result.head),
                new_head: scenario.server_commit_result.new_head,
            },
            pull_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_pull_existing_diverged_dataset<TServerHarness: ServerSideHarness>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario =
        SmartPullExistingDivergedDatasetScenario::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();
    let client_handle = async {
        let pull_result = scenario
            .client_harness
            .pull_dataset_result(
                odf::DatasetRefAny::from(scenario.server_dataset_ref),
                true, /* diverged! */
            )
            .await;

        let new_head = match scenario.server_compaction_result {
            CompactionResult::NothingToDo => panic!("unexpected compaction result"),
            CompactionResult::Success { new_head, .. } => new_head,
        };

        assert_eq!(
            PullResult::Updated {
                old_head: Some(scenario.server_precompaction_result.new_head),
                new_head,
            },
            pull_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_pull_existing_advanced_dataset_fails<
    TServerHarness: ServerSideHarness,
>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario =
        SmartPullExistingAdvancedDatasetFailsScenario::prepare(a_client_harness, a_server_harness)
            .await;

    let api_server_handle = scenario.server_harness.api_server_run();
    let client_handle = async {
        let pull_responses = scenario
            .client_harness
            .pull_datasets(odf::DatasetRefAny::from(scenario.server_dataset_ref), false)
            .await;

        // TODO: try expecting better error message
        assert!(pull_responses[0].result.is_err());
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_pull_aborted_read_of_new_reread_succeeds<
    TServerHarness: ServerSideHarness,
>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario = SmartPullAbortedReadOfNewRereadSucceedsScenario::prepare(
        a_client_harness,
        a_server_harness,
    )
    .await;

    let api_server_handle = scenario.server_harness.api_server_run();
    let client_handle = async {
        let pull_result = scenario
            .client_harness
            .pull_dataset_result(odf::DatasetRefAny::from(scenario.server_dataset_ref), false)
            .await;

        assert_eq!(
            PullResult::Updated {
                old_head: None,
                new_head: scenario.server_commit_result.new_head,
            },
            pull_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_pull_aborted_read_of_existing_evolved_dataset_reread_succeeds<
    TServerHarness: ServerSideHarness,
>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario = SmartPullAbortedReadOfExistingEvolvedRereadSucceedsScenario::prepare(
        a_client_harness,
        a_server_harness,
    )
    .await;

    let api_server_handle = scenario.server_harness.api_server_run();
    let client_handle = async {
        let pull_result = scenario
            .client_harness
            .pull_dataset_result(odf::DatasetRefAny::from(scenario.server_dataset_ref), false)
            .await;

        assert_eq!(
            PullResult::Updated {
                old_head: Some(scenario.server_create_result.head),
                new_head: scenario.server_commit_result.new_head,
            },
            pull_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
