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
use crate::tests::tests_push::scenarios::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_push_new_dataset<TServerHarness: ServerSideHarness>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario = SmartPushNewDatasetScenario::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset_result(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                false,
                odf::DatasetVisibility::Private,
            )
            .await;

        assert_eq!(
            SyncResult::Updated {
                old_head: None,
                new_head: scenario.client_commit_result.new_head,
                num_blocks: 4,
            },
            push_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_push_new_dataset_as_public<TServerHarness: ServerSideHarness>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario = SmartPushNewDatasetScenario::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset_result(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                false,
                odf::DatasetVisibility::Public,
            )
            .await;

        assert_eq!(
            SyncResult::Updated {
                old_head: None,
                new_head: scenario.client_commit_result.new_head,
                num_blocks: 4,
            },
            push_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_push_new_empty_dataset<TServerHarness: ServerSideHarness>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario =
        SmartPushNewEmptyDatasetScenario::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset_result(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                false,
                odf::DatasetVisibility::Private,
            )
            .await;

        assert_eq!(
            SyncResult::Updated {
                old_head: None,
                new_head: scenario.client_create_result.head,
                num_blocks: 1,
            },
            push_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_push_existing_up_to_date_dataset<
    TServerHarness: ServerSideHarness,
>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario =
        SmartPushExistingUpToDateDatasetScenario::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();
    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset_result(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                false,
                odf::DatasetVisibility::Private,
            )
            .await;

        assert_eq!(SyncResult::UpToDate {}, push_result);

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_push_existing_evolved_dataset<TServerHarness: ServerSideHarness>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario =
        SmartPushExistingEvolvedDatasetScenario::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset_result(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                false,
                odf::DatasetVisibility::Private,
            )
            .await;

        assert_eq!(
            SyncResult::Updated {
                old_head: Some(scenario.client_create_result.head),
                new_head: scenario.client_commit_result.new_head,
                num_blocks: 2,
            },
            push_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_push_existing_diverged_dataset<TServerHarness: ServerSideHarness>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario =
        SmartPushExistingDivergedDatasetScenario::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset_result(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                true, /* diverged! */
                odf::DatasetVisibility::Private,
            )
            .await;

        let new_head = match scenario.client_compaction_result {
            CompactionResult::NothingToDo => panic!("unexpected compaction result"),
            CompactionResult::Success { new_head, .. } => new_head,
        };

        assert_eq!(
            SyncResult::Updated {
                old_head: Some(scenario.client_precompaction_result.new_head),
                new_head,
                num_blocks: 4,
            },
            push_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_push_existing_dataset_fails_as_server_advanced<
    TServerHarness: ServerSideHarness,
>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario = SmartPushExistingDatasetFailsAsServerAdvancedScenario::prepare(
        a_client_harness,
        a_server_harness,
    )
    .await;

    let api_server_handle = scenario.server_harness.api_server_run();
    let client_handle = async {
        let push_responses = scenario
            .client_harness
            .push_dataset(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                false,
                odf::DatasetVisibility::Private,
            )
            .await;

        // TODO: try expecting better error message
        assert!(push_responses[0].result.is_err());
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_push_aborted_write_of_new_rewrite_succeeds<
    TServerHarness: ServerSideHarness,
>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario =
        SmartPushAbortedWriteOfNewWriteSucceeds::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();
    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset_result(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                false,
                odf::DatasetVisibility::Private,
            )
            .await;

        assert_eq!(
            SyncResult::Updated {
                old_head: None,
                new_head: scenario.client_commit_result.new_head,
                num_blocks: 4,
            },
            push_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_push_aborted_write_of_updated_rewrite_succeeds<
    TServerHarness: ServerSideHarness,
>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario =
        SmartPushAbortedWriteOfUpdatedWriteSucceeds::prepare(a_client_harness, a_server_harness)
            .await;

    let api_server_handle = scenario.server_harness.api_server_run();
    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset_result(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                false,
                odf::DatasetVisibility::Private,
            )
            .await;

        assert_eq!(
            SyncResult::Updated {
                old_head: Some(scenario.client_create_result.head),
                new_head: scenario.client_commit_result.new_head,
                num_blocks: 2,
            },
            push_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn test_smart_push_via_repo_ref<TServerHarness: ServerSideHarness>(
    a_client_harness: ClientSideHarness,
    a_server_harness: TServerHarness,
) {
    let scenario =
        SmartPushNewDatasetViaRepoRefScenario::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset_result(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref,
                false,
                odf::DatasetVisibility::Private,
            )
            .await;

        assert_eq!(
            SyncResult::Updated {
                old_head: None,
                new_head: scenario.client_commit_result.new_head,
                num_blocks: 4,
            },
            push_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
