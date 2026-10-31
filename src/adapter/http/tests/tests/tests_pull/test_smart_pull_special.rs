// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu::domain::{PullError, PullResult, SyncError};
use kamu_adapter_http::smart_protocol::messages::DatasetPullRequest;
use kamu_core::TenancyConfig;

use crate::harness::{
    ClientSideHarness,
    ClientSideHarnessOptions,
    ServerSideHarness,
    ServerSideHarnessOptions,
    ServerSideLocalFsHarness,
    await_client_server_flow,
};
use crate::tests::tests_pull::scenarios::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_pull_unauthenticated() {
    let scenario = SmartPullNewDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: TenancyConfig::SingleTenant,
            authenticated_remotely: false,
        })
        .await,
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::SingleTenant,
            authorized_writes: true,
            ..ServerSideHarnessOptions::default()
        })
        .await,
    )
    .await;
    let dataset_fixture = scenario.server_harness.dataset_fixture();

    let api_server_handle = scenario.server_harness.api_server_run();

    // Unauthenticated pull should pass:
    //  reading a public dataset does not require authentication

    let client_handle = async {
        let pull_result = scenario
            .client_harness
            .pull_dataset_result(odf::DatasetRefAny::from(scenario.server_dataset_ref), false)
            .await;

        pretty_assertions::assert_eq!(
            PullResult::Updated {
                old_head: None,
                new_head: scenario.server_commit_result.new_head,
                has_more: false,
            },
            pull_result
        );

        dataset_fixture
            .assert_dataset_in_sync(
                &scenario.server_dataset_handle,
                &scenario.client_dataset_layout,
            )
            .await
            .unwrap();
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_pull_incompatible_version_err() {
    let scenario = SmartPullNewDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authenticated_remotely: true,
        })
        .await,
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authorized_writes: true,
            ..ServerSideHarnessOptions::default()
        })
        .await,
    )
    .await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let connect_result = scenario
            .client_harness
            .try_connect_to_websocket(scenario.server_dataset_ref.url().unwrap(), "pull")
            .await;

        assert_matches!(
            connect_result,
            Err(msg)
                if msg == r#"{"message":"Incompatible client version"}"#
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_pull_of_missing_dataset_fails() {
    let scenario = SmartPullNewDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authenticated_remotely: true,
        })
        .await,
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authorized_writes: true,
            ..ServerSideHarnessOptions::default()
        })
        .await,
    )
    .await;
    let missing_dataset_url = scenario.server_harness.dataset_url(&odf::DatasetAlias::new(
        Some(scenario.server_harness.api_server_account().account_name),
        odf::DatasetName::new_unchecked("missing"),
    ));

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let pull_responses = scenario
            .client_harness
            .pull_datasets(odf::DatasetRefAny::from(missing_dataset_url), false)
            .await;

        assert_matches!(
            &pull_responses[0].result,
            Err(PullError::SyncError(SyncError::DatasetNotFound(_)))
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The real client requests the metadata right after the plan, so only a raw
/// session lets the server's `HEAD` move in between
#[test_log::test(tokio::test)]
async fn test_smart_pull_metadata_ends_at_planned_head() {
    let scenario = SmartPullExistingEvolvedDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: TenancyConfig::SingleTenant,
            authenticated_remotely: true,
        })
        .await,
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::SingleTenant,
            authorized_writes: true,
            ..ServerSideHarnessOptions::default()
        })
        .await,
    )
    .await;
    let dataset_handle = &scenario.server_create_result.dataset_handle;
    let server_commits = scenario.server_harness.server_commits();
    let stored_heads = scenario.server_harness.stored_heads();

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let mut session = scenario
            .client_harness
            .open_raw_pull_session(scenario.server_dataset_ref.url().unwrap())
            .await;
        let transfer_plan = session
            .plan_pull(DatasetPullRequest {
                begin_after: Some(scenario.server_create_result.head.clone()),
                stop_at: None,
                force_update_if_diverged: false,
            })
            .await;

        let concurrent_head = server_commits.commit_set_info(dataset_handle).await;
        pretty_assertions::assert_eq!(stored_heads.get(&dataset_handle.id).await, concurrent_head);

        let metadata = session.request_metadata().await;

        pretty_assertions::assert_eq!(transfer_plan.num_blocks, 2);
        pretty_assertions::assert_eq!(metadata.num_blocks, transfer_plan.num_blocks);
        pretty_assertions::assert_eq!(
            metadata.block_hashes,
            vec![
                scenario.server_commit_result.old_head.clone().unwrap(),
                scenario.server_commit_result.new_head.clone(),
            ]
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_pull_head_moved_concurrently() {
    HeadMovedConcurrentlyCase { force: false }
        .assert_pull_fails()
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_pull_forced_head_moved_concurrently() {
    HeadMovedConcurrentlyCase { force: true }
        .assert_pull_fails()
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `HEAD` moves after the pull read it: the commit fails and keeps the
/// concurrent head, with or without `force`
struct HeadMovedConcurrentlyCase {
    force: bool,
}

impl HeadMovedConcurrentlyCase {
    async fn assert_pull_fails(self) {
        let scenario = SmartPullExistingEvolvedDatasetScenario::prepare(
            ClientSideHarness::new(ClientSideHarnessOptions {
                tenancy_config: TenancyConfig::SingleTenant,
                authenticated_remotely: true,
            })
            .await,
            ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
                tenancy_config: TenancyConfig::SingleTenant,
                authorized_writes: true,
                ..ServerSideHarnessOptions::default()
            })
            .await,
        )
        .await;

        let api_server_handle = scenario.server_harness.api_server_run();

        let client_handle = async {
            let dataset_handle = &scenario.server_create_result.dataset_handle;
            let concurrent_head = scenario
                .client_harness
                .append_unreferenced_block(dataset_handle)
                .await;
            scenario
                .client_harness
                .move_head_before_next_commit(concurrent_head.clone());

            let pull_responses = scenario
                .client_harness
                .pull_datasets(
                    odf::DatasetRefAny::from(scenario.server_dataset_ref),
                    self.force,
                )
                .await;

            pretty_assertions::assert_eq!(pull_responses.len(), 1);
            assert_matches!(
                &pull_responses[0].result,
                Err(PullError::HeadMoved(e))
                    if e.actual.as_ref() == Some(&concurrent_head)
            );
            pretty_assertions::assert_eq!(
                scenario
                    .client_harness
                    .stored_head(&dataset_handle.id)
                    .await,
                concurrent_head
            );
        };

        await_client_server_flow!(api_server_handle, client_handle);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
