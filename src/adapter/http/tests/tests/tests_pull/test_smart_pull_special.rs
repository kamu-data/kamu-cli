// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu::domain::{
    CorruptedSourceError,
    DatasetIdMismatchError,
    PullError,
    PullResult,
    SyncError,
    SyncResult,
};
use kamu_adapter_http::smart_protocol::messages::DatasetPullRequest;
use kamu_core::TenancyConfig;

use crate::harness::{
    LocalFsTransferOptions,
    MetadataScript,
    ObjectsScript,
    PullScript,
    ScriptedPullServer,
    ServerSideHarness,
    await_client_server_flow,
};
use crate::tests::tests_pull::scenarios::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_pull_unauthenticated() {
    let (client_harness, server_harness) = LocalFsTransferOptions {
        client_tenancy: TenancyConfig::SingleTenant,
        authenticated_remotely: false,
        server_tenancy: TenancyConfig::SingleTenant,
        authorized_writes: true,
    }
    .build()
    .await;
    let scenario = SmartPullNewDatasetScenario::prepare(client_harness, server_harness).await;
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
    let (client_harness, server_harness) = LocalFsTransferOptions {
        client_tenancy: TenancyConfig::MultiTenant,
        authenticated_remotely: true,
        server_tenancy: TenancyConfig::MultiTenant,
        authorized_writes: true,
    }
    .build()
    .await;
    let scenario = SmartPullNewDatasetScenario::prepare(client_harness, server_harness).await;

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
    let (client_harness, server_harness) = LocalFsTransferOptions {
        client_tenancy: TenancyConfig::MultiTenant,
        authenticated_remotely: true,
        server_tenancy: TenancyConfig::MultiTenant,
        authorized_writes: true,
    }
    .build()
    .await;
    let scenario = SmartPullNewDatasetScenario::prepare(client_harness, server_harness).await;
    let missing_dataset_url = scenario.server_harness.dataset_url(&odf::DatasetAlias::new(
        Some(
            scenario
                .server_harness
                .server_account()
                .account_name
                .clone(),
        ),
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

#[test_log::test(tokio::test)]
async fn test_smart_pull_without_referenced_objects_is_not_committed() {
    // The new data block references a data file and a checkpoint, and neither is sent
    ScriptedPullCase {
        script: PullScript::Objects(ObjectsScript::Omit),
        expected_message: "Server did not send 2 file(s)",
    }
    .assert_pull_fails()
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The object type picks the repository a file is stored in, so a file sent
/// as the other type does not count as sent
#[test_log::test(tokio::test)]
async fn test_smart_pull_with_mislabeled_objects_is_not_committed() {
    ScriptedPullCase {
        script: PullScript::Objects(ObjectsScript::Mislabel),
        expected_message: "Server did not send 2 file(s)",
    }
    .assert_pull_fails()
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_pull_with_corrupted_objects_is_not_committed() {
    ScriptedPullCase {
        script: PullScript::Objects(ObjectsScript::Corrupt),
        expected_message: "file hash didn't match the computed",
    }
    .assert_pull_fails()
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_pull_with_undecodable_metadata_is_not_committed() {
    ScriptedPullCase {
        script: PullScript::Metadata(MetadataScript::Undecodable),
        expected_message: "Server sent a metadata batch that cannot be read",
    }
    .assert_pull_fails()
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_pull_without_planned_blocks_is_not_committed() {
    ScriptedPullCase {
        script: PullScript::Metadata(MetadataScript::NoBlocks),
        expected_message: "Server sent no blocks for a non-empty transfer plan",
    }
    .assert_pull_fails()
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The commit compares `HEAD` only with the head the pull started from, so a
/// batch the server bases on an older block would replace the blocks after it
#[test_log::test(tokio::test)]
async fn test_smart_pull_of_blocks_not_extending_local_head_is_not_committed() {
    let (client_harness, server_harness) = LocalFsTransferOptions {
        client_tenancy: TenancyConfig::SingleTenant,
        authenticated_remotely: false,
        server_tenancy: TenancyConfig::SingleTenant,
        authorized_writes: true,
    }
    .build()
    .await;
    let scenario =
        SmartPullExistingDivergedDatasetScenario::prepare(client_harness, server_harness).await;
    let dataset_id = &scenario.server_dataset_handle.id;
    let client_head = scenario.client_harness.stored_head(dataset_id).await;
    let server = ScriptedPullServer::new(
        scenario.server_harness.cli_dataset_registry().as_ref(),
        &scenario.server_dataset_handle,
        PullScript::Metadata(MetadataScript::AfterSeed),
    )
    .await;
    let server_dataset_url = server.dataset_url("foo");

    let server_handle = server.run();

    let client_handle = async {
        let pull_responses = scenario
            .client_harness
            .pull_datasets(odf::DatasetRefAny::from(server_dataset_url), false)
            .await;

        assert_matches!(
            &pull_responses[0].result,
            Err(PullError::SyncError(SyncError::Corrupted(CorruptedSourceError { message, .. })))
                if message.contains("Server sent blocks that do not extend the local head")
        );
        pretty_assertions::assert_eq!(
            scenario.client_harness.stored_head(dataset_id).await,
            client_head
        );
    };

    await_client_server_flow!(server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_pull_into_existing_reports_increment() {
    let (client_harness, server_harness) = LocalFsTransferOptions {
        client_tenancy: TenancyConfig::SingleTenant,
        authenticated_remotely: true,
        server_tenancy: TenancyConfig::SingleTenant,
        authorized_writes: true,
    }
    .build()
    .await;
    let scenario =
        SmartPullExistingEvolvedDatasetScenario::prepare(client_harness, server_harness).await;
    let dataset_handle = &scenario.server_create_result.dataset_handle;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let sync_result = scenario
            .client_harness
            .sync_into_existing(scenario.server_dataset_ref.clone(), dataset_handle)
            .await;

        // The server added a `SetInfo` and an `AddData` of 10 records
        pretty_assertions::assert_eq!(
            sync_result,
            SyncResult::Updated {
                old_head: Some(scenario.server_create_result.head.clone()),
                new_head: scenario.server_commit_result.new_head.clone(),
                num_blocks: 2,
                increment: odf::dataset::MetadataChainIncrementInterval {
                    num_blocks: 2,
                    num_records: 10,
                    updated_watermark: None,
                },
            }
        );
        pretty_assertions::assert_eq!(
            scenario
                .client_harness
                .stored_head(&dataset_handle.id)
                .await,
            scenario.server_commit_result.new_head
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A dataset left without `HEAD` receives the whole chain, `Seed` included, so
/// the `Seed` of another dataset must be refused. With `force`, the ID
/// comparison before the transfer refuses it first
#[test_log::test(tokio::test)]
async fn test_smart_pull_into_headless_dataset_of_another_id_fails() {
    let (client_harness, server_harness) = LocalFsTransferOptions {
        client_tenancy: TenancyConfig::SingleTenant,
        authenticated_remotely: true,
        server_tenancy: TenancyConfig::SingleTenant,
        authorized_writes: true,
    }
    .build()
    .await;
    let scenario = SmartPullNewDatasetScenario::prepare(client_harness, server_harness).await;
    let headless_dataset_handle = scenario
        .client_harness
        .create_headless_dataset(scenario.server_dataset_handle.alias.dataset_name.as_str())
        .await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        for force in [false, true] {
            let pull_responses = scenario
                .client_harness
                .pull_datasets(
                    odf::DatasetRefAny::from(scenario.server_dataset_ref.clone()),
                    force,
                )
                .await;

            pretty_assertions::assert_eq!(pull_responses.len(), 1);
            assert_matches!(
                &pull_responses[0].result,
                Err(PullError::SyncError(SyncError::DatasetIdMismatch(DatasetIdMismatchError {
                    src_dataset_id,
                    dst_dataset_id,
                })))
                if *src_dataset_id == scenario.server_dataset_handle.id
                    && *dst_dataset_id == headless_dataset_handle.id
            );
            pretty_assertions::assert_eq!(
                scenario
                    .client_harness
                    .find_stored_head(&headless_dataset_handle.id)
                    .await,
                None
            );
        }
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The real client requests the metadata right after the plan, so only a raw
/// session lets the server's `HEAD` move in between
#[test_log::test(tokio::test)]
async fn test_smart_pull_metadata_ends_at_planned_head() {
    let (client_harness, server_harness) = LocalFsTransferOptions {
        client_tenancy: TenancyConfig::SingleTenant,
        authenticated_remotely: true,
        server_tenancy: TenancyConfig::SingleTenant,
        authorized_writes: true,
    }
    .build()
    .await;
    let scenario =
        SmartPullExistingEvolvedDatasetScenario::prepare(client_harness, server_harness).await;
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
async fn test_smart_pull_ref_moved_concurrently() {
    RefMovedConcurrentlyCase { force: false }
        .assert_pull_fails()
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_pull_forced_ref_moved_concurrently() {
    RefMovedConcurrentlyCase { force: true }
        .assert_pull_fails()
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `HEAD` moves after the pull read it: the commit fails and keeps the
/// concurrent head, with or without `force`
struct RefMovedConcurrentlyCase {
    force: bool,
}

impl RefMovedConcurrentlyCase {
    async fn assert_pull_fails(self) {
        let (client_harness, server_harness) = LocalFsTransferOptions {
            client_tenancy: TenancyConfig::SingleTenant,
            authenticated_remotely: true,
            server_tenancy: TenancyConfig::SingleTenant,
            authorized_writes: true,
        }
        .build()
        .await;
        let scenario =
            SmartPullExistingEvolvedDatasetScenario::prepare(client_harness, server_harness).await;

        let api_server_handle = scenario.server_harness.api_server_run();

        let client_handle = async {
            let dataset_handle = &scenario.server_create_result.dataset_handle;
            let concurrent_head = scenario
                .client_harness
                .append_unreferenced_block(dataset_handle)
                .await;
            scenario
                .client_harness
                .move_reference_before_next_update(concurrent_head.clone());

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
                Err(PullError::RefMoved(e))
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

/// A server whose transfer of a new dataset breaks the way the script says,
/// the same way on every retry
struct ScriptedPullCase {
    script: PullScript,
    expected_message: &'static str,
}

impl ScriptedPullCase {
    async fn assert_pull_fails(self) {
        let (client_harness, server_harness) = LocalFsTransferOptions {
            client_tenancy: TenancyConfig::SingleTenant,
            authenticated_remotely: false,
            server_tenancy: TenancyConfig::SingleTenant,
            authorized_writes: true,
        }
        .build()
        .await;
        let scenario = SmartPullNewDatasetScenario::prepare(client_harness, server_harness).await;
        let server = ScriptedPullServer::new(
            scenario.server_harness.cli_dataset_registry().as_ref(),
            &scenario.server_dataset_handle,
            self.script,
        )
        .await;
        let server_dataset_url = server.dataset_url("foo");

        let server_handle = server.run();

        let client_handle = async {
            let pull_responses = scenario
                .client_harness
                .pull_datasets(odf::DatasetRefAny::from(server_dataset_url), false)
                .await;

            assert_matches!(
                &pull_responses[0].result,
                Err(PullError::SyncError(e @ SyncError::Corrupted(CorruptedSourceError { message, .. })))
                    if message.contains(self.expected_message) && !e.is_recoverable()
            );
            pretty_assertions::assert_eq!(
                scenario
                    .client_harness
                    .find_stored_head(&scenario.server_dataset_handle.id)
                    .await,
                None
            );
        };

        await_client_server_flow!(server_handle, client_handle);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
