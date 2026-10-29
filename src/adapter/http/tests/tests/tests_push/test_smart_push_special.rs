// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu::domain::{PushError, SyncError, SyncHeadMovedError};
use kamu_adapter_http::smart_protocol::messages::{
    DatasetPushRequest,
    DatasetPushRequestError,
    TransferPlan,
};
use kamu_core::TenancyConfig;

use crate::harness::{
    ClientSideHarness,
    ClientSideHarnessOptions,
    ServerSideHarness,
    ServerSideHarnessOptions,
    ServerSideLocalFsHarness,
    await_client_server_flow,
};
use crate::tests::tests_push::scenarios::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_new_dataset_unauthenticated() {
    let scenario = SmartPushNewDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: TenancyConfig::SingleTenant,
            authenticated_remotely: false,
        })
        .await,
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authorized_writes: true,
            base_catalog: None,
        })
        .await,
    )
    .await;

    let api_server_handle = scenario.server_harness.api_server_run();

    // Unauthenticated push should fail, writing a dataset requires authentication

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                false,
                odf::DatasetVisibility::Private,
            )
            .await;

        let dataset_result = &push_result.first().unwrap().result;
        match dataset_result {
            Ok(_) => panic!(),
            Err(e) => assert_matches!(
                e,
                PushError::SyncError(SyncError::Access(odf::AccessError::Unauthenticated(_))),
            ),
        }
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_new_dataset_wrong_user() {
    let scenario = SmartPushNewDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: TenancyConfig::SingleTenant,
            authenticated_remotely: true,
        })
        .await,
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authorized_writes: true,
            base_catalog: None,
        })
        .await,
    )
    .await;

    let wrong_server_alias = odf::DatasetAlias::new(
        Some(odf::AccountName::new_unchecked("bad-account")),
        scenario.dataset_name,
    );
    let wrong_server_odf_url = scenario.server_harness.dataset_url(&wrong_server_alias);
    let wrong_server_dataset_ref = odf::DatasetRefRemote::from(&wrong_server_odf_url);

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset(
                scenario.client_dataset_ref,
                wrong_server_dataset_ref.try_into().unwrap(),
                false,
                odf::DatasetVisibility::Private,
            )
            .await;

        let dataset_result = &push_result.first().unwrap().result;
        match dataset_result {
            Ok(_) => panic!(),
            Err(e) => assert_matches!(
                e,
                PushError::SyncError(SyncError::Access(odf::AccessError::Unauthorized(_)))
            ),
        }
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_existing_dataset_unauthenticated() {
    let scenario = SmartPushExistingEvolvedDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: TenancyConfig::SingleTenant,
            authenticated_remotely: false,
        })
        .await,
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authorized_writes: false,
            base_catalog: None,
        })
        .await,
    )
    .await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                false,
                odf::DatasetVisibility::Private,
            )
            .await;

        let dataset_result = &push_result.first().unwrap().result;
        match dataset_result {
            Ok(_) => panic!(),
            Err(e) => assert_matches!(e, PushError::SyncError(SyncError::DatasetNotFound(_))),
        }
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_existing_dataset_unauthorized() {
    let scenario = SmartPushExistingEvolvedDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: TenancyConfig::SingleTenant,
            authenticated_remotely: true,
        })
        .await,
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authorized_writes: false,
            base_catalog: None,
        })
        .await,
    )
    .await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                false,
                odf::DatasetVisibility::Private,
            )
            .await;

        let dataset_result = &push_result.first().unwrap().result;
        match dataset_result {
            Ok(_) => panic!(),
            Err(e) => assert_matches!(e, PushError::SyncError(SyncError::DatasetNotFound(_))),
        }
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_existing_ref_collision() {
    let scenario = SmartPushExistingRefCollisionScenarion::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authenticated_remotely: true,
        })
        .await,
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authorized_writes: true,
            base_catalog: None,
        })
        .await,
    )
    .await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                false,
                odf::DatasetVisibility::Private,
            )
            .await;

        let dataset_result = &push_result.first().unwrap().result;
        match dataset_result {
            Ok(r) => panic!("{r:?}"),
            Err(e) => assert_matches!(e, PushError::SyncError(SyncError::RefCollision(_))),
        }
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_incompatible_version_err() {
    let scenario = SmartPushExistingRefCollisionScenarion::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authenticated_remotely: true,
        })
        .await,
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authorized_writes: true,
            base_catalog: None,
        })
        .await,
    )
    .await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let connect_result = scenario
            .client_harness
            .try_connect_to_websocket(scenario.server_dataset_ref.url().unwrap(), "push")
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
async fn test_smart_push_head_moved_concurrently() {
    let scenario = SmartPushExistingEvolvedDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: TenancyConfig::SingleTenant,
            authenticated_remotely: true,
        })
        .await,
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authorized_writes: true,
            base_catalog: None,
        })
        .await,
    )
    .await;

    let server_dataset_handle = scenario
        .server_harness
        .cli_dataset_registry()
        .resolve_dataset_handle_by_ref(&scenario.client_create_result.dataset_handle.as_local_ref())
        .await
        .unwrap();
    let concurrent_head = scenario
        .server_harness
        .append_unreferenced_block(&server_dataset_handle)
        .await;
    scenario
        .server_harness
        .move_head_before_next_commit(concurrent_head.clone());

    let stored_heads = scenario.server_harness.stored_heads();
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

        pretty_assertions::assert_eq!(push_responses.len(), 1);
        assert_matches!(
            &push_responses[0].result,
            Err(PushError::SyncError(SyncError::HeadMoved(
                SyncHeadMovedError { heads: None }
            )))
        );
        pretty_assertions::assert_eq!(
            stored_heads.get(&server_dataset_handle.id).await,
            concurrent_head
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_from_stale_head_is_rejected_with_both_heads() {
    let scenario = SmartPushExistingEvolvedDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: TenancyConfig::SingleTenant,
            authenticated_remotely: true,
        })
        .await,
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authorized_writes: true,
            base_catalog: None,
        })
        .await,
    )
    .await;

    // A pushed dataset keeps its ID on the server
    let dataset_id = &scenario.client_create_result.dataset_handle.id;
    let stored_heads = scenario.server_harness.stored_heads();
    let server_head = stored_heads.get(dataset_id).await;
    let stale_head = odf::Multihash::from_digest_sha3_256(b"stale");

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let request = DatasetPushRequest {
            current_head: Some(stale_head.clone()),
            transfer_plan: TransferPlan {
                num_blocks: 1,
                num_objects: 0,
                num_records: 0,
                bytes_in_raw_blocks: 0,
                bytes_in_raw_objects: 0,
            },
            force_update_if_diverged: false,
            visibility_for_created_dataset: odf::DatasetVisibility::Private,
        };
        let reply = scenario
            .client_harness
            .send_raw_push_request(scenario.server_dataset_ref.url().unwrap(), &request)
            .await;

        assert_matches!(
            reply,
            Err(DatasetPushRequestError::InvalidHead(e))
                if e.actual_head.as_ref() == Some(&server_head)
                    && e.expected_head.as_ref() == Some(&stale_head)
        );
        pretty_assertions::assert_eq!(stored_heads.get(dataset_id).await, server_head);
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_forced_head_moved_after_initiation() {
    let scenario = SmartPushExistingDivergedDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: TenancyConfig::SingleTenant,
            authenticated_remotely: true,
        })
        .await,
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authorized_writes: true,
            base_catalog: None,
        })
        .await,
    )
    .await;

    let server_dataset_handle = scenario
        .server_harness
        .cli_dataset_registry()
        .resolve_dataset_handle_by_ref(&scenario.client_dataset_handle.as_local_ref())
        .await
        .unwrap();
    let concurrent_head = scenario
        .server_harness
        .append_unreferenced_block(&server_dataset_handle)
        .await;
    scenario
        .server_harness
        .move_head_before_next_append(concurrent_head.clone());

    let stored_heads = scenario.server_harness.stored_heads();
    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_responses = scenario
            .client_harness
            .push_dataset(
                scenario.client_dataset_ref,
                scenario.server_dataset_ref.try_into().unwrap(),
                true, /* diverged! */
                odf::DatasetVisibility::Private,
            )
            .await;

        pretty_assertions::assert_eq!(push_responses.len(), 1);
        assert_matches!(
            &push_responses[0].result,
            Err(PushError::SyncError(SyncError::HeadMoved(
                SyncHeadMovedError { heads: None }
            )))
        );
        pretty_assertions::assert_eq!(
            stored_heads.get(&server_dataset_handle.id).await,
            concurrent_head
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
