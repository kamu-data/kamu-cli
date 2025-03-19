// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu::domain::{PushError, SyncError};
use kamu_core::TenancyConfig;

use crate::harness::{
    await_client_server_flow,
    ClientSideHarness,
    ClientSideHarnessOptions,
    ServerSideHarness,
    ServerSideHarnessOptions,
    ServerSideLocalFsHarness,
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
                PushError::SyncError(SyncError::Access(odf::AccessError::Unauthorized(_))),
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
                PushError::SyncError(SyncError::Access(odf::AccessError::Forbidden(_)))
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
