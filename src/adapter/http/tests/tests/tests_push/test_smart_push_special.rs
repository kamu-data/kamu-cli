// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu::domain::{AccessError, PushError, SyncError};
use opendatafabric::{AccountName, DatasetAlias, DatasetRefRemote};

use crate::harness::{
    await_client_server_flow,
    ClientSideHarness,
    ClientSideHarnessOptions,
    ServerSideHarness,
    ServerSideHarnessOptions,
    ServerSideLocalFsHarness,
};
use crate::tests::tests_push::scenarios::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_new_dataset_unauthenticated() {
    let scenario = SmartPushNewDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            multi_tenant: false,
            authenticated_remotely: false,
        }),
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            multi_tenant: true,
            authorized_writes: true,
        })
        .await,
    )
    .await;

    let api_server_handle = scenario.server_harness.api_server_run();

    // Unauthenticated push should fail, writing a dataset requires authentication

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset(scenario.client_dataset_ref, scenario.server_dataset_ref)
            .await;

        let dataset_result = &push_result.get(0).unwrap().result;

        assert_matches!(
            dataset_result,
            Err(PushError::SyncError(SyncError::Access(
                AccessError::Unauthorized(_)
            )))
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_new_dataset_wrong_user() {
    let scenario = SmartPushNewDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            multi_tenant: false,
            authenticated_remotely: true,
        }),
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            multi_tenant: true,
            authorized_writes: true,
        })
        .await,
    )
    .await;

    let wrong_server_alias = DatasetAlias::new(
        Some(AccountName::new_unchecked("bad-account")),
        scenario.dataset_name,
    );
    let wrong_server_odf_url = scenario.server_harness.dataset_url(&wrong_server_alias);
    let wrong_server_dataset_ref = DatasetRefRemote::from(&wrong_server_odf_url);

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset(scenario.client_dataset_ref, wrong_server_dataset_ref)
            .await;

        let dataset_result = &push_result.get(0).unwrap().result;

        assert_matches!(
            dataset_result,
            Err(PushError::SyncError(SyncError::Access(
                AccessError::Forbidden(_)
            )))
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_existing_dataset_unauthenticated() {
    let scenario = SmartPushExistingEvolvedDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            multi_tenant: false,
            authenticated_remotely: false,
        }),
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            multi_tenant: true,
            authorized_writes: false,
        })
        .await,
    )
    .await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset(scenario.client_dataset_ref, scenario.server_dataset_ref)
            .await;

        let dataset_result = &push_result.get(0).unwrap().result;

        assert_matches!(
            dataset_result,
            Err(PushError::SyncError(SyncError::Access(
                AccessError::Unauthorized(_)
            )))
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_existing_dataset_unauthorized() {
    let scenario = SmartPushExistingEvolvedDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            multi_tenant: false,
            authenticated_remotely: true,
        }),
        ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            multi_tenant: true,
            authorized_writes: false,
        })
        .await,
    )
    .await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset(scenario.client_dataset_ref, scenario.server_dataset_ref)
            .await;

        let dataset_result = &push_result.get(0).unwrap().result;

        assert_matches!(
            dataset_result,
            Err(PushError::SyncError(SyncError::Access(
                AccessError::Forbidden(_)
            )))
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////
