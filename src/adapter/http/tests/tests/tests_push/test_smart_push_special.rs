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

use crate::harness::{
    await_client_server_flow,
    ClientSideHarness,
    ClientSideHarnessOptions,
    ServerSideHarness,
    ServerSideLocalFsHarness,
};
use crate::tests::tests_push::test_smart_push_shared::SmartPushNewDatasetScenario;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_unauthenticated() {
    let scenario = SmartPushNewDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            multi_tenant: false,
            authenticated_remotely: false,
        }),
        ServerSideLocalFsHarness::new(false).await,
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
