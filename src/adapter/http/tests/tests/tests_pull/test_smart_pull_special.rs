// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::PullResult;
use kamu::testing::DatasetTestHelper;
use opendatafabric::DatasetRefAny;

use crate::harness::{
    await_client_server_flow,
    ClientSideHarness,
    ClientSideHarnessOptions,
    ServerSideHarness,
    ServerSideLocalFsHarness,
};
use crate::tests::tests_pull::test_smart_pull_shared::SmartPullNewDatasetScenario;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_push_unauthenticated() {
    let scenario = SmartPullNewDatasetScenario::prepare(
        ClientSideHarness::new(ClientSideHarnessOptions {
            multi_tenant: false,
            authenticated_remotely: false,
        }),
        ServerSideLocalFsHarness::new(false).await,
    )
    .await;

    let api_server_handle = scenario.server_harness.api_server_run();

    // Unauthenticated pull should pass, reading a dataset does not require
    // authentication

    let client_handle = async {
        let pull_result = scenario
            .client_harness
            .pull_dataset_result(DatasetRefAny::from(scenario.server_dataset_ref))
            .await;

        assert_eq!(
            PullResult::Updated {
                old_head: None,
                new_head: scenario.server_commit_result.new_head,
                num_blocks: 3
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

/////////////////////////////////////////////////////////////////////////////////////////
