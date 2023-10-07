// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod test_authentication_layer;
mod test_dataset_authorization_layer;
mod test_routing;

mod tests_pull;
mod tests_push;

/////////////////////////////////////////////////////////////////////////////////////////

macro_rules! test_client_server_local_fs_harness_permutations {
    ($test_template: expr) => {
        $test_template(
            ClientSideHarness::new(false),
            ServerSideLocalFsHarness::new(false).await,
        )
        .await;

        $test_template(
            ClientSideHarness::new(true),
            ServerSideLocalFsHarness::new(false).await,
        )
        .await;

        $test_template(
            ClientSideHarness::new(false),
            ServerSideLocalFsHarness::new(true).await,
        )
        .await;

        $test_template(
            ClientSideHarness::new(true),
            ServerSideLocalFsHarness::new(true).await,
        )
        .await;
    };
}

macro_rules! test_client_server_s3_harness_permutations {
    ($test_template: expr) => {
        $test_template(
            ClientSideHarness::new(false),
            ServerSideS3Harness::new(false).await,
        )
        .await;

        $test_template(
            ClientSideHarness::new(true),
            ServerSideS3Harness::new(false).await,
        )
        .await;

        $test_template(
            ClientSideHarness::new(false),
            ServerSideS3Harness::new(true).await,
        )
        .await;

        $test_template(
            ClientSideHarness::new(true),
            ServerSideS3Harness::new(true).await,
        )
        .await;
    };
}

pub(crate) use {
    test_client_server_local_fs_harness_permutations,
    test_client_server_s3_harness_permutations,
};

/////////////////////////////////////////////////////////////////////////////////////////
