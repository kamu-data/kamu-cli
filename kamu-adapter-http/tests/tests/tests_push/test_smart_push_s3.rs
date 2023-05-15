// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::harness::ServerSideS3Harness;
use crate::tests::tests_push::test_smart_push_shared;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_smart_push_s3_new_dataset() {
    let server_harness = ServerSideS3Harness::new().await;
    test_smart_push_shared::test_smart_push_new_dataset(server_harness).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_smart_push_s3_existing_up_to_date_dataset() {
    let server_harness = ServerSideS3Harness::new().await;
    test_smart_push_shared::test_smart_push_existing_up_to_date_dataset(server_harness).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_smart_push_s3_existing_evolved_dataset() {
    let server_harness = ServerSideS3Harness::new().await;
    test_smart_push_shared::test_smart_push_existing_evolved_dataset(server_harness).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_smart_push_s3_existing_dataset_fails_as_server_advanced() {
    let server_harness = ServerSideS3Harness::new().await;
    test_smart_push_shared::test_smart_push_existing_dataset_fails_as_server_advanced(
        server_harness,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_smart_push_s3_aborted_write_of_new_rewrite_succeeds() {
    let server_harness = ServerSideS3Harness::new().await;
    test_smart_push_shared::test_smart_push_aborted_write_of_new_rewrite_succeeds(server_harness)
        .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_smart_push_s3_aborted_write_of_updated_rewrite_succeeds() {
    let server_harness = ServerSideS3Harness::new().await;
    test_smart_push_shared::test_smart_push_aborted_write_of_updated_rewrite_succeeds(
        server_harness,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////
