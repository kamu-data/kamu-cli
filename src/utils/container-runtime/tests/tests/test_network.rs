// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use container_runtime::ContainerRuntime;

use crate::common;

#[test_group::group(containerized)]
#[tokio::test]
async fn test_network_handle_free_not_called() {
    let rt = ContainerRuntime::default();

    let network_name = common::get_random_name("kamu-test-network-");

    let network = rt.create_network(&network_name).await.unwrap();

    assert!(rt.has_network(&network_name).await.unwrap());

    // Network dropped without freeing
    // Drop will complain loudly and perform a blocking clean up
    drop(network);

    assert!(!rt.has_network(&network_name).await.unwrap());
}

#[test_group::group(containerized)]
#[tokio::test]
async fn test_network_handle_free_awaited() {
    let rt = ContainerRuntime::default();

    let network = rt
        .create_network(&common::get_random_name("kamu-test-network-"))
        .await
        .unwrap();

    network.free().await.unwrap();
}
