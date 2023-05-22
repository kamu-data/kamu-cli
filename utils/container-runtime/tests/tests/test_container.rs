// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::time::Duration;

use container_runtime::{ContainerRuntime, ContainerRuntimeError, TerminateStatus};

const TEST_IMAGE: &str = "docker.io/busybox:latest";

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_container_terminate_not_called() {
    let rt = ContainerRuntime::default();

    rt.ensure_image(TEST_IMAGE, None).await.unwrap();

    let container = rt
        .run_attached(TEST_IMAGE)
        .container_name_prefix("kamu-test-")
        .args(["sleep", "9999"])
        .init(true)
        .spawn()
        .unwrap();

    // TODO: It seems that podman itself has some race condition that when attached
    // process is terminated very early during the initialization it may leave a
    // hanging container
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_matches!(
        container
            .wait_for_container(Duration::from_millis(1000))
            .await,
        Ok(_)
    );

    let container_name = container.container_name().to_string();

    // ContainerProcess::terminate() not called
    // Drop will perform blocking cleanup and will complain in logs
    drop(container);

    // Ensure container no longer exists
    assert_matches!(
        rt.wait_for_container(&container_name, Duration::from_millis(100))
            .await,
        Err(ContainerRuntimeError::Timeout(_))
    )
}

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_container_terminate_awaited() {
    let rt = ContainerRuntime::default();

    rt.ensure_image(TEST_IMAGE, None).await.unwrap();

    let mut container = rt
        .run_attached(TEST_IMAGE)
        .args(["sleep", "999"])
        .init(true)
        .spawn()
        .unwrap();

    // TODO: It seems that podman itself has some race condition that when attached
    // process is terminated very early during the initialization it may leave a
    // hanging container
    tokio::time::sleep(Duration::from_millis(100)).await;

    let status = container.terminate().await.unwrap();
    assert_matches!(status, TerminateStatus::Exited(_));
}
