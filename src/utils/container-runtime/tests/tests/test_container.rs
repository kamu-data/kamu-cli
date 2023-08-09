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

use container_runtime::*;

pub const TEST_IMAGE: &str = "docker.io/busybox:latest";

// TODO: Remove this once we finished debugging the flaky test problem
fn dump_state(hint: &str) {
    let podman = std::process::Command::new("podman")
        .args(["ps", "-a"])
        .output()
        .unwrap();
    let ps = std::process::Command::new("sh")
        .args(["-c", "ps -ef | grep podman"])
        .output()
        .unwrap();

    tracing::warn!("{}", hint);
    tracing::warn!("{}", std::str::from_utf8(&podman.stdout).unwrap());
    tracing::warn!("{}", std::str::from_utf8(&ps.stdout).unwrap());
}

#[test_group::group(containerized, flaky)]
#[test_log::test(tokio::test)]
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

    assert_matches!(
        container
            .wait_for_container(Duration::from_millis(5000))
            .await,
        Ok(_)
    );

    dump_state("<<<<<<<<< State pre-drop:");

    let container_name = container.container_name().to_string();

    // ContainerProcess::terminate() not called
    // Drop will perform blocking cleanup and will complain in logs
    drop(container);

    dump_state(">>>>>>>>> State post-drop:");

    // Ensure container no longer exists
    assert_matches!(
        rt.wait_for_container(&container_name, Duration::from_millis(100))
            .await,
        Err(WaitForResourceError::Timeout(_))
    );
}

#[test_group::group(containerized, flaky)]
#[test_log::test(tokio::test)]
async fn test_container_terminate_awaited() {
    let rt = ContainerRuntime::default();

    rt.ensure_image(TEST_IMAGE, None).await.unwrap();

    let mut container = rt
        .run_attached(TEST_IMAGE)
        .args(["sleep", "4999"])
        .init(true)
        .spawn()
        .unwrap();

    assert_matches!(
        container
            .wait_for_container(Duration::from_millis(5000))
            .await,
        Ok(_)
    );

    dump_state("<<<<<<<<< State pre-terminate:");

    let status = container.terminate().await.unwrap();

    dump_state(">>>>>>>>> State post-terminate:");

    assert_matches!(status, TerminateStatus::Exited(_));
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_volume_mount_read_write() {
    let tempdir = tempfile::tempdir().unwrap();

    let rt = ContainerRuntime::default();
    rt.ensure_image(TEST_IMAGE, None).await.unwrap();

    let mut container = rt
        .run_attached(TEST_IMAGE)
        .volume((tempdir.path(), "/out"))
        .args(["touch", "/out/test"])
        .init(true)
        .spawn()
        .unwrap();

    container.wait().await.unwrap().exit_ok().unwrap();

    assert!(tempdir.path().join("test").exists());
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_volume_mount_read_only() {
    let tempdir = tempfile::tempdir().unwrap();

    let rt = ContainerRuntime::default();
    rt.ensure_image(TEST_IMAGE, None).await.unwrap();

    let mut container = rt
        .run_attached(TEST_IMAGE)
        .volume((tempdir.path(), "/out", VolumeAccess::ReadOnly))
        .args(["touch", "/out/test"])
        .init(true)
        .spawn()
        .unwrap();

    let res = container.wait().await.unwrap();

    assert_ne!(res.code().unwrap(), 0);
}
