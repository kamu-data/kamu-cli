// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use container_runtime::ContainerRuntime;
use kamu::utils::docker_images;
use test_utils::test_docker_images;

// Not really a test - used by CI to separate pulling of test images
// into its own phase
#[test_group::group(setup, containerized)]
#[test_log::test(tokio::test)]
async fn test_setup_pull_images() {
    let container_runtime = ContainerRuntime::default();

    // Disabled for disk space issue
    /*container_runtime
        .ensure_image(docker_images::SPARK, None)
        .await
        .unwrap();
    container_runtime
        .ensure_image(docker_images::FLINK, None)
        .await
        .unwrap();*/
    // Disabled for disk space issue
    // See: https://github.com/kamu-data/kamu-cli/issues/599
    // container_runtime
    //     .ensure_image(docker_images::RISINGWAVE, None)
    //     .await
    //     .unwrap();
    container_runtime
        .ensure_image(docker_images::DATAFUSION, None)
        .await
        .unwrap();
    container_runtime
        .ensure_image(test_docker_images::HTTPD, None)
        .await
        .unwrap();
    container_runtime
        .ensure_image(test_docker_images::MINIO, None)
        .await
        .unwrap();

    cfg_if::cfg_if! {
        if #[cfg(feature = "ingest-mqtt")] {
            container_runtime
                .ensure_image(docker_images::RUMQTTD, None)
                .await
                .unwrap();
        }
    }

    cfg_if::cfg_if! {
        if #[cfg(feature = "ingest-ftp")] {
            container_runtime.ensure_image(docker_images::FTP, None).await.unwrap();
        }
    }
}
