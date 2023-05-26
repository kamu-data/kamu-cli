// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use container_runtime::ContainerRuntime;

use super::test_container::TEST_IMAGE;

// Not really a test - used by CI to separate pulling of test images
// into its own phase
#[cfg_attr(feature = "skip_docker_tests", ignore)]
#[test_log::test(tokio::test)]
async fn test_setup_pull_images() {
    let container_runtime = ContainerRuntime::default();

    container_runtime
        .ensure_image(TEST_IMAGE, None)
        .await
        .unwrap();
}
