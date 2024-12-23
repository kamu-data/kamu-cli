// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use container_runtime::*;
use test_utils::test_docker_images;

// TODO: Implement caching headers in `HttpFileServer` so we could get rid of
// this class
pub struct HttpServer {
    pub container_name: String,
    pub address: String,
    pub host_port: u16,
    #[allow(dead_code)]
    container: ContainerProcess,
}

impl HttpServer {
    pub const IMAGE: &'static str = test_docker_images::HTTPD;

    pub async fn new(server_dir: &Path) -> Self {
        let container_runtime = ContainerRuntime::default();
        container_runtime
            .ensure_image(Self::IMAGE, None)
            .await
            .unwrap();

        let server_port = 80;

        if !server_dir.exists() {
            std::fs::create_dir(server_dir).unwrap();
        }

        let container = container_runtime
            .run_attached(Self::IMAGE)
            .random_container_name_with_prefix("kamu-test-http-")
            .expose_port(server_port)
            .volume((server_dir, "/usr/local/apache2/htdocs"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let host_port = container
            .wait_for_host_socket(server_port, Duration::from_secs(20))
            .await
            .unwrap();

        let address = container_runtime.get_runtime_host_addr();

        Self {
            container_name: container.container_name().to_string(),
            container,
            address,
            host_port,
        }
    }
}
