// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::time::Duration;

use container_runtime::{ContainerRuntime, RunArgs};
use kamu::infra::utils::docker_images;

// TODO: Implement caching headers in `HttpFileServer` so we could get rid of
// this class
pub struct HttpServer {
    container_runtime: ContainerRuntime,
    pub container_name: String,
    pub address: String,
    pub host_port: u16,
    process: std::process::Child,
}

impl HttpServer {
    pub const IMAGE: &str = docker_images::HTTPD;

    pub fn new(server_dir: &Path) -> Self {
        use rand::Rng;

        let container_runtime = ContainerRuntime::default();
        container_runtime.ensure_image(Self::IMAGE, None);

        let mut server_name = "kamu-test-http-".to_owned();
        server_name.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(30)
                .map(char::from),
        );
        let server_port = 80;

        if !server_dir.exists() {
            std::fs::create_dir(&server_dir).unwrap();
        }

        let process = container_runtime
            .run_cmd(RunArgs {
                image: Self::IMAGE.to_owned(),
                container_name: Some(server_name.to_owned()),
                expose_ports: vec![server_port],
                volume_map: vec![(
                    server_dir.to_owned(),
                    PathBuf::from("/usr/local/apache2/htdocs"),
                )],
                ..RunArgs::default()
            })
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();

        let host_port = container_runtime
            .wait_for_host_port(&server_name, server_port, Duration::from_secs(20))
            .unwrap();

        container_runtime
            .wait_for_socket(host_port, Duration::from_secs(20))
            .unwrap();

        let address = container_runtime.get_runtime_host_addr();

        Self {
            container_runtime,
            container_name: server_name,
            process,
            address,
            host_port,
        }
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        let _ = self
            .container_runtime
            .kill_cmd(&self.container_name)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let _ = self.process.wait();
    }
}
