// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::infra::utils::docker_images;
use container_runtime::{ContainerRuntime, RunArgs};

use std::path::{Path, PathBuf};
use std::time::Duration;

pub struct MinioServer {
    container_runtime: ContainerRuntime,
    pub container_name: String,
    pub address: String,
    pub host_port: u16,
    pub access_key: String,
    pub secret_key: String,
    process: std::process::Child,
}

impl MinioServer {
    pub const IMAGE: &str = docker_images::MINIO;

    pub fn new(server_dir: &Path, access_key: &str, secret_key: &str) -> Self {
        use rand::Rng;

        let container_runtime = ContainerRuntime::default();
        container_runtime.ensure_image(Self::IMAGE, None);

        let mut server_name = "kamu-test-minio-".to_owned();
        server_name.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(30)
                .map(char::from),
        );
        let server_port = 9000;

        if !server_dir.exists() {
            std::fs::create_dir(&server_dir).unwrap();
        }

        let process = container_runtime
            .run_cmd(RunArgs {
                image: Self::IMAGE.to_owned(),
                container_name: Some(server_name.to_owned()),
                expose_ports: vec![server_port],
                volume_map: vec![(server_dir.to_owned(), PathBuf::from("/data"))],
                environment_vars: vec![
                    ("MINIO_ACCESS_KEY".to_owned(), access_key.to_owned()),
                    ("MINIO_SECRET_KEY".to_owned(), secret_key.to_owned()),
                ],
                args: vec!["server".to_owned(), "/data".to_owned()],
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
            process: process,
            address: address,
            host_port: host_port,
            access_key: access_key.to_owned(),
            secret_key: secret_key.to_owned(),
        }
    }
}

impl Drop for MinioServer {
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
