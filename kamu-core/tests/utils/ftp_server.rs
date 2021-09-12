// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::infra::utils::docker_client::*;
use kamu::infra::utils::docker_images;

use std::path::{Path, PathBuf};
use std::time::Duration;

// TODO: Consider replacing with in-process server for speed
pub struct FtpServer {
    container_runtime: DockerClient,
    pub container_name: String,
    pub host_port: u16,
    process: std::process::Child,
}

impl FtpServer {
    pub fn new(server_dir: &Path) -> Self {
        use rand::Rng;

        let container_runtime = DockerClient::default();

        let mut server_name = "kamu-test-ftp-".to_owned();
        server_name.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(30)
                .map(char::from),
        );

        if !server_dir.exists() {
            std::fs::create_dir(&server_dir).unwrap();
        }

        assert!(
            container_runtime.has_image(docker_images::FTP),
            "Please pull {} image before running this test",
            docker_images::FTP
        );

        // TODO: this is likely very brittle because of all the port mapping
        // FTP is a crazy protocol :(
        let process = container_runtime
            .run_cmd(DockerRunArgs {
                image: docker_images::FTP.to_owned(),
                container_name: Some(server_name.to_owned()),
                expose_ports: vec![21],
                expose_port_map_range: vec![((47400, 47470), (47400, 47470))],
                volume_map: vec![(server_dir.to_owned(), PathBuf::from("/home/vsftpd"))],
                environment_vars: vec![
                    ("FTP_USER".to_owned(), "foo".to_owned()),
                    ("FTP_PASS".to_owned(), "bar".to_owned()),
                    ("PASV_ADDRESS".to_owned(), "localhost".to_owned()),
                ],
                ..DockerRunArgs::default()
            })
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();

        let host_port = container_runtime
            .wait_for_host_port(&server_name, 21, Duration::from_secs(20))
            .unwrap();

        container_runtime
            .wait_for_socket(host_port, Duration::from_secs(20))
            .unwrap();

        Self {
            container_runtime,
            container_name: server_name,
            process: process,
            host_port: host_port,
        }
    }
}

impl Drop for FtpServer {
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
