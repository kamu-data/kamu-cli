// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::time::Duration;

use container_runtime::*;
use kamu::utils::docker_images;

// TODO: Consider replacing with in-process server for speed
pub struct FtpServer {
    pub container_name: String,
    pub host_port: u16,
    #[allow(dead_code)]
    container: ContainerProcess,
}

impl FtpServer {
    pub const IMAGE: &'static str = docker_images::FTP;

    pub async fn new(server_dir: &Path) -> Self {
        let container_runtime = ContainerRuntime::default();

        container_runtime
            .ensure_image(Self::IMAGE, None)
            .await
            .unwrap();

        if !server_dir.exists() {
            std::fs::create_dir(&server_dir).unwrap();
        }

        // TODO: this is likely very brittle because of all the port mapping
        // FTP is a crazy protocol :(
        let container = container_runtime
            .run_attached(Self::IMAGE)
            .container_name_prefix("kamu-test-ftp-")
            .expose_port(21)
            .map_port_range((47400, 47470), (47400, 47470))
            .volume((server_dir, "/home/vsftpd"))
            .environment_vars([
                ("FTP_USER", "foo"),
                ("FTP_PASS", "bar"),
                ("PASV_ADDRESS", "localhost"),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();

        let host_port = container
            .wait_for_host_socket(21, Duration::from_secs(20))
            .await
            .unwrap();

        Self {
            container_name: container.container_name().to_string(),
            container,
            host_port,
        }
    }
}
