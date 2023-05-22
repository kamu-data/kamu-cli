// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::process::Stdio;

use kamu::infra::utils::ipfs_wrapper::IpfsClient;
use tokio::process::{Child, Command};

pub struct IpfsDaemon {
    temp_dir: tempfile::TempDir,
    #[allow(dead_code)]
    process: Child,
}

impl IpfsDaemon {
    pub async fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();

        Command::new("ipfs")
            .env("IPFS_PATH", temp_dir.path())
            .args(["init", "--profile", "test"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .unwrap()
            .exit_ok()
            .unwrap();

        // TODO: assign random free port
        Command::new("ipfs")
            .env("IPFS_PATH", temp_dir.path())
            .args(["config", "Addresses.Gateway", "/ip4/127.0.0.1/tcp/33101"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .unwrap()
            .exit_ok()
            .unwrap();

        let process = Command::new("ipfs")
            .env("IPFS_PATH", temp_dir.path())
            .args(["daemon", "--offline"])
            .kill_on_drop(true)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let this = Self { temp_dir, process };

        // Yuck: re-using wait-for-socket functionality
        let container_runtime = container_runtime::ContainerRuntime::default();
        container_runtime
            .wait_for_socket(this.http_port(), std::time::Duration::from_secs(15))
            .await
            .unwrap();

        this
    }

    pub fn ipfs_path(&self) -> &std::path::Path {
        self.temp_dir.path()
    }

    // TODO: assign random free port
    pub fn http_port(&self) -> u16 {
        33101
    }

    pub fn client(&self) -> IpfsClient {
        IpfsClient::new(Some(self.ipfs_path()), true)
    }
}
