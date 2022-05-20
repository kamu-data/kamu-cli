// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::infra::utils::ipfs_wrapper::IpfsClient;

pub struct IpfsDaemon {
    temp_dir: tempfile::TempDir,
    process: std::process::Child,
}

impl IpfsDaemon {
    pub fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();

        std::process::Command::new("ipfs")
            .env("IPFS_PATH", temp_dir.path())
            .args(["init", "--profile", "test"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .exit_ok()
            .unwrap();

        // TODO: assign random free port
        std::process::Command::new("ipfs")
            .env("IPFS_PATH", temp_dir.path())
            .args(["config", "Addresses.Gateway", "/ip4/127.0.0.1/tcp/33101"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .exit_ok()
            .unwrap();

        let process = std::process::Command::new("ipfs")
            .env("IPFS_PATH", temp_dir.path())
            .args(["daemon", "--offline"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();

        let this = Self { temp_dir, process };

        // Yuck: re-using wait-for-socket functionality
        let container_runtime = container_runtime::ContainerRuntime::default();
        container_runtime
            .wait_for_socket(this.http_port(), std::time::Duration::from_secs(15))
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

impl Drop for IpfsDaemon {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}
