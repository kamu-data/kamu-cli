// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::File;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use container_runtime::*;
use kamu::domain::error::*;
use kamu::infra::*;

pub struct LivyServerImpl {
    container_runtime: Arc<ContainerRuntime>,
    image: String,
}

impl LivyServerImpl {
    pub fn new(container_runtime: Arc<ContainerRuntime>, image: String) -> Self {
        Self {
            container_runtime,
            image,
        }
    }

    pub async fn ensure_images(
        &self,
        listener: &mut dyn PullImageListener,
    ) -> Result<(), ImagePullError> {
        self.container_runtime
            .ensure_image(&self.image, Some(listener))
            .await
    }

    pub async fn run<StartedClb>(
        &self,
        addr: &str,
        host_port: u16,
        workspace_layout: &WorkspaceLayout,
        inherit_stdio: bool,
        on_started: StartedClb,
    ) -> Result<(), InternalError>
    where
        StartedClb: FnOnce() + Send + 'static,
    {
        const LIVY_PORT: u16 = 8998;

        let livy_stdout_path = workspace_layout.run_info_dir.join("livy.out.txt");
        let livy_stderr_path = workspace_layout.run_info_dir.join("livy.err.txt");

        let mut livy = self
            .container_runtime
            .run_attached(&self.image)
            .container_name("kamu-livy")
            .entry_point("/opt/livy/bin/livy-server")
            .user("root")
            .map_port_with_address(addr, host_port, LIVY_PORT)
            .work_dir("/opt/bitnami/spark/work-dir")
            .volume(
                &workspace_layout.datasets_dir,
                "/opt/bitnami/spark/work-dir",
            )
            .stdout(if inherit_stdio {
                Stdio::inherit()
            } else {
                Stdio::from(File::create(&livy_stdout_path).int_err()?)
            })
            .stderr(if inherit_stdio {
                Stdio::inherit()
            } else {
                Stdio::from(File::create(&livy_stderr_path).int_err()?)
            })
            .spawn()
            .int_err()?;

        livy.wait_for_host_socket(LIVY_PORT, Duration::from_secs(60))
            .await
            .int_err()?;

        on_started();

        let exit = Arc::new(AtomicBool::new(false));
        signal_hook::flag::register(libc::SIGINT, exit.clone()).int_err()?;
        signal_hook::flag::register(libc::SIGTERM, exit.clone()).int_err()?;

        while !exit.load(Ordering::Relaxed) {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        livy.terminate().await.int_err()?;
        Ok(())
    }
}
