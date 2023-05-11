// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::infra::*;

use container_runtime::{ContainerHandle, ContainerRuntime, PullImageListener, RunArgs};
use std::fs::File;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

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

    pub fn ensure_images(&self, listener: &mut dyn PullImageListener) {
        self.container_runtime
            .ensure_image(&self.image, Some(listener));
    }

    pub fn run<StartedClb>(
        &self,
        addr: &str,
        host_port: u16,
        workspace_layout: &WorkspaceLayout,
        inherit_stdio: bool,
        on_started: StartedClb,
    ) -> Result<(), std::io::Error>
    where
        StartedClb: FnOnce() + Send + 'static,
    {
        const LIVY_PORT: u16 = 8998;

        let livy_stdout_path = workspace_layout.run_info_dir.join("livy.out.txt");
        let livy_stderr_path = workspace_layout.run_info_dir.join("livy.err.txt");

        let mut livy_cmd = self.container_runtime.run_cmd(RunArgs {
            image: self.image.clone(),
            container_name: Some("kamu-livy".to_owned()),
            entry_point: Some("/opt/livy/bin/livy-server".to_owned()),
            user: Some("root".to_owned()),
            expose_port_map_addr: vec![(addr.to_owned(), host_port, LIVY_PORT)],
            work_dir: Some(PathBuf::from("/opt/bitnami/spark/work-dir")),
            volume_map: vec![(
                workspace_layout.datasets_dir.clone(),
                PathBuf::from("/opt/bitnami/spark/work-dir"),
            )],
            ..RunArgs::default()
        });

        tracing::info!(command = ?livy_cmd, "Starting Livy container");

        let mut livy = livy_cmd
            .stdout(if inherit_stdio {
                Stdio::inherit()
            } else {
                Stdio::from(File::create(&livy_stdout_path)?)
            })
            .stderr(if inherit_stdio {
                Stdio::inherit()
            } else {
                Stdio::from(File::create(&livy_stderr_path)?)
            })
            .spawn()?;

        let _drop_livy = ContainerHandle::new(self.container_runtime.clone(), "kamu-livy");

        self.container_runtime
            .wait_for_socket(host_port, Duration::from_secs(60))
            .expect("Livy did not start");

        on_started();

        let exit = Arc::new(AtomicBool::new(false));
        signal_hook::flag::register(libc::SIGINT, exit.clone())?;
        signal_hook::flag::register(libc::SIGTERM, exit.clone())?;

        while !exit.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        cfg_if::cfg_if! {
            if #[cfg(unix)] {
                unsafe {
                    libc::kill(livy.id() as libc::pid_t, libc::SIGTERM);
                }
            }
        }

        livy.wait()?;
        Ok(())
    }
}
