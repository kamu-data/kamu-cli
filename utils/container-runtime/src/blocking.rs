// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::common::ContainerRuntimeCommon;
use super::errors::*;
use super::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct ContainerRuntime {
    pub config: ContainerRuntimeConfig,
}

// TODO: This is a hack primarily for tests that use containers not to
// spam docker/podman with many pull requests at a time. While docker's daemon
// architecture handles concurrent pulls well, podman seem to sometimes does not
// deduplicate pulls well.
static PULL_IMAGE_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[dill::component(pub)]
impl ContainerRuntime {
    pub fn new(config: ContainerRuntimeConfig) -> Self {
        Self { config }
    }

    fn new_command(&self) -> Command {
        ContainerRuntimeCommon::new_command(&self.config)
    }

    pub fn has_image(&self, image: &str) -> bool {
        ContainerRuntimeCommon::has_image_cmd(&self.config, image)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("Process failed")
            .success()
    }

    pub fn pull_cmd(&self, image: &str) -> Command {
        ContainerRuntimeCommon::pull_cmd(&self.config, image)
    }

    pub fn ensure_image(&self, image: &str, maybe_listener: Option<&dyn PullImageListener>) {
        let null_listener = NullPullImageListener;
        let listener = maybe_listener.unwrap_or(&null_listener);

        if !self.has_image(image) {
            listener.begin(image);

            {
                let _lock = PULL_IMAGE_MUTEX.lock().unwrap();

                // TODO: Detect reasons for a pull failure
                self.pull_cmd(image)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .expect("Failed to start pull process")
                    .exit_ok()
                    .expect("Failed to pull image");
            }

            listener.success();
        }
    }

    pub fn run_cmd(&self, args: RunArgs) -> Command {
        ContainerRuntimeCommon::run_cmd(&self.config, args)
    }

    pub fn run_shell_cmd<I, S>(&self, args: RunArgs, shell_cmd: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        ContainerRuntimeCommon::run_shell_cmd(&self.config, args, shell_cmd)
    }

    pub fn exec_cmd<I, S>(&self, exec_args: ExecArgs, container_name: &str, cmd_args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        ContainerRuntimeCommon::exec_cmd(&self.config, exec_args, container_name, cmd_args)
    }

    pub fn exec_shell_cmd<I, S>(
        &self,
        exec_args: ExecArgs,
        container_name: &str,
        shell_cmd: I,
    ) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        ContainerRuntimeCommon::exec_shell_cmd(&self.config, exec_args, container_name, shell_cmd)
    }

    pub fn kill_cmd(&self, container_name: &str) -> Command {
        ContainerRuntimeCommon::kill_cmd(&self.config, container_name)
    }

    pub fn create_network_cmd(&self, network_name: &str) -> Command {
        ContainerRuntimeCommon::create_network_cmd(&self.config, network_name)
    }

    pub fn remove_network_cmd(&self, network_name: &str) -> Command {
        ContainerRuntimeCommon::remove_network_cmd(&self.config, network_name)
    }

    pub fn create_network(&self, network_name: &str) -> NetworkHandle {
        let output = self
            .create_network_cmd(network_name)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .unwrap();

        if !output.status.success() {
            panic!(
                "Failed to create network: exit code: {} stdout: {} stderr: {}",
                output.status,
                std::str::from_utf8(&output.stdout).unwrap(),
                std::str::from_utf8(&output.stderr).unwrap(),
            )
        }

        let remove = self.remove_network_cmd(network_name);
        NetworkHandle::new(remove)
    }

    pub fn get_host_port(&self, container_name: &str, container_port: u16) -> Option<u16> {
        if self.config.network_ns == NetworkNamespaceType::Host {
            return Some(container_port);
        }

        let res = ContainerRuntimeCommon::inspect_host_port_cmd(
            &self.config,
            container_name,
            container_port,
        )
        .output();

        match res {
            Ok(output) => std::str::from_utf8(&output.stdout)
                .unwrap()
                .trim_matches(&['\r', '\n'][..])
                .parse()
                .ok(),
            _ => None,
        }
    }

    pub fn wait_for_container(
        &self,
        container_name: &str,
        timeout: Duration,
    ) -> Result<(), TimeoutError> {
        let start = Instant::now();

        loop {
            let res = self
                .new_command()
                .arg("inspect")
                .arg(container_name)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();

            if res.is_ok() && res.unwrap().success() {
                break Ok(());
            } else if start.elapsed() >= timeout {
                break Err(TimeoutError::new(timeout));
            } else {
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }

    pub fn wait_for_host_port(
        &self,
        container_name: &str,
        container_port: u16,
        timeout: Duration,
    ) -> Result<u16, TimeoutError> {
        let start = Instant::now();
        loop {
            let res = self.get_host_port(container_name, container_port);
            if let Some(hp) = res {
                break Ok(hp);
            } else if start.elapsed() >= timeout {
                break Err(TimeoutError::new(timeout));
            } else {
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }

    pub fn get_runtime_host_addr(&self) -> String {
        ContainerRuntimeCommon::get_runtime_host_addr(&self.config)
    }

    pub fn check_socket(&self, host_port: u16) -> bool {
        ContainerRuntimeCommon::check_socket(&self.config, host_port).unwrap_or(false)
    }

    pub fn wait_for_socket(&self, host_port: u16, timeout: Duration) -> Result<(), TimeoutError> {
        let start = Instant::now();
        loop {
            if self.check_socket(host_port) {
                break Ok(());
            } else if start.elapsed() >= timeout {
                break Err(TimeoutError::new(timeout));
            } else {
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// NetworkHandle
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct NetworkHandle {
    remove: Command,
}

impl NetworkHandle {
    fn new(remove: Command) -> Self {
        Self { remove }
    }
}

impl Drop for NetworkHandle {
    fn drop(&mut self) {
        self.remove
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
    }
}

///////////////////////////////////////////////////////////////////////////////
// ContainerHandle
///////////////////////////////////////////////////////////////////////////////

pub struct ContainerHandle {
    runtime: Arc<ContainerRuntime>,
    name: String,
}

impl ContainerHandle {
    pub fn new<S: Into<String>>(runtime: Arc<ContainerRuntime>, name: S) -> Self {
        Self {
            runtime,
            name: name.into(),
        }
    }
}

impl Drop for ContainerHandle {
    fn drop(&mut self) {
        let _ = self
            .runtime
            .kill_cmd(&self.name)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}
