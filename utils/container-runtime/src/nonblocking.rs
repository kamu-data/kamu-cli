// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::process::Command;

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
static PULL_IMAGE_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[dill::component(pub)]
impl ContainerRuntime {
    pub fn new(config: ContainerRuntimeConfig) -> Self {
        Self { config }
    }

    pub fn into_blocking(self) -> super::blocking::ContainerRuntime {
        super::blocking::ContainerRuntime::new(self.config)
    }

    fn new_command(&self) -> Command {
        ContainerRuntimeCommon::new_command(&self.config).into()
    }

    pub async fn has_image(&self, image: &str) -> Result<bool, ContainerRuntimeError> {
        let mut cmd: Command = ContainerRuntimeCommon::has_image_cmd(&self.config, image).into();

        let success = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await?
            .success();

        Ok(success)
    }

    pub fn pull_cmd(&self, image: &str) -> Command {
        ContainerRuntimeCommon::pull_cmd(&self.config, image).into()
    }

    pub async fn ensure_image(
        &self,
        image: &str,
        maybe_listener: Option<&dyn PullImageListener>,
    ) -> Result<(), ImagePullError> {
        if !self
            .has_image(image)
            .await
            .map_err(|err| ImagePullError::runtime(image, err.into()))?
        {
            self.pull_image(image, maybe_listener).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all, fields(image))]
    pub async fn pull_image(
        &self,
        image: &str,
        maybe_listener: Option<&dyn PullImageListener>,
    ) -> Result<(), ImagePullError> {
        let null_listener = NullPullImageListener;
        let listener = maybe_listener.unwrap_or(&null_listener);

        listener.begin(image);
        {
            let _lock = PULL_IMAGE_MUTEX.lock().await;

            // TODO: Detect reasons for a pull failure
            let mut cmd = self.pull_cmd(image);
            let output = cmd
                .output()
                .await
                .map_err(|err| ImagePullError::runtime(image, err.into()))?;

            if !output.status.success() {
                // Replicating command construction as Command does not implement Clone
                let command = ContainerRuntimeCommon::pull_cmd(&self.config, image);
                return Err(ImagePullError::runtime(
                    image,
                    ProcessError::from_output(command, output).into(),
                ));
            }
        }

        listener.success();
        Ok(())
    }

    pub fn run_cmd(&self, args: RunArgs) -> Command {
        ContainerRuntimeCommon::run_cmd(&self.config, args).into()
    }

    pub fn run_shell_cmd<I, S>(&self, args: RunArgs, shell_cmd: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        ContainerRuntimeCommon::run_shell_cmd(&self.config, args, shell_cmd).into()
    }

    pub fn exec_cmd<I, S>(&self, exec_args: ExecArgs, container_name: &str, cmd_args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        ContainerRuntimeCommon::exec_cmd(&self.config, exec_args, container_name, cmd_args).into()
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
            .into()
    }

    pub fn kill_cmd(&self, container_name: &str) -> Command {
        ContainerRuntimeCommon::kill_cmd(&self.config, container_name).into()
    }

    pub fn create_network_cmd(&self, network_name: &str) -> Command {
        ContainerRuntimeCommon::create_network_cmd(&self.config, network_name).into()
    }

    pub fn remove_network_cmd(&self, network_name: &str) -> Command {
        ContainerRuntimeCommon::remove_network_cmd(&self.config, network_name).into()
    }

    pub async fn create_network(
        &self,
        network_name: &str,
    ) -> Result<NetworkHandle, ContainerRuntimeError> {
        let output = self.create_network_cmd(network_name).output().await?;

        if !output.status.success() {
            // Replicating command construction as Command does not implement Clone
            let command = ContainerRuntimeCommon::create_network_cmd(&self.config, network_name);
            return Err(ProcessError::from_output(command, output).into());
        }

        let remove = self.remove_network_cmd(network_name);
        Ok(NetworkHandle::new(remove))
    }

    pub async fn try_get_host_port(
        &self,
        container_name: &str,
        container_port: u16,
    ) -> Result<Option<u16>, ContainerRuntimeError> {
        if self.config.network_ns == NetworkNamespaceType::Host {
            return Ok(Some(container_port));
        }

        let mut cmd: Command = ContainerRuntimeCommon::inspect_host_port_cmd(
            &self.config,
            container_name,
            container_port,
        )
        .into();

        let output = cmd.output().await?;

        if output.status.success() {
            let port = std::str::from_utf8(&output.stdout)
                .unwrap()
                .trim_matches(&['\r', '\n'][..])
                .parse()
                .ok();
            Ok(port)
        } else {
            // TODO: Differentiate errors
            Ok(None)
        }
    }

    pub async fn wait_for_container(
        &self,
        container_name: &str,
        timeout: Duration,
    ) -> Result<(), ContainerRuntimeError> {
        let start = Instant::now();

        loop {
            let res = self
                .new_command()
                .arg("inspect")
                .arg(container_name)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await?;

            if res.success() {
                break Ok(());
            } else if start.elapsed() >= timeout {
                break Err(TimeoutError::new(timeout).into());
            } else {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    pub async fn wait_for_host_port(
        &self,
        container_name: &str,
        container_port: u16,
        timeout: Duration,
    ) -> Result<u16, ContainerRuntimeError> {
        let start = Instant::now();
        loop {
            let res = self
                .try_get_host_port(container_name, container_port)
                .await?;
            if let Some(hp) = res {
                break Ok(hp);
            } else if start.elapsed() >= timeout {
                break Err(TimeoutError::new(timeout).into());
            } else {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    pub fn get_runtime_host_addr(&self) -> String {
        ContainerRuntimeCommon::get_runtime_host_addr(&self.config)
    }

    pub async fn check_socket(&self, host_port: u16) -> Result<bool, ContainerRuntimeError> {
        // TODO: Do we need async implementation for this quick check?
        ContainerRuntimeCommon::check_socket(&self.config, host_port)
    }

    pub async fn wait_for_socket(
        &self,
        host_port: u16,
        timeout: Duration,
    ) -> Result<(), ContainerRuntimeError> {
        let start = Instant::now();
        loop {
            if self.check_socket(host_port).await? {
                break Ok(());
            } else if start.elapsed() >= timeout {
                break Err(TimeoutError::new(timeout).into());
            } else {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// NetworkHandle
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct NetworkHandle {
    remove: Option<Command>,
}

impl NetworkHandle {
    fn new(remove: Command) -> Self {
        Self {
            remove: Some(remove),
        }
    }

    pub async fn free(mut self) -> Result<(), ContainerRuntimeError> {
        if let Some(mut cmd) = self.remove.take() {
            cmd.stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await?;
        }
        Ok(())
    }
}

impl Drop for NetworkHandle {
    fn drop(&mut self) {
        if let Some(mut cmd) = self.remove.take() {
            tokio::spawn(async move {
                let _ = cmd
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .await;
            });
        }
    }
}
