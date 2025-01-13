// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use random_names::get_random_name;
use tokio::process::{ChildStderr, ChildStdin, ChildStdout, Command};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ContainerRunCommand
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A builder for attached container processes
pub struct ContainerRunCommand {
    runtime: ContainerRuntime,
    args: RunArgs,
    stdin: Option<Stdio>,
    stdout: Option<Stdio>,
    stderr: Option<Stdio>,
    terminate_timeout: Duration,
}

impl ContainerRunCommand {
    const DEFAULT_TERMINATE_TIMEOUT: Duration = Duration::from_secs(3);

    pub(crate) fn new(runtime: ContainerRuntime, image: String) -> Self {
        Self {
            runtime,
            args: RunArgs {
                image,
                ..Default::default()
            },
            stdin: None,
            stdout: None,
            stderr: None,
            terminate_timeout: Self::DEFAULT_TERMINATE_TIMEOUT,
        }
    }

    pub fn container_name(mut self, v: impl Into<String>) -> Self {
        self.args.container_name = Some(v.into());
        self
    }

    pub fn random_container_network_with_prefix(mut self, prefix: &'static str) -> Self {
        let network_name = get_random_name(Some(prefix), 10);

        self.args.network = Some(network_name);
        self
    }

    pub fn random_container_name_with_prefix(mut self, prefix: &'static str) -> Self {
        let container_name = get_random_name(Some(prefix), 10);

        self.args.container_name = Some(container_name);
        self
    }

    pub fn args<I, S>(mut self, v: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for arg in v {
            self.args.args.push(arg.into());
        }
        self
    }

    pub fn arg(mut self, v: impl Into<String>) -> Self {
        self.args.args.push(v.into());
        self
    }

    pub fn entry_point(mut self, v: impl Into<String>) -> Self {
        self.args.entry_point = Some(v.into());
        self
    }

    pub fn environment_vars<I, S1, S2>(mut self, v: I) -> Self
    where
        I: IntoIterator<Item = (S1, S2)>,
        S1: Into<String>,
        S2: Into<String>,
    {
        for (name, value) in v {
            self.args.environment_vars.push((name.into(), value.into()));
        }
        self
    }

    pub fn environment_var(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.args.environment_vars.push((name.into(), value.into()));
        self
    }

    pub fn expose_all_ports(mut self, v: bool) -> Self {
        self.args.expose_all_ports = v;
        self
    }

    pub fn expose_ports(mut self, v: impl IntoIterator<Item = u16>) -> Self {
        for port in v {
            self.args.expose_ports.push(port);
        }
        self
    }

    pub fn expose_port(mut self, v: u16) -> Self {
        self.args.expose_ports.push(v);
        self
    }

    pub fn map_ports(mut self, v: impl IntoIterator<Item = (u16, u16)>) -> Self {
        for (host, container) in v {
            self.args.expose_port_map.push((host, container));
        }
        self
    }

    pub fn map_port(mut self, host: u16, container: u16) -> Self {
        if host != 0 {
            self.args.expose_port_map.push((host, container));
        } else {
            self.args.expose_ports.push(container);
        }
        self
    }

    pub fn map_ports_with_address<I, S>(mut self, v: I) -> Self
    where
        I: IntoIterator<Item = (S, u16, u16)>,
        S: Into<String>,
    {
        for (addr, host, container) in v {
            self.args
                .expose_port_map_addr
                .push((addr.into(), host, container));
        }
        self
    }

    pub fn map_port_with_address(
        mut self,
        addr: impl Into<String>,
        host: u16,
        container: u16,
    ) -> Self {
        self.args
            .expose_port_map_addr
            .push((addr.into(), host, container));
        self
    }

    pub fn map_port_ranges(
        mut self,
        v: impl IntoIterator<Item = ((u16, u16), (u16, u16))>,
    ) -> Self {
        for (host, container) in v {
            self.args.expose_port_map_range.push((host, container));
        }
        self
    }

    pub fn map_port_range(mut self, host: (u16, u16), container: (u16, u16)) -> Self {
        self.args.expose_port_map_range.push((host, container));
        self
    }

    pub fn hostname(mut self, v: impl Into<String>) -> Self {
        self.args.hostname = Some(v.into());
        self
    }

    pub fn init(mut self, v: bool) -> Self {
        self.args.init = v;
        self
    }

    pub fn interactive(mut self, v: bool) -> Self {
        self.args.interactive = v;
        self
    }

    pub fn remove(mut self, v: bool) -> Self {
        self.args.remove = v;
        self
    }

    pub fn tty(mut self, v: bool) -> Self {
        self.args.tty = v;
        self
    }

    pub fn network(mut self, v: impl Into<String>) -> Self {
        self.args.network = Some(v.into());
        self
    }

    pub fn user(mut self, v: impl Into<String>) -> Self {
        self.args.user = Some(v.into());
        self
    }

    /// Adds a range of volume mounts. For convenience, mounts can be specified
    /// as pairs `("/host/path", "/container/path")` or as triplets
    /// `("/host", "/container", VolumeAccess::ReadOnly)`
    pub fn volumes<I, V>(mut self, v: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<VolumeSpec>,
    {
        for spec in v {
            self.args.volumes.push(spec.into());
        }
        self
    }

    /// Add a volume mount. For convenience can be specified as a pair
    /// `("/host/path", "/container/path")` or as a triplet `("/host",
    /// "/container", VolumeAccess::ReadOnly)`
    pub fn volume(mut self, spec: impl Into<VolumeSpec>) -> Self {
        self.args.volumes.push(spec.into());
        self
    }

    pub fn work_dir(mut self, v: impl Into<PathBuf>) -> Self {
        self.args.work_dir = Some(v.into());
        self
    }

    pub fn extra_host(mut self, spec: impl Into<ExtraHostSpec>) -> Self {
        self.args.extra_hosts.push(spec.into());
        self
    }

    pub fn into_command(self) -> tokio::process::Command {
        let mut cmd = self.runtime.run_cmd(self.args);

        if let Some(stdin) = self.stdin {
            cmd.stdin(stdin);
        }
        if let Some(stdout) = self.stdout {
            cmd.stdout(stdout);
        }
        if let Some(stderr) = self.stderr {
            cmd.stderr(stderr);
        }
        cmd
    }

    pub fn shell_cmd(self, shell_cmd: impl Into<String>) -> Self {
        self.entry_point("sh").arg("-c").arg(shell_cmd)
    }

    pub fn stdin(mut self, v: impl Into<Stdio>) -> Self {
        self.stdin = Some(v.into());
        self
    }

    pub fn stdout(mut self, v: impl Into<Stdio>) -> Self {
        self.stdout = Some(v.into());
        self
    }

    pub fn stderr(mut self, v: impl Into<Stdio>) -> Self {
        self.stderr = Some(v.into());
        self
    }

    pub fn terminate_timeout(mut self, v: Duration) -> Self {
        self.terminate_timeout = v;
        self
    }

    pub fn spawn(mut self) -> std::io::Result<ContainerProcess> {
        assert!(
            !self.args.detached,
            "Cannot be used for detached containers"
        );

        if self.args.container_name.is_none() {
            self = self.random_container_name_with_prefix("");
        }

        let image = self.args.image.clone();
        let container_name = self.args.container_name.clone().unwrap();
        let runtime = self.runtime.clone();

        let terminate_timeout = self.terminate_timeout;
        let mut cmd = self.into_command();

        tracing::info!(?cmd, %container_name, %image, "Spawning container");

        let child = cmd.spawn()?;
        Ok(ContainerProcess::new(
            child,
            runtime,
            container_name,
            terminate_timeout,
        ))
    }

    pub async fn status(self) -> std::io::Result<ExitStatus> {
        let mut container = self.spawn()?;
        container.wait().await
    }

    pub fn maybe(self, cond: bool, fun: impl FnOnce(Self) -> Self) -> Self {
        if cond {
            fun(self)
        } else {
            self
        }
    }

    pub fn maybe_or(
        self,
        cond: bool,
        fun_if: impl FnOnce(Self) -> Self,
        fun_else: impl FnOnce(Self) -> Self,
    ) -> Self {
        if cond {
            fun_if(self)
        } else {
            fun_else(self)
        }
    }

    pub fn map<T>(self, opt: Option<T>, fun: impl FnOnce(Self, T) -> Self) -> Self {
        if let Some(val) = opt {
            fun(self, val)
        } else {
            self
        }
    }

    pub fn map_or<T>(
        self,
        opt: Option<T>,
        fun_if: impl FnOnce(Self, T) -> Self,
        fun_else: impl FnOnce(Self) -> Self,
    ) -> Self {
        if let Some(val) = opt {
            fun_if(self, val)
        } else {
            fun_else(self)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ContainerProcess
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ContainerProcess {
    child: tokio::process::Child,
    runtime: ContainerRuntime,
    container_name: String,
    terminate_timeout: Duration,
}

impl ContainerProcess {
    pub(crate) fn new(
        child: tokio::process::Child,
        runtime: ContainerRuntime,
        container_name: String,
        terminate_timeout: Duration,
    ) -> Self {
        Self {
            child,
            runtime,
            container_name,
            terminate_timeout,
        }
    }

    pub fn container_name(&self) -> &str {
        &self.container_name
    }

    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.stdin.take()
    }

    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout.take()
    }

    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.stderr.take()
    }

    pub fn exec_cmd<I, S>(&self, exec_args: ExecArgs, cmd_args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        self.runtime
            .exec_cmd(exec_args, &self.container_name, cmd_args)
    }

    pub fn exec_shell_cmd(&self, exec_args: ExecArgs, shell_cmd: impl AsRef<str>) -> Command {
        self.runtime
            .exec_shell_cmd(exec_args, &self.container_name, shell_cmd)
    }

    pub async fn wait_for_container(&self, timeout: Duration) -> Result<(), WaitForResourceError> {
        self.runtime
            .wait_for_container(&self.container_name, timeout)
            .await
    }

    pub async fn try_get_host_port(
        &self,
        container_port: u16,
    ) -> Result<Option<u16>, ContainerRuntimeError> {
        self.runtime
            .try_get_host_port(&self.container_name, container_port)
            .await
    }

    pub async fn wait_for_host_port(
        &self,
        container_port: u16,
        timeout: Duration,
    ) -> Result<u16, WaitForResourceError> {
        self.runtime
            .wait_for_host_port(&self.container_name, container_port, timeout)
            .await
    }

    /// Waits for specified container port to be mapped to host and start
    /// accepting TCP connections. Returns the corresponding port on the
    /// host network.
    pub async fn wait_for_host_socket(
        &self,
        container_port: u16,
        timeout: Duration,
    ) -> Result<u16, WaitForResourceError> {
        let start = std::time::Instant::now();

        let host_port = self.wait_for_host_port(container_port, timeout).await?;

        let elapsed = start.elapsed();
        if elapsed >= timeout {
            return Err(TimeoutError::new(timeout).into());
        }

        self.runtime
            .wait_for_socket(host_port, timeout - elapsed)
            .await?;

        Ok(host_port)
    }

    pub async fn wait(&mut self) -> std::io::Result<ExitStatus> {
        self.child.wait().await
    }

    #[tracing::instrument(level = "debug", name = "terminate_container", skip_all, fields(container_name = %self.container_name))]
    pub async fn terminate(&mut self) -> std::io::Result<TerminateStatus> {
        self.child.terminate(self.terminate_timeout).await
    }
}

impl Drop for ContainerProcess {
    fn drop(&mut self) {
        // id is present only when process have not yet been awaited for
        if self.child.id().is_some() {
            tracing::warn!(
                container_name = %self.container_name,
                "Container was not terminated - cleaning up synchronously"
            );

            let _ = self.child.terminate_blocking(self.terminate_timeout);
        }
    }
}
