// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use random_strings::get_random_name;
use tokio::process::Command;

use super::errors::*;
use super::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    const TICK_INTERVAL: Duration = Duration::from_millis(50);

    pub fn new(config: ContainerRuntimeConfig) -> Self {
        Self { config }
    }

    pub(crate) fn new_command(&self) -> Command {
        Command::new(match self.config.runtime {
            ContainerRuntimeType::Docker => "docker",
            ContainerRuntimeType::Podman => "podman",
        })
    }

    pub(crate) fn new_command_std(&self) -> std::process::Command {
        std::process::Command::new(match self.config.runtime {
            ContainerRuntimeType::Docker => "docker",
            ContainerRuntimeType::Podman => "podman",
        })
    }

    pub async fn has_image(&self, image: &str) -> Result<bool, ContainerRuntimeError> {
        let status = self
            .new_command()
            .arg("image")
            .arg("inspect")
            .arg(image)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await?;

        Ok(status.success())
    }

    pub fn pull_cmd(&self, image: &str) -> Command {
        let mut cmd = self.new_command();
        cmd.arg("pull").arg(image);
        cmd
    }

    pub async fn ensure_image(
        &self,
        image: &str,
        maybe_listener: Option<&dyn PullImageListener>,
    ) -> Result<(), ImagePullError> {
        if !self
            .has_image(image)
            .await
            .map_err(|err| ImagePullError::runtime(image, err))?
        {
            tracing::warn!(image, runtime=?self.config.runtime, "No image locally");
            self.pull_image(image, maybe_listener).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all, fields(%image))]
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
                return Err(ImagePullError::runtime(
                    image,
                    ProcessError::from_output(cmd, output).into(),
                ));
            }
        }

        listener.success();
        Ok(())
    }

    pub fn info(&self) -> Command {
        let mut cmd = self.new_command();
        cmd.arg("info");
        cmd.arg("-f");
        cmd.arg("json");
        cmd
    }

    pub fn run_cmd(&self, args: RunArgs) -> Command {
        let mut cmd = self.new_command();
        cmd.arg("run");
        if args.remove {
            cmd.arg("--rm");
        }
        if args.tty {
            cmd.arg("-t");
        }
        if args.init {
            cmd.arg("--init");
        }
        if args.interactive {
            cmd.arg("-i");
        }
        if args.detached {
            cmd.arg("-d");
        }
        args.container_name.map(|v| cmd.arg(format!("--name={v}")));
        args.hostname.map(|v| cmd.arg(format!("--hostname={v}")));
        args.network.map(|v| cmd.arg(format!("--network={v}")));
        if args.expose_all_ports {
            cmd.arg("-P");
        }
        args.expose_ports.iter().for_each(|v| {
            cmd.arg("-p");
            cmd.arg(format!("{v}"));
        });
        args.expose_port_map.iter().for_each(|(h, c)| {
            cmd.arg("-p");
            cmd.arg(format!("{h}:{c}"));
        });
        args.expose_port_map_addr.iter().for_each(|(addr, h, c)| {
            cmd.arg("-p");
            cmd.arg(format!("{addr}:{h}:{c}"));
        });
        args.expose_port_map_range
            .iter()
            .for_each(|((hl, hr), (cl, cr))| {
                cmd.arg("-p");
                cmd.arg(format!("{hl}-{hr}:{cl}-{cr}"));
            });
        args.volumes.into_iter().for_each(|v| {
            cmd.arg("-v");

            let mut volume = format!(
                "{}:{}",
                Self::format_host_path(v.source),
                Self::format_container_path(&v.dest),
            );

            let is_readonly = matches!(v.access, VolumeAccess::ReadOnly);
            let has_selinux = is_selinux_present();

            match (is_readonly, has_selinux) {
                (true, true) => volume += ":ro,Z",
                (false, true) => volume += ":Z",
                (true, false) => volume += ":ro",
                (false, false) => { /* no labels needed */ }
            }

            cmd.arg(volume);
        });
        args.extra_hosts.into_iter().for_each(|h| {
            cmd.arg("--add-host");
            cmd.arg(format!("{}:{}", h.source, h.dest));
        });
        args.user.map(|v| cmd.arg(format!("--user={v}")));
        args.work_dir
            .map(|v| cmd.arg(format!("--workdir={}", v.display())));
        args.environment_vars.iter().for_each(|(n, v)| {
            cmd.arg("-e");
            cmd.arg(format!("{n}={v}"));
        });
        args.entry_point
            .map(|v| cmd.arg(format!("--entrypoint={v}")));
        cmd.arg(args.image);
        cmd.args(args.args);
        cmd
    }

    /// Use to run a container as a child process
    pub fn run_attached(&self, image: impl Into<String>) -> ContainerRunCommand {
        ContainerRunCommand::new(self.clone(), image.into())
    }

    pub fn exec_cmd<I, S>(&self, exec_args: ExecArgs, container_name: &str, cmd_args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let mut cmd = self.new_command();
        cmd.arg("exec");
        if exec_args.tty {
            cmd.arg("-t");
        }
        if exec_args.interactive {
            cmd.arg("-i");
        }
        exec_args
            .work_dir
            .map(|v| cmd.arg(format!("--workdir={}", v.display())));
        cmd.arg(container_name);
        cmd.args(cmd_args);
        cmd
    }

    pub fn exec_shell_cmd(
        &self,
        exec_args: ExecArgs,
        container_name: &str,
        shell_cmd: impl AsRef<str>,
    ) -> Command {
        self.exec_cmd(exec_args, container_name, ["sh", "-c", shell_cmd.as_ref()])
    }

    pub fn kill_cmd(&self, container_name: &str, signal: Signal) -> Command {
        self.kill_cmd_std(container_name, signal).into()
    }

    pub fn kill_cmd_std(&self, container_name: &str, signal: Signal) -> std::process::Command {
        let mut cmd = self.new_command_std();
        cmd.arg("kill")
            .arg("--signal")
            .arg(match signal {
                Signal::TERM => "TERM",
                Signal::KILL => "KILL",
            })
            .arg(container_name);
        cmd
    }

    pub fn create_network_cmd(&self, network_name: &str) -> Command {
        let mut cmd = self.new_command();
        cmd.arg("network").arg("create").arg(network_name);
        cmd
    }

    pub fn remove_network_cmd(&self, network_name: &str) -> Command {
        self.remove_network_cmd_std(network_name).into()
    }

    pub(crate) fn remove_network_cmd_std(&self, network_name: &str) -> std::process::Command {
        let mut cmd = self.new_command_std();
        cmd.arg("network").arg("rm").arg(network_name);
        cmd
    }

    pub async fn create_network(
        &self,
        network_name: &str,
    ) -> Result<NetworkHandle, ContainerRuntimeError> {
        let output = self.create_network_cmd(network_name).output().await?;

        if !output.status.success() {
            // Replicating command construction as Command does not implement Clone
            let command = self.create_network_cmd(network_name);
            return Err(ProcessError::from_output(command, output).into());
        }

        Ok(NetworkHandle::new(self.clone(), network_name.to_string()))
    }

    pub async fn create_random_network_with_prefix(
        &self,
        network_prefix: &str,
    ) -> Result<NetworkHandle, ContainerRuntimeError> {
        let network_name = get_random_name(Some(network_prefix), 10);
        let output = self.create_network_cmd(&network_name).output().await?;

        if !output.status.success() {
            // Replicating command construction as Command does not implement Clone
            let command = self.create_network_cmd(&network_name);
            return Err(ProcessError::from_output(command, output).into());
        }

        Ok(NetworkHandle::new(self.clone(), network_name))
    }

    pub async fn has_network(&self, name: &str) -> Result<bool, ContainerRuntimeError> {
        let status = self
            .new_command()
            .arg("network")
            .arg("inspect")
            .arg(name)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await?;

        Ok(status.success())
    }

    pub async fn try_get_host_port(
        &self,
        container_name: &str,
        container_port: u16,
    ) -> Result<Option<u16>, ContainerRuntimeError> {
        if self.config.network_ns == NetworkNamespaceType::Host {
            return Ok(Some(container_port));
        }

        let format = format!(
            "--format={{{{ (index (index .NetworkSettings.Ports \"{container_port}/tcp\") \
             0).HostPort }}}}"
        );

        let output = self
            .new_command()
            .arg("inspect")
            .arg(format)
            .arg(container_name)
            .output()
            .await?;

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

    #[tracing::instrument(level = "debug", skip_all, fields(%container_name))]
    pub async fn wait_for_container(
        &self,
        container_name: &str,
        timeout: Duration,
    ) -> Result<(), WaitForResourceError> {
        let start = Instant::now();

        loop {
            let res = self
                .new_command()
                .args(["inspect", "--format", "{{ .State.Status }}"])
                .arg(container_name)
                .output()
                .await?;

            let stdout = std::str::from_utf8(&res.stdout).unwrap().trim();

            if res.status.success() {
                match stdout {
                    "running" => return Ok(()),
                    "created" => {}
                    _ => {
                        return Err(ResourceFailedError::new(format!(
                            "Container transitioned into '{stdout}' state"
                        ))
                        .into());
                    }
                }
            }

            if start.elapsed() >= timeout {
                return Err(TimeoutError::new(timeout).into());
            }

            tokio::time::sleep(Self::TICK_INTERVAL).await;
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%container_name))]
    pub async fn wait_for_host_port(
        &self,
        container_name: &str,
        container_port: u16,
        timeout: Duration,
    ) -> Result<u16, WaitForResourceError> {
        let start = Instant::now();

        loop {
            let res = self
                .try_get_host_port(container_name, container_port)
                .await?;
            if let Some(hp) = res {
                break Ok(hp);
            } else if start.elapsed() >= timeout {
                break Err(TimeoutError::new(timeout).into());
            }

            tokio::time::sleep(Self::TICK_INTERVAL).await;
        }
    }

    pub fn get_runtime_host_addr(&self) -> String {
        match self.config.runtime {
            ContainerRuntimeType::Podman => "127.0.0.1".to_owned(),
            ContainerRuntimeType::Docker => {
                if let Some(url) = std::env::var("DOCKER_HOST")
                    .ok()
                    .and_then(|s| url::Url::parse(&s).ok())
                {
                    if url.scheme() == "unix" {
                        // Rootless docker
                        "127.0.0.1".to_owned()
                    } else {
                        format!("{}", url.host().unwrap())
                    }
                } else {
                    "127.0.0.1".to_owned()
                }
            }
        }
    }

    pub fn check_socket(&self, host_port: u16) -> Result<bool, ContainerRuntimeError> {
        use std::io::Read;
        use std::net::{TcpStream, ToSocketAddrs};

        let saddr = format!("{}:{}", self.get_runtime_host_addr(), host_port);
        let addr = saddr
            .to_socket_addrs()?
            .next()
            .expect("Couldn't resolve local sockaddr");

        let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(100)) else {
            return Ok(false);
        };

        stream.set_read_timeout(Some(Duration::from_millis(1000)))?;

        let mut buf = [0; 1];
        match stream.read(&mut buf) {
            Ok(0) => Ok(false),
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%host_port, ?timeout))]
    pub async fn wait_for_socket(
        &self,
        host_port: u16,
        timeout: Duration,
    ) -> Result<(), WaitForResourceError> {
        let start = Instant::now();

        loop {
            if self.check_socket(host_port)? {
                break Ok(());
            } else if start.elapsed() >= timeout {
                break Err(TimeoutError::new(timeout).into());
            }

            tokio::time::sleep(Self::TICK_INTERVAL).await;
        }
    }

    pub fn get_random_free_port(&self) -> Result<u16, std::io::Error> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        Ok(listener.local_addr()?.port())
    }

    pub fn format_host_path(path: PathBuf) -> String {
        if !cfg!(windows) {
            path.to_str().unwrap().to_owned()
        } else {
            // boot2docker scenario
            let re = regex::Regex::new("([a-zA-Z]):").unwrap();
            let norm_path = Self::strip_unc(path);
            let s = norm_path.to_str().unwrap();
            re.replace(s, |caps: &regex::Captures| {
                format!("/{}", caps[1].to_lowercase())
            })
            .replace('\\', "/")
        }
    }

    pub fn format_container_path(path: &Path) -> String {
        if !cfg!(windows) {
            path.to_str().unwrap().to_owned()
        } else {
            // When formatting path on windows we may get wrong separators
            path.to_str().unwrap().replace('\\', "/")
        }
    }

    // TODO: move to utils
    fn strip_unc(path: PathBuf) -> PathBuf {
        if !cfg!(windows) {
            path
        } else {
            let s = path.to_str().unwrap();
            let s_norm = if let Some(end) = s.strip_prefix("\\\\?\\") {
                end
            } else {
                s
            };

            PathBuf::from(s_norm)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Copy, Clone)]
pub enum Signal {
    TERM,
    KILL,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(not(target_os = "linux"))]
fn is_selinux_present() -> bool {
    false
}

#[cfg(target_os = "linux")]
fn is_selinux_present() -> bool {
    use once_cell::sync::OnceCell;

    static FLAG: OnceCell<bool> = OnceCell::new();

    *FLAG.get_or_init(|| {
        let output = std::process::Command::new("command")
            .arg("-v")
            .arg("sestatus")
            .output();

        match output {
            Ok(output) => {
                let is_present = output.status.success();

                tracing::debug!("SELinux present: {is_present}");

                is_present
            }
            Err(e) => {
                tracing::warn!(error = ?e, "Error detecting SELinux presence");

                false
            }
        }
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
