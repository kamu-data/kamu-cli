// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use super::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ContainerRuntimeCommon;

/// Provides command constructors that can later be executed by the specific
/// version of the runtime
impl ContainerRuntimeCommon {
    pub fn new_command(config: &ContainerRuntimeConfig) -> Command {
        Command::new(match config.runtime {
            ContainerRuntimeType::Docker => "docker",
            ContainerRuntimeType::Podman => "podman",
        })
    }

    pub fn has_image_cmd(config: &ContainerRuntimeConfig, image: &str) -> Command {
        let mut cmd = Self::new_command(config);
        cmd.arg("image").arg("inspect").arg(image);
        cmd
    }

    pub fn pull_cmd(config: &ContainerRuntimeConfig, image: &str) -> Command {
        let mut cmd = Self::new_command(config);
        cmd.arg("pull").arg(image);
        cmd
    }

    pub fn run_cmd(config: &ContainerRuntimeConfig, args: RunArgs) -> Command {
        let mut cmd = Self::new_command(config);
        cmd.arg("run");
        if args.remove {
            cmd.arg("--rm");
        }
        if args.tty {
            cmd.arg("-t");
        }
        if args.interactive {
            cmd.arg("-i");
        }
        if args.detached {
            cmd.arg("-d");
        }
        args.container_name
            .map(|v| cmd.arg(format!("--name={}", v)));
        args.hostname.map(|v| cmd.arg(format!("--hostname={}", v)));
        args.network.map(|v| cmd.arg(format!("--network={}", v)));
        if args.expose_all_ports {
            cmd.arg("-P");
        }
        args.expose_ports.iter().for_each(|v| {
            cmd.arg("-p");
            cmd.arg(format!("{}", v));
        });
        args.expose_port_map.iter().for_each(|(h, c)| {
            cmd.arg("-p");
            cmd.arg(format!("{}:{}", h, c));
        });
        args.expose_port_map_addr.iter().for_each(|(addr, h, c)| {
            cmd.arg("-p");
            cmd.arg(format!("{}:{}:{}", addr, h, c));
        });
        args.expose_port_map_range
            .iter()
            .for_each(|((hl, hr), (cl, cr))| {
                cmd.arg("-p");
                cmd.arg(format!("{}-{}:{}-{}", hl, hr, cl, cr));
            });
        args.volume_map.into_iter().for_each(|(h, c)| {
            cmd.arg("-v");
            cmd.arg(format!(
                "{}:{}",
                Self::format_host_path(h),
                Self::format_container_path(c),
            ));
        });
        args.user.map(|v| cmd.arg(format!("--user={}", v)));
        args.work_dir
            .map(|v| cmd.arg(format!("--workdir={}", v.display())));
        args.environment_vars.iter().for_each(|(n, v)| {
            cmd.arg("-e");
            cmd.arg(format!("{}={}", n, v));
        });
        args.entry_point
            .map(|v| cmd.arg(format!("--entrypoint={}", v)));
        cmd.arg(args.image);
        cmd.args(args.args);
        cmd
    }

    pub fn run_shell_cmd<I, S>(
        config: &ContainerRuntimeConfig,
        args: RunArgs,
        shell_cmd: I,
    ) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let shell_cmd_vec: Vec<String> = shell_cmd
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();

        Self::run_cmd(
            config,
            RunArgs {
                entry_point: Some("bash".to_owned()),
                args: vec!["-c".to_owned(), shell_cmd_vec.join(" ")],
                ..args
            },
        )
    }

    pub fn exec_cmd<I, S>(
        config: &ContainerRuntimeConfig,
        exec_args: ExecArgs,
        container_name: &str,
        cmd_args: I,
    ) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let mut cmd = Self::new_command(config);
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

    pub fn exec_shell_cmd<I, S>(
        config: &ContainerRuntimeConfig,
        exec_args: ExecArgs,
        container_name: &str,
        shell_cmd: I,
    ) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let shell_cmd_vec: Vec<String> = shell_cmd
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();

        let args = vec!["bash".to_owned(), "-c".to_owned(), shell_cmd_vec.join(" ")];
        Self::exec_cmd(config, exec_args, container_name, args)
    }

    pub fn kill_cmd(config: &ContainerRuntimeConfig, container_name: &str) -> Command {
        let mut cmd = Self::new_command(config);
        cmd.arg("kill").arg(container_name);
        cmd
    }

    pub fn create_network_cmd(config: &ContainerRuntimeConfig, network_name: &str) -> Command {
        let mut cmd = Self::new_command(config);
        cmd.arg("network").arg("create").arg(network_name);
        cmd
    }

    pub fn remove_network_cmd(config: &ContainerRuntimeConfig, network_name: &str) -> Command {
        let mut cmd = Self::new_command(config);
        cmd.arg("network").arg("rm").arg(network_name);
        cmd
    }

    pub fn inspect_host_port_cmd(
        config: &ContainerRuntimeConfig,
        container_name: &str,
        container_port: u16,
    ) -> Command {
        let format = format!(
            "--format={{{{ (index (index .NetworkSettings.Ports \"{}/tcp\") 0).HostPort }}}}",
            container_port
        );

        let mut cmd = Self::new_command(config);
        cmd.arg("inspect").arg(format).arg(container_name);
        cmd
    }

    pub fn get_runtime_host_addr(config: &ContainerRuntimeConfig) -> String {
        match config.runtime {
            ContainerRuntimeType::Podman => "127.0.0.1".to_owned(),
            ContainerRuntimeType::Docker => std::env::var("DOCKER_HOST")
                .ok()
                .and_then(|s| url::Url::parse(&s).ok())
                .map(|url| format!("{}", url.host().unwrap()))
                .unwrap_or("127.0.0.1".to_owned()),
        }
    }

    pub fn check_socket(
        config: &ContainerRuntimeConfig,
        host_port: u16,
    ) -> Result<bool, ContainerRuntimeError> {
        use std::io::Read;
        use std::net::{TcpStream, ToSocketAddrs};

        let saddr = format!("{}:{}", Self::get_runtime_host_addr(config), host_port);
        let addr = saddr
            .to_socket_addrs()?
            .next()
            .expect("Couldn't resolve local sockaddr");

        let mut stream = match TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            Ok(s) => s,
            _ => return Ok(false),
        };

        stream.set_read_timeout(Some(Duration::from_millis(1000)))?;

        let mut buf = [0; 1];
        match stream.read(&mut buf) {
            Ok(0) => Ok(false),
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => Ok(true),
            Err(e) => Err(e.into()),
        }
    }

    pub fn format_host_path(path: PathBuf) -> String {
        if !cfg!(windows) {
            path.to_str().unwrap().to_owned()
        } else {
            // Boot2Docker scenario
            let re = regex::Regex::new("([a-zA-Z]):").unwrap();
            let norm_path = Self::strip_unc(path);
            let s = norm_path.to_str().unwrap();
            re.replace(s, |caps: &regex::Captures| {
                format!("/{}", caps[1].to_lowercase())
            })
            .replace("\\", "/")
        }
    }

    pub fn format_container_path(path: PathBuf) -> String {
        if !cfg!(windows) {
            path.to_str().unwrap().to_owned()
        } else {
            // When formatting path on windows we may get wrong separators
            path.to_str().unwrap().replace("\\", "/")
        }
    }

    // TODO: move to utils
    fn strip_unc(path: PathBuf) -> PathBuf {
        if !cfg!(windows) {
            path
        } else {
            let s = path.to_str().unwrap();
            let s_norm = if s.starts_with("\\\\?\\") {
                &s[4..]
            } else {
                &s
            };
            PathBuf::from(s_norm)
        }
    }
}
