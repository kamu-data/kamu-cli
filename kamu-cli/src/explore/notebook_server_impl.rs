// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::infra::*;

use container_runtime::{
    ContainerHandle, ContainerRuntime, ContainerRuntimeType, PullImageListener, RunArgs,
};
use std::fs::File;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::info;

use crate::JupyterConfig;

pub struct NotebookServerImpl {
    container_runtime: Arc<ContainerRuntime>,
    jupyter_config: Arc<JupyterConfig>,
}

impl NotebookServerImpl {
    pub fn new(
        container_runtime: Arc<ContainerRuntime>,
        jupyter_config: Arc<JupyterConfig>,
    ) -> Self {
        Self {
            container_runtime,
            jupyter_config,
        }
    }

    pub fn ensure_images(&self, listener: &dyn PullImageListener) {
        self.container_runtime.ensure_image(
            self.jupyter_config.livy_image.as_ref().unwrap(),
            Some(listener),
        );
        self.container_runtime
            .ensure_image(self.jupyter_config.image.as_ref().unwrap(), Some(listener));
    }

    pub fn run<StartedClb, ShutdownClb>(
        &self,
        workspace_layout: &WorkspaceLayout,
        address: Option<IpAddr>,
        port: Option<u16>,
        environment_vars: Vec<(String, String)>,
        inherit_stdio: bool,
        on_started: StartedClb,
        on_shutdown: ShutdownClb,
    ) -> Result<(), std::io::Error>
    where
        StartedClb: FnOnce(&str) + Send + 'static,
        ShutdownClb: FnOnce() + Send + 'static,
    {
        if address.is_some() {
            panic!("Exposing Notebook server on a host network interface is not yet supported");
        }

        let network_name = "kamu";

        // Delete network if exists from previous run
        let _ = self
            .container_runtime
            .remove_network_cmd(network_name)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .output();

        let _network = self.container_runtime.create_network(network_name);

        let cwd = Path::new(".").canonicalize().unwrap();

        let livy_stdout_path = workspace_layout.run_info_dir.join("livy.out.txt");
        let livy_stderr_path = workspace_layout.run_info_dir.join("livy.err.txt");
        let jupyter_stdout_path = workspace_layout.run_info_dir.join("jupyter.out.txt");
        let jupyter_stderr_path = workspace_layout.run_info_dir.join("jupyter.err.txt");

        let mut livy_cmd = self.container_runtime.run_cmd(RunArgs {
            image: self.jupyter_config.livy_image.clone().unwrap(),
            container_name: Some("kamu-livy".to_owned()),
            hostname: Some("kamu-livy".to_owned()),
            network: Some(network_name.to_owned()),
            user: Some("root".to_owned()),
            work_dir: Some(PathBuf::from("/opt/bitnami/spark/work-dir")),
            volume_map: vec![(
                workspace_layout.datasets_dir.clone(),
                PathBuf::from("/opt/bitnami/spark/work-dir"),
            )],
            entry_point: Some("/opt/livy/bin/livy-server".to_owned()),
            ..RunArgs::default()
        });

        info!(command = ?livy_cmd, "Starting Livy container");

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

        let mut jupyter_cmd = self.container_runtime.run_cmd(RunArgs {
            image: self.jupyter_config.image.clone().unwrap(),
            container_name: Some("kamu-jupyter".to_owned()),
            network: Some(network_name.to_owned()),
            user: Some("root".to_owned()),
            work_dir: Some(PathBuf::from("/opt/workdir")),
            expose_ports: vec![80],
            volume_map: vec![(cwd.clone(), PathBuf::from("/opt/workdir"))],
            environment_vars: environment_vars,
            args: vec![
                "jupyter".to_owned(),
                "notebook".to_owned(),
                "--allow-root".to_owned(),
                "--ip".to_owned(),
                address
                    .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
                    .to_string(),
                "--port".to_owned(),
                port.unwrap_or(80).to_string(),
            ],
            ..RunArgs::default()
        });

        info!(command = ?jupyter_cmd, "Starting Jupyter container");

        let mut jupyter = jupyter_cmd
            .stdout(if inherit_stdio {
                Stdio::inherit()
            } else {
                Stdio::from(File::create(&jupyter_stdout_path)?)
            })
            .stderr(Stdio::piped())
            .spawn()?;
        let _drop_jupyter = ContainerHandle::new(self.container_runtime.clone(), "kamu-jupyter");

        let docker_host = self.container_runtime.get_runtime_host_addr();
        let jupyter_port = self
            .container_runtime
            .wait_for_host_port("kamu-jupyter", 80, std::time::Duration::from_secs(5))
            .unwrap_or_default();
        let token_clb = move |token: &str| {
            let url = format!("http://{}:{}/?token={}", docker_host, jupyter_port, token);
            on_started(&url);
        };

        let token_extractor = if inherit_stdio {
            TokenExtractor::new(
                jupyter.stderr.take().unwrap(),
                std::io::stderr(),
                Some(token_clb),
            )?
        } else {
            TokenExtractor::new(
                jupyter.stderr.take().unwrap(),
                File::create(&jupyter_stderr_path)?,
                Some(token_clb),
            )?
        };

        let exit = Arc::new(AtomicBool::new(false));
        signal_hook::flag::register(libc::SIGINT, exit.clone())?;
        signal_hook::flag::register(libc::SIGTERM, exit.clone())?;

        // TODO: Detect crashed processes
        // Relying on shell to send signal to child processes
        while !exit.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        on_shutdown();

        jupyter.wait()?;
        livy.wait()?;
        token_extractor.handle.join().unwrap();

        // Fix permissions
        if self.container_runtime.config.runtime == ContainerRuntimeType::Docker {
            cfg_if::cfg_if! {
                if #[cfg(unix)] {
                    self.container_runtime
                        .run_shell_cmd(
                            RunArgs {
                                image: self.jupyter_config.image.clone().unwrap(),
                                user: Some("root".to_owned()),
                                container_name: Some("kamu-jupyter".to_owned()),
                                volume_map: vec![(cwd, PathBuf::from("/opt/workdir"))],
                                ..RunArgs::default()
                            },
                            &[format!(
                                "chown -R {}:{} {}",
                                users::get_current_uid(),
                                users::get_current_gid(),
                                "/opt/workdir"
                            )],
                        )
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()?;
                }
            }
        }

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////
// TokenExtractor
///////////////////////////////////////////////////////////////////////////////

struct TokenExtractor {
    handle: std::thread::JoinHandle<()>,
}

impl TokenExtractor {
    fn new<R, W, Clb>(
        input: R,
        mut output: W,
        mut on_token: Option<Clb>,
    ) -> Result<Self, std::io::Error>
    where
        R: std::io::Read + Send + 'static,
        W: std::io::Write + Send + 'static,
        Clb: FnOnce(&str) + Send + 'static,
    {
        let handle = std::thread::Builder::new()
            .name("jupyter-io".to_owned())
            .spawn({
                move || {
                    use std::io::BufRead;
                    let mut reader = std::io::BufReader::new(input);
                    let mut line = String::with_capacity(128);
                    let re = regex::Regex::new("token=([a-z0-9]+)").unwrap();
                    loop {
                        line.clear();
                        if reader.read_line(&mut line).unwrap() == 0 {
                            break;
                        }
                        output.write_all(line.as_bytes()).unwrap();
                        if let Some(capture) = re.captures(&line) {
                            if let Some(clb) = on_token.take() {
                                let token = capture.get(1).unwrap().as_str();
                                clb(token);
                            }
                        }
                    }
                }
            })?;
        Ok(Self { handle: handle })
    }
}
