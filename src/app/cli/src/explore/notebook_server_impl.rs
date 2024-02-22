// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use container_runtime::*;
use internal_error::*;

use crate::config::JupyterConfig;
use crate::error::{CommandRunError, SubprocessError};

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

    pub async fn ensure_images(
        &self,
        listener: &dyn PullImageListener,
    ) -> Result<(), ImagePullError> {
        self.container_runtime
            .ensure_image(
                self.jupyter_config.livy_image.as_ref().unwrap(),
                Some(listener),
            )
            .await?;

        self.container_runtime
            .ensure_image(self.jupyter_config.image.as_ref().unwrap(), Some(listener))
            .await?;

        Ok(())
    }

    pub async fn run<StartedClb, ShutdownClb>(
        &self,
        datasets_dir: &PathBuf,
        run_info_dir: &Path,
        address: Option<IpAddr>,
        port: Option<u16>,
        environment_vars: Vec<(String, String)>,
        inherit_stdio: bool,
        on_started: StartedClb,
        on_shutdown: ShutdownClb,
    ) -> Result<(), CommandRunError>
    where
        StartedClb: FnOnce(&str) + Send + 'static,
        ShutdownClb: FnOnce() + Send + 'static,
    {
        assert!(
            address.is_none(),
            "Exposing Notebook server on a host network interface is not yet supported"
        );

        let network = self
            .container_runtime
            .create_random_network_with_prefix("kamu-")
            .await
            .int_err()?;
        let network_name = network.name();

        let cwd = Path::new(".").canonicalize().unwrap();

        let livy_stdout_path = run_info_dir.join("livy.out.txt");
        let livy_stderr_path = run_info_dir.join("livy.err.txt");
        let jupyter_stdout_path = run_info_dir.join("jupyter.out.txt");
        let jupyter_stderr_path = run_info_dir.join("jupyter.err.txt");

        let mut livy = self
            .container_runtime
            .run_attached(self.jupyter_config.livy_image.as_ref().unwrap())
            .random_container_name_with_prefix("kamu-livy-")
            .hostname("kamu-livy")
            .network(network_name)
            .user("root")
            .work_dir("/opt/bitnami/spark/work-dir")
            .volume((&datasets_dir, "/opt/bitnami/spark/work-dir"))
            .entry_point("/opt/livy/bin/livy-server")
            .stdout(if inherit_stdio {
                Stdio::inherit()
            } else {
                Stdio::from(std::fs::File::create(&livy_stdout_path).int_err()?)
            })
            .stderr(if inherit_stdio {
                Stdio::inherit()
            } else {
                Stdio::from(std::fs::File::create(&livy_stderr_path).int_err()?)
            })
            .spawn()
            .map_err(|err| {
                CommandRunError::SubprocessError(SubprocessError::new(
                    vec![livy_stderr_path, livy_stdout_path],
                    err,
                ))
            })?;

        let jupyter_port_in_container = port.unwrap_or(8080);
        let mut jupyter = self
            .container_runtime
            .run_attached(self.jupyter_config.image.as_ref().unwrap())
            .random_container_name_with_prefix("kamu-jupyter-")
            .network(network_name)
            .user("root")
            // Start jupyter under root which suits better for rootless podman
            // See: https://github.com/jupyter/docker-stacks/pull/2039
            .environment_vars([("NB_USER", "root"), ("NB_UID", "0"), ("NB_GID", "0")])
            .work_dir("/opt/workdir")
            .expose_port(jupyter_port_in_container)
            .volume((&cwd, "/opt/workdir"))
            .environment_vars(environment_vars)
            .args([
                "jupyter".to_owned(),
                "notebook".to_owned(),
                "--allow-root".to_owned(),
                "--ip".to_owned(),
                address
                    .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
                    .to_string(),
                "--port".to_owned(),
                jupyter_port_in_container.to_string(),
                // TODO: Remove show_banner option after Sparkmagic supports novebook >= 7.0.0
                // See: https://github.com/jupyter-incubator/sparkmagic/issues/885
                "--NotebookApp.show_banner=False".to_string(),
            ])
            .stdout(if inherit_stdio {
                Stdio::inherit()
            } else {
                Stdio::from(std::fs::File::create(&jupyter_stdout_path).int_err()?)
            })
            .stderr(Stdio::piped())
            .spawn()
            .int_err()?;

        let docker_host = self.container_runtime.get_runtime_host_addr();
        let jupyter_port = jupyter
            .wait_for_host_socket(jupyter_port_in_container, Duration::from_secs(10))
            .await
            .int_err()?;

        let token_clb = move |token: &str| {
            let url = format!("http://{docker_host}:{jupyter_port}/?token={token}");
            on_started(&url);
        };

        let token_extractor = if inherit_stdio {
            TokenExtractor::new(
                jupyter.take_stderr().unwrap(),
                tokio::io::stderr(),
                Some(token_clb),
            )
        } else {
            TokenExtractor::new(
                jupyter.take_stderr().unwrap(),
                tokio::fs::File::create(&jupyter_stderr_path)
                    .await
                    .map_err(|err| {
                        CommandRunError::SubprocessError(SubprocessError::new(
                            vec![jupyter_stderr_path, jupyter_stdout_path],
                            err,
                        ))
                    })?,
                Some(token_clb),
            )
        };

        let exit = Arc::new(AtomicBool::new(false));
        signal_hook::flag::register(libc::SIGINT, exit.clone()).int_err()?;
        signal_hook::flag::register(libc::SIGTERM, exit.clone()).int_err()?;

        // TODO: Detect crashed processes
        // Relying on shell to send signal to child processes
        while !exit.load(Ordering::Relaxed) {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        on_shutdown();

        jupyter.terminate().await.int_err()?;
        livy.terminate().await.int_err()?;
        network.free().await.int_err()?;
        token_extractor.handle.await.int_err()?;

        // Fix permissions
        if self.container_runtime.config.runtime == ContainerRuntimeType::Docker {
            cfg_if::cfg_if! {
                if #[cfg(unix)] {
                    self.container_runtime
                        .run_attached(self.jupyter_config.image.as_ref().unwrap())
                        .random_container_name_with_prefix("kamu-jupyter-permissions-")
                        .shell_cmd(format!(
                            "chown -Rf {}:{} {}",
                            unsafe { libc::geteuid() },
                            unsafe { libc::getegid() },
                            "/opt/workdir"
                        ))
                        .user("root")
                        .volume((cwd, "/opt/workdir"))
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                        .await
                        .int_err()?;
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
    handle: tokio::task::JoinHandle<()>,
}

impl TokenExtractor {
    fn new<R, W, Clb>(input: R, mut output: W, mut on_token: Option<Clb>) -> Self
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
        Clb: FnOnce(&str) + Send + 'static,
    {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

        let handle = tokio::spawn(async move {
            let re = regex::Regex::new("token=([a-z0-9]+)").unwrap();
            let mut reader = tokio::io::BufReader::new(input);
            let mut line = String::with_capacity(1024);
            loop {
                line.clear();
                if reader.read_line(&mut line).await.unwrap() == 0 {
                    output.flush().await.unwrap();
                    break;
                }

                output.write_all(line.as_bytes()).await.unwrap();

                if let Some(capture) = re.captures(&line) {
                    if let Some(clb) = on_token.take() {
                        let token = capture.get(1).unwrap().as_str();
                        clb(token);
                    }
                }
            }
        });

        Self { handle }
    }
}
