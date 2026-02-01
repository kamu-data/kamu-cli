// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use container_runtime::*;
use internal_error::*;

use crate::WorkspaceLayout;
use crate::config::JupyterConfig;
use crate::error::{CommandRunError, SubprocessError};

#[dill::component]
pub struct NotebookServerFactory {
    container_runtime: Arc<ContainerRuntime>,
    jupyter_config: Arc<JupyterConfig>,
    workspace_layout: Arc<WorkspaceLayout>,
}

impl NotebookServerFactory {
    pub async fn ensure_image(
        &self,
        listener: Option<&dyn PullImageListener>,
    ) -> Result<(), ImagePullError> {
        self.container_runtime
            .ensure_image(&self.jupyter_config.image, listener)
            .await?;

        Ok(())
    }

    pub async fn start<StartedClb>(
        &self,
        client_url: &url::Url,
        engine: &str,
        address: Option<IpAddr>,
        port: Option<u16>,
        network: Option<&str>,
        environment_vars: Vec<(String, String)>,
        inherit_stdio: bool,
        on_started: StartedClb,
    ) -> Result<NotebookContainer, CommandRunError>
    where
        StartedClb: FnOnce(&str) + Send + 'static,
    {
        let container_port = 8080;

        let cwd = Path::new(".").canonicalize().unwrap();

        let stdout_path = self.workspace_layout.run_info_dir.join("jupyter.out.txt");
        let stderr_path = self.workspace_layout.run_info_dir.join("jupyter.err.txt");

        let mut contaniner = self
            .container_runtime
            .run_attached(&self.jupyter_config.image)
            .random_container_name_with_prefix("kamu-jupyter-")
            .user("root")
            // Start jupyter under root which suits better for rootless podman
            // See: https://github.com/jupyter/docker-stacks/pull/2039
            .environment_vars([
                ("NB_USER", "root"),
                ("NB_UID", "0"),
                ("NB_GID", "0"),
                ("KAMU_CLIENT_URL", client_url.as_str()),
                ("KAMU_CLIENT_ENGINE", engine),
            ])
            .work_dir("/opt/workdir")
            .map(network, container_runtime::ContainerRunCommand::network)
            .extra_host(("host.docker.internal", "host-gateway"))
            .map_or(
                address,
                |c, address| {
                    c.map_port_with_address(
                        address.to_string(),
                        port.expect("Must specify port"),
                        container_port,
                    )
                },
                |c| c.map_port(port.unwrap_or(0), container_port),
            )
            .volume((&cwd, "/opt/workdir"))
            .environment_vars(environment_vars)
            .args([
                "jupyter".to_owned(),
                "lab".to_owned(),
                "--allow-root".to_owned(),
                "--ip".to_owned(),
                "0.0.0.0".to_string(),
                "--port".to_owned(),
                container_port.to_string(),
            ])
            .stdout(if inherit_stdio {
                Stdio::inherit()
            } else {
                Stdio::from(std::fs::File::create(&stdout_path).int_err()?)
            })
            .stderr(Stdio::piped())
            .spawn()
            .int_err()?;

        let port = contaniner
            .wait_for_host_port(container_port, Duration::from_secs(30))
            .await
            .int_err()?;

        // FIXME: When used with FlightSQL we have notebook container communicating with
        // FlightSQL server running on the host using the special `host.docker.internal`
        // bridge. This however causes host traffic use external interface instead of
        // localhost and may result in firewalls trapping it. Here we perform a sanity
        // check that client address is reachable from the Jupyter container to display
        // a nice error message.
        if client_url.as_str().contains("host.docker.internal") {
            let cmd = format!(
                "nc -zv -w 5 {} {}",
                client_url.host_str().unwrap(),
                client_url.port().unwrap(),
            );

            tracing::info!(cmd, "Checking Jupyter to data server connectivity");

            let status = contaniner
                .exec_shell_cmd(ExecArgs::default(), cmd.clone())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await
                .int_err()?;

            if !status.success() {
                return Err(indoc::formatdoc!(
                    r#"
                    Connection test from Jupyter container failed:
                        {cmd}

                    Because Jupyter container is trying to reach the FlightSQL port on he host the traffic is routed through external interface and may be affected by the firewall. Consider checking your firewall rules and allowing traffic from private subnet. If the error doesn't go away - please submit and bug report!
                    "#
                )
                .trim()
                .int_err()
                .into());
            }
        }

        let host_addr = self.container_runtime.get_runtime_host_addr();

        let token_clb = move |token: &str| {
            let url = format!("http://{host_addr}:{port}/?token={token}");
            on_started(&url);
        };

        let token_extractor = if inherit_stdio {
            TokenExtractor::new(
                contaniner.take_stderr().unwrap(),
                tokio::io::stderr(),
                Some(token_clb),
            )
        } else {
            TokenExtractor::new(
                contaniner.take_stderr().unwrap(),
                tokio::fs::File::create(&stderr_path).await.map_err(|err| {
                    CommandRunError::SubprocessError(SubprocessError::new(
                        vec![stderr_path, stdout_path],
                        err,
                    ))
                })?,
                Some(token_clb),
            )
        };

        Ok(NotebookContainer {
            contaniner,
            token_extractor,
            container_runtime: self.container_runtime.clone(),
            chown_image: self.jupyter_config.image.clone(),
            chown_dir: cwd,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NotebookContainer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Wraps notebook container to perform post-exit permission fix-up
pub struct NotebookContainer {
    contaniner: ContainerProcess,
    token_extractor: TokenExtractor,
    container_runtime: Arc<ContainerRuntime>,
    chown_image: String,
    chown_dir: PathBuf,
}

impl NotebookContainer {
    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.contaniner.wait().await
    }

    pub async fn terminate(mut self) -> Result<(), CommandRunError> {
        self.contaniner.terminate().await.int_err()?;
        self.token_extractor.handle.await.int_err()?;

        // Fix permissions
        if self.container_runtime.config.runtime == ContainerRuntimeType::Docker {
            cfg_if::cfg_if! {
                if #[cfg(unix)] {
                    self.container_runtime
                        .run_attached(&self.chown_image)
                        .random_container_name_with_prefix("kamu-jupyter-permissions-")
                        .shell_cmd(format!(
                            "chown -Rf {}:{} {}",
                            unsafe { libc::geteuid() },
                            unsafe { libc::getegid() },
                            "/opt/workdir"
                        ))
                        .user("root")
                        .volume((&self.chown_dir, "/opt/workdir"))
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TokenExtractor
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Proxies STDOUT of the notebook server to fetch the authorization token
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

                if let Some(capture) = re.captures(&line)
                    && let Some(clb) = on_token.take()
                {
                    let token = capture.get(1).unwrap().as_str();
                    clb(token);
                }
            }
        });

        Self { handle }
    }
}
