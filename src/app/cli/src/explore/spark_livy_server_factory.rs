// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, SocketAddr};
use std::process::Stdio;
use std::sync::Arc;

use container_runtime::*;
use internal_error::*;

use crate::config::JupyterConfig;
use crate::{CommandRunError, SubprocessError, WorkspaceLayout};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
pub struct SparkLivyServerFactory {
    container_runtime: Arc<ContainerRuntime>,
    jupyter_config: Arc<JupyterConfig>,
    workspace_layout: Arc<WorkspaceLayout>,
}

impl SparkLivyServerFactory {
    pub async fn ensure_image(
        &self,
        listener: Option<&dyn PullImageListener>,
    ) -> Result<(), ImagePullError> {
        self.container_runtime
            .ensure_image(&self.jupyter_config.livy_image, listener)
            .await?;

        Ok(())
    }

    pub async fn start(
        &self,
        address: Option<IpAddr>,
        port: Option<u16>,
        inherit_stdio: bool,
        network: Option<&str>,
    ) -> Result<LivyContainer, CommandRunError> {
        const CONTAINER_PORT: u16 = 8998;

        let livy_stdout_path = self.workspace_layout.run_info_dir.join("livy.out.txt");
        let livy_stderr_path = self.workspace_layout.run_info_dir.join("livy.err.txt");

        let container = self
            .container_runtime
            .run_attached(&self.jupyter_config.livy_image)
            .random_container_name_with_prefix("kamu-livy-")
            .hostname("kamu-livy")
            .maybe(network.is_some(), |c| c.network(network.unwrap()))
            .map_or(
                address,
                |c, address| {
                    c.map_port_with_address(
                        address.to_string(),
                        port.expect("Must specify port"),
                        CONTAINER_PORT,
                    )
                },
                |c| c.map_port(port.unwrap_or(0), CONTAINER_PORT),
            )
            .user("root")
            .work_dir("/opt/bitnami/spark/work-dir")
            .volume((
                &self.workspace_layout.datasets_dir,
                "/opt/bitnami/spark/work-dir",
            ))
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

        let host_port = container
            .wait_for_host_port(CONTAINER_PORT, std::time::Duration::from_secs(30))
            .await
            .int_err()?;

        let addr = format!(
            "{}:{}",
            self.container_runtime.get_runtime_host_addr(),
            host_port
        )
        .parse()
        .int_err()?;

        Ok(LivyContainer {
            container_runtime: self.container_runtime.clone(),
            container,
            addr,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct LivyContainer {
    container_runtime: Arc<ContainerRuntime>,
    container: ContainerProcess,
    addr: SocketAddr,
}

impl LivyContainer {
    pub fn local_addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub async fn wait_for_socket(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), WaitForResourceError> {
        self.container_runtime
            .wait_for_socket(self.addr.port(), timeout)
            .await
    }
}

impl std::ops::Deref for LivyContainer {
    type Target = ContainerProcess;

    fn deref(&self) -> &Self::Target {
        &self.container
    }
}

impl std::ops::DerefMut for LivyContainer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.container
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
