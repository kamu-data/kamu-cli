// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::File;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use container_runtime::*;
use internal_error::*;
use odf::dataset::DatasetLayout;
use random_strings::{get_random_string, AllowedSymbols};

use crate::error::{CommandRunError, SubprocessError};
use crate::WorkspaceLayout;

pub struct SqlShellImpl {
    container_runtime: Arc<ContainerRuntime>,
    image: String,
}

// TODO: Need to allocate pseudo-terminal to perfectly forward to the shell
impl SqlShellImpl {
    pub fn new(container_runtime: Arc<ContainerRuntime>, image: String) -> Self {
        Self {
            container_runtime,
            image,
        }
    }

    pub async fn ensure_images(
        &self,
        listener: &mut dyn PullImageListener,
    ) -> Result<(), ImagePullError> {
        self.container_runtime
            .ensure_image(&self.image, Some(listener))
            .await
    }

    pub async fn run_server(
        &self,
        workspace_layout: &WorkspaceLayout,
        extra_volume_map: Vec<(PathBuf, PathBuf)>,
        address: Option<&IpAddr>,
        port: Option<u16>,
    ) -> Result<ContainerProcess, CommandRunError> {
        let cwd = Path::new(".").canonicalize().unwrap();

        let spark_stdout_path = workspace_layout.run_info_dir.join("spark.out.txt");
        let spark_stderr_path = workspace_layout.run_info_dir.join("spark.err.txt");
        let thrift_stdout_path = workspace_layout.run_info_dir.join("thrift.out.txt");
        let thrift_stderr_path = workspace_layout.run_info_dir.join("thrift.err.txt");

        // Start container in the idle loop
        let container = {
            let mut container_builder = self
                .container_runtime
                .run_attached(&self.image)
                .shell_cmd("sleep 999999")
                .random_container_name_with_prefix("kamu-spark-")
                .user("root")
                .volumes([
                    (cwd, "/opt/bitnami/spark/kamu_shell"),
                    (
                        workspace_layout.datasets_dir.clone(),
                        "/opt/bitnami/spark/kamu_data",
                    ),
                ])
                .volumes(extra_volume_map)
                .stdin(Stdio::null())
                .stdout(std::fs::File::create(&spark_stdout_path).int_err()?)
                .stderr(std::fs::File::create(&spark_stderr_path).int_err()?);

            container_builder = if let Some(p) = port {
                container_builder.map_port_with_address(
                    address.map_or(String::from("127.0.0.1"), ToString::to_string),
                    p,
                    10000,
                )
            } else {
                container_builder.expose_port(10000)
            };

            let container = container_builder.spawn().map_err(|err| {
                CommandRunError::SubprocessError(SubprocessError::new(
                    vec![spark_stderr_path, spark_stdout_path],
                    err,
                ))
            })?;

            container
                .wait_for_container(Duration::from_secs(30))
                .await
                .int_err()?;

            container
        };

        // Start Thrift Server process inside Spark container
        {
            let mut command = container.exec_shell_cmd(
                ExecArgs {
                    work_dir: Some(PathBuf::from("/opt/bitnami/spark")),
                    ..ExecArgs::default()
                },
                "sbin/start-thriftserver.sh",
            );

            tracing::info!(?command, "Starting Thrift Server");

            command
                .stdin(Stdio::null())
                .stdout(std::fs::File::create(&thrift_stdout_path).int_err()?)
                .stderr(std::fs::File::create(&thrift_stderr_path).int_err()?)
                .status()
                .await
                .map_err(|err| {
                    CommandRunError::SubprocessError(SubprocessError::new(
                        vec![thrift_stderr_path, thrift_stdout_path],
                        err,
                    ))
                })?
                .exit_ok()
                .int_err()?;
        }

        let host_port = if let Some(p) = port {
            p
        } else {
            container.try_get_host_port(10000).await.int_err()?.unwrap()
        };

        self.container_runtime
            .wait_for_socket(host_port, Duration::from_secs(60))
            .await
            .int_err()?;

        Ok(container)
    }

    pub async fn run_shell<S1, S2>(
        &self,
        output_format: Option<S1>,
        command: Option<S2>,
        url: String,
        run_info_dir: &Path,
    ) -> Result<(), CommandRunError>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        tracing::info!("Starting SQL shell");
        let stderr_file_path = run_info_dir.join("sql-shell.err.log");

        let mut cmd = self.container_runtime.run_cmd(RunArgs {
            image: self.image.clone(),
            container_name: Some(get_random_string(
                Some("kamu-spark-shell-"),
                10,
                &AllowedSymbols::Alphanumeric,
            )),
            user: Some("root".to_owned()),
            network: Some("host".to_owned()),
            tty: true,
            interactive: true,
            entry_point: Some("bash".to_owned()),
            args: vec![
                String::from("/opt/bitnami/spark/bin/beeline"),
                String::from("-u"),
                url,
                String::from("--color=true"),
                match command {
                    Some(s) => format!("-e '{}'", s.as_ref()),
                    None => String::new(),
                },
                match output_format {
                    Some(s) => format!("--outputformat={}", s.as_ref()),
                    None => String::new(),
                },
            ],
            ..RunArgs::default()
        });

        cmd.stderr(File::create(&stderr_file_path).int_err()?)
            .spawn()
            .map_err(|err| {
                CommandRunError::SubprocessError(SubprocessError::new(vec![stderr_file_path], err))
            })?
            .wait()
            .await
            .int_err()?;

        Ok(())
    }

    pub async fn run_two_in_one<S1, S2, StartedClb>(
        &self,
        workspace_layout: &WorkspaceLayout,
        output_format: Option<S1>,
        command: Option<S2>,
        started_clb: StartedClb,
    ) -> Result<(), InternalError>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        StartedClb: FnOnce() + Send + 'static,
    {
        let init_script_path = workspace_layout.run_info_dir.join("shell_init.sql");
        std::fs::write(
            &init_script_path,
            Self::prepare_shell_init(workspace_layout).int_err()?,
        )
        .int_err()?;

        let mut container = self
            .run_server(
                workspace_layout,
                vec![(
                    init_script_path,
                    PathBuf::from("/opt/bitnami/spark/shell_init.sql"),
                )],
                None,
                None,
            )
            .await
            .int_err()?;

        {
            started_clb();
            tracing::info!("Starting SQL shell");

            let beeline_command = format!(
                "../bin/beeline -u jdbc:hive2://localhost:10000 -i ../shell_init.sql \
                 --color=true{}{}",
                match command {
                    Some(s) => format!("-e '{}'", s.as_ref()),
                    None => String::new(),
                },
                match output_format {
                    Some(s) => format!("--outputformat={}", s.as_ref()),
                    None => String::new(),
                },
            );

            let mut beeline_cmd = container.exec_shell_cmd(
                ExecArgs {
                    tty: true,
                    interactive: true,
                    work_dir: Some(PathBuf::from("/opt/bitnami/spark/kamu_shell")),
                },
                beeline_command,
            );

            tracing::info!(command = ?beeline_cmd, "Running beeline");

            // Relying on shell to send signal to child processes
            beeline_cmd.spawn().int_err()?.wait().await.int_err()?;
        }

        container.terminate().await.int_err()?;

        Ok(())
    }

    pub async fn run<S1, S2, StartedClb>(
        &self,
        workspace_layout: &WorkspaceLayout,
        output_format: Option<S1>,
        url: Option<String>,
        command: Option<S2>,
        started_clb: StartedClb,
    ) -> Result<(), InternalError>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        StartedClb: FnOnce() + Send + 'static,
    {
        if let Some(url) = url {
            started_clb();
            self.run_shell(output_format, command, url, &workspace_layout.run_info_dir)
                .await
                .int_err()
        } else {
            self.run_two_in_one(workspace_layout, output_format, command, started_clb)
                .await
        }
    }

    // TODO: Too many layout assumptions here
    fn prepare_shell_init(workspace_layout: &WorkspaceLayout) -> Result<String, std::io::Error> {
        use std::fmt::Write;

        let mut ret = String::with_capacity(2048);
        for entry in std::fs::read_dir(&workspace_layout.datasets_dir)? {
            let p = entry?.path();
            if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                if name.starts_with('.') {
                    continue;
                }

                let layout = DatasetLayout::new(&p);
                if layout.data_dir.exists() {
                    writeln!(
                        ret,
                        "CREATE TEMP VIEW `{name}` AS (SELECT * FROM \
                         parquet.`kamu_data/{name}/data`);"
                    )
                    .unwrap();
                }
            }
        }
        Ok(ret)
    }
}
