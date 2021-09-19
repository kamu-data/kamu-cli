// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::infra::utils::docker_images;
use crate::infra::*;

use container_runtime::{ContainerHandle, ContainerRuntime, ExecArgs, PullImageListener, RunArgs};
use slog::{info, Logger};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub struct SqlShellImpl {
    container_runtime: Arc<ContainerRuntime>,
}

// TODO: Need to allocate pseudo-terminal to perfectly forward to the shell
impl SqlShellImpl {
    pub fn new(container_runtime: Arc<ContainerRuntime>) -> Self {
        Self { container_runtime }
    }

    pub fn ensure_images(&self, listener: &mut dyn PullImageListener) {
        self.container_runtime
            .ensure_image(docker_images::SPARK, Some(listener));
    }

    pub fn run_server(
        &self,
        workspace_layout: &WorkspaceLayout,
        volume_layout: &VolumeLayout,
        mut extra_volume_map: Vec<(PathBuf, PathBuf)>,
        logger: Logger,
        address: Option<&str>,
        port: Option<u16>,
    ) -> Result<std::process::Child, std::io::Error> {
        let cwd = Path::new(".").canonicalize().unwrap();

        let spark_stdout_path = workspace_layout.run_info_dir.join("spark.out.txt");
        let spark_stderr_path = workspace_layout.run_info_dir.join("spark.err.txt");
        let thrift_stdout_path = workspace_layout.run_info_dir.join("thrift.out.txt");
        let thrift_stderr_path = workspace_layout.run_info_dir.join("thrift.err.txt");

        // TODO: probably does not belong here?
        let exit = Arc::new(AtomicBool::new(false));
        signal_hook::flag::register(libc::SIGINT, exit.clone())?;
        signal_hook::flag::register(libc::SIGTERM, exit.clone())?;

        let mut volume_map = vec![(cwd, PathBuf::from("/opt/bitnami/spark/kamu_shell"))];
        if volume_layout.data_dir.exists() {
            volume_map.push((
                volume_layout.data_dir.clone(),
                PathBuf::from("/opt/bitnami/spark/kamu_data"),
            ));
        }
        volume_map.append(&mut extra_volume_map);

        // Start Spark container in the idle loop
        let spark = {
            let args = RunArgs {
                image: docker_images::SPARK.to_owned(),
                container_name: Some("kamu-spark".to_owned()),
                user: Some("root".to_owned()),
                volume_map,
                ..RunArgs::default()
            };

            let args = if let Some(p) = port {
                RunArgs {
                    network: Some("host".to_owned()),
                    expose_port_map_addr: vec![(
                        address.unwrap_or("127.0.0.1").to_owned(),
                        p,
                        10000,
                    )],
                    ..args
                }
            } else {
                RunArgs {
                    expose_ports: vec![10000],
                    ..args
                }
            };

            let mut cmd = self
                .container_runtime
                .run_shell_cmd(args, &["sleep".to_owned(), "999999".to_owned()]);

            info!(logger, "Starting Spark container"; "command" => ?cmd, "stdout" => ?spark_stdout_path, "stderr" => ?spark_stderr_path);

            cmd.stdin(Stdio::null())
                .stdout(Stdio::from(File::create(&spark_stdout_path)?))
                .stderr(Stdio::from(File::create(&spark_stderr_path)?))
                .spawn()?
        };

        info!(logger, "Waiting for container");
        self.container_runtime
            .wait_for_container("kamu-spark", std::time::Duration::from_secs(20))
            .expect("Container did not start");

        // Start Thrift Server process inside Spark container
        {
            let mut cmd = self.container_runtime.exec_shell_cmd(
                ExecArgs {
                    tty: false,
                    interactive: false,
                    work_dir: Some(PathBuf::from("/opt/bitnami/spark")),
                    ..ExecArgs::default()
                },
                "kamu-spark",
                &["sbin/start-thriftserver.sh"],
            );

            info!(logger, "Starting Thrift Server"; "command" => ?cmd);

            cmd.stdin(Stdio::null())
                .stdout(Stdio::from(File::create(&thrift_stdout_path)?))
                .stderr(Stdio::from(File::create(&thrift_stderr_path)?))
                .spawn()?
                .wait()?
                .exit_ok()
                .expect("Thrift server start script returned non-zero code");
        }

        let host_port = if let Some(p) = port {
            p
        } else {
            self.container_runtime
                .get_host_port("kamu-spark", 10000)
                .unwrap()
        };

        self.container_runtime
            .wait_for_socket(host_port, std::time::Duration::from_secs(60))
            .expect("Thrift Server did not start");

        Ok(spark)
    }

    pub fn run_shell<S1, S2>(
        &self,
        output_format: Option<S1>,
        command: Option<S2>,
        logger: Logger,
        url: String,
    ) -> Result<(), std::io::Error>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        info!(logger, "Starting SQL shell");

        let mut cmd = self.container_runtime.run_cmd(RunArgs {
            image: docker_images::SPARK.to_owned(),
            container_name: Some("kamu-spark-shell".to_owned()),
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
                    None => "".to_owned(),
                },
                match output_format {
                    Some(s) => format!("--outputformat={}", s.as_ref()),
                    None => "".to_owned(),
                },
            ],
            ..RunArgs::default()
        });

        cmd.spawn()?.wait()?;

        Ok(())
    }

    pub fn run_two_in_one<S1, S2, StartedClb>(
        &self,
        workspace_layout: &WorkspaceLayout,
        volume_layout: &VolumeLayout,
        output_format: Option<S1>,
        command: Option<S2>,
        logger: Logger,
        started_clb: StartedClb,
    ) -> Result<(), std::io::Error>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        StartedClb: FnOnce() + Send + 'static,
    {
        let tempdir = tempfile::tempdir()?;
        let init_script_path = tempdir.path().join("init.sql");
        std::fs::write(&init_script_path, Self::prepare_shell_init(volume_layout)?)?;

        let mut spark = self.run_server(
            workspace_layout,
            volume_layout,
            vec![(
                init_script_path,
                PathBuf::from("/opt/bitnami/spark/shell_init.sql"),
            )],
            logger.clone(),
            None,
            None,
        )?;

        {
            let _drop_spark = ContainerHandle::new(self.container_runtime.clone(), "kamu-spark");

            started_clb();
            info!(logger, "Starting SQL shell");

            // Relying on shell to send signal to child processes
            let mut beeline_cmd = self.container_runtime
                .exec_shell_cmd(
                    ExecArgs {
                        tty: true,
                        interactive: true,
                        work_dir: Some(PathBuf::from("/opt/bitnami/spark/kamu_shell")),
                    },
                    "kamu-spark",
                    &[
                        "../bin/beeline -u jdbc:hive2://localhost:10000 -i ../shell_init.sql --color=true".to_owned(),
                        match command {
                            Some(s) => format!("-e '{}'", s.as_ref()),
                            None => "".to_owned(),
                        },
                        match output_format {
                            Some(s) => format!("--outputformat={}", s.as_ref()),
                            None => "".to_owned(),
                        }
                    ],
                );

            info!(logger, "Running beeline"; "command" => ?beeline_cmd);
            beeline_cmd.spawn()?.wait()?;
        }

        spark.wait()?;

        Ok(())
    }

    pub fn run<S1, S2, StartedClb>(
        &self,
        workspace_layout: &WorkspaceLayout,
        volume_layout: &VolumeLayout,
        output_format: Option<S1>,
        url: Option<String>,
        command: Option<S2>,
        logger: Logger,
        started_clb: StartedClb,
    ) -> Result<(), std::io::Error>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        StartedClb: FnOnce() + Send + 'static,
    {
        if let Some(url) = url {
            started_clb();
            self.run_shell(output_format, command, logger, url)
        } else {
            self.run_two_in_one(
                workspace_layout,
                volume_layout,
                output_format,
                command,
                logger,
                started_clb,
            )
        }
    }

    fn prepare_shell_init(volume_layout: &VolumeLayout) -> Result<String, std::io::Error> {
        use std::fmt::Write;

        if !volume_layout.data_dir.exists() {
            return Ok(String::default());
        }

        let mut ret = String::with_capacity(2048);
        for entry in std::fs::read_dir(&volume_layout.data_dir)? {
            let p = entry?.path();
            writeln!(
                ret,
                "CREATE TEMP VIEW `{0}` AS (SELECT * FROM parquet.`kamu_data/{0}`);",
                p.file_name().unwrap().to_str().unwrap()
            )
            .unwrap();
        }
        Ok(ret)
    }
}
