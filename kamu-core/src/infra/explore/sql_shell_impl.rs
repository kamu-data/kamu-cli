use crate::domain::PullImageListener;
use crate::infra::utils::docker_client::*;
use crate::infra::utils::docker_images;
use crate::infra::*;

use slog::{info, Logger};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub struct SqlShellImpl;

// TODO: Need to allocate pseudo-terminal to perfectly forward to the shell
impl SqlShellImpl {
    pub fn ensure_images(listener: &mut dyn PullImageListener) {
        let docker_client = DockerClient::new();
        docker_client.ensure_image(docker_images::SPARK, Some(listener));
    }

    pub fn run_server(
        workspace_layout: &WorkspaceLayout,
        volume_layout: &VolumeLayout,
        logger: Logger,
        address: Option<&str>,
        port: Option<u16>,
    ) -> Result<std::process::Child, std::io::Error> {
        let tempdir = tempfile::tempdir()?;
        let init_script_path = tempdir.path().join("init.sql");
        std::fs::write(&init_script_path, Self::prepare_shell_init(volume_layout)?)?;

        let docker_client = DockerClient::new();

        let cwd = Path::new(".").canonicalize().unwrap();

        let spark_stdout_path = workspace_layout.run_info_dir.join("spark.out.txt");
        let spark_stderr_path = workspace_layout.run_info_dir.join("spark.err.txt");

        // TODO: probably does not belong here?
        let exit = Arc::new(AtomicBool::new(false));
        signal_hook::flag::register(libc::SIGINT, exit.clone())?;
        signal_hook::flag::register(libc::SIGTERM, exit.clone())?;

        let args = DockerRunArgs {
            image: docker_images::SPARK.to_owned(),
            container_name: Some("kamu-spark".to_owned()),
            user: Some("root".to_owned()),
            volume_map: if volume_layout.data_dir.exists() {
                vec![
                    (
                        volume_layout.data_dir.clone(),
                        PathBuf::from("/opt/spark/kamu_data"),
                    ),
                    (cwd, PathBuf::from("/opt/spark/kamu_shell")),
                    (init_script_path, PathBuf::from("/opt/spark/shell_init.sql")),
                ]
            } else {
                vec![]
            },
            args: vec![String::from("sleep"), String::from("999999")],
            ..DockerRunArgs::default()
        };

        let args = if let Some(p) = port {
            DockerRunArgs {
                network: Some("host".to_owned()),
                expose_port_map_addr: vec![(address.unwrap_or("127.0.0.1").to_owned(), p, 10000)],
                ..args
            }
        } else {
            DockerRunArgs {
                expose_ports: vec![10000],
                ..args
            }
        };

        let mut cmd = docker_client.run_cmd(args);

        info!(logger, "Starting Spark container"; "command" => ?cmd, "stdout" => ?spark_stdout_path, "stderr" => ?spark_stderr_path);

        let spark = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::from(File::create(&spark_stdout_path)?))
            .stderr(Stdio::from(File::create(&spark_stderr_path)?))
            .spawn()?;

        info!(logger, "Waiting for container");
        docker_client
            .wait_for_container("kamu-spark", std::time::Duration::from_secs(20))
            .expect("Container did not start");

        info!(logger, "Starting Thrift Server");
        docker_client
            .exec_shell_cmd(
                ExecArgs {
                    tty: false,
                    interactive: false,
                    work_dir: Some(PathBuf::from("/opt/spark")),
                    ..ExecArgs::default()
                },
                "kamu-spark",
                &["sbin/start-thriftserver.sh"],
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?
            .wait()?;

        let host_port = if let Some(p) = port {
            p
        } else {
            docker_client.get_host_port("kamu-spark", 10000).unwrap()
        };

        docker_client
            .wait_for_socket(host_port, std::time::Duration::from_secs(60))
            .expect("Thrift Server did not start");

        Ok(spark)
    }

    pub fn run_shell<S1, S2>(
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

        let docker_client = DockerClient::new();
        let mut cmd = docker_client.run_cmd(DockerRunArgs {
            image: docker_images::SPARK.to_owned(),
            container_name: Some("kamu-spark-shell".to_owned()),
            user: Some("root".to_owned()),
            network: Some("host".to_owned()),
            tty: true,
            interactive: true,
            args: vec![
                String::from("/opt/spark/bin/beeline"),
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
            ..DockerRunArgs::default()
        });

        cmd.spawn()?.wait()?;

        Ok(())
    }

    pub fn run_two_in_one<S1, S2, StartedClb>(
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
        let docker_client = DockerClient::new();
        let mut spark =
            Self::run_server(workspace_layout, volume_layout, logger.clone(), None, None)?;

        {
            let _drop_spark = DropContainer::new(docker_client.clone(), "kamu-spark");

            started_clb();
            info!(logger, "Starting SQL shell");

            // Relying on shell to send signal to child processes
            let mut beeline_cmd = docker_client
                .exec_shell_cmd(
                    ExecArgs {
                        tty: true,
                        interactive: true,
                        work_dir: Some(PathBuf::from("/opt/spark/kamu_shell")),
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
            Self::run_shell(output_format, command, logger, url)
        } else {
            Self::run_two_in_one(
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
