use crate::domain::*;
use crate::infra::utils::docker_client::*;
use crate::infra::*;

use rand::Rng;
use slog::{info, o, Logger};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::time::Duration;

pub struct FlinkEngine {
    image: String,
    workspace_layout: WorkspaceLayout,
    logger: Logger,
}

impl FlinkEngine {
    pub fn new(image: &str, workspace_layout: &WorkspaceLayout, logger: Logger) -> Self {
        Self {
            image: image.to_owned(),
            workspace_layout: workspace_layout.clone(),
            logger: logger,
        }
    }

    fn volume_dir_in_container(&self) -> PathBuf {
        PathBuf::from("/opt/engine/volume")
    }

    fn in_out_dir_in_container(&self) -> PathBuf {
        PathBuf::from("/opt/engine/in-out")
    }

    fn to_container_path(&self, path: &Path) -> PathBuf {
        assert!(path.is_absolute());
        assert!(self.workspace_layout.local_volume_dir.is_absolute());
        let rel = path
            .strip_prefix(&self.workspace_layout.local_volume_dir)
            .unwrap();
        let joined = self.volume_dir_in_container().join(rel);
        let unix_path = joined.to_str().unwrap().replace("\\", "/");
        PathBuf::from(unix_path)
    }

    fn submit(
        &self,
        run_id: &str,
        in_out_dir: &Path,
        checkpoints_dir: &Path,
    ) -> Result<(), EngineError> {
        let docker = DockerClient::new();

        let network_name = format!("kamu-flink-{}", run_id);
        let _network = docker.create_network(&network_name);

        let job_manager_stdout_path = self
            .workspace_layout
            .run_info_dir
            .join(format!("flink-jobmanager-{}.out.txt", run_id));
        let job_manager_stderr_path = self
            .workspace_layout
            .run_info_dir
            .join(format!("flink-jobmanager-{}.err.txt", run_id));

        let task_manager_stdout_path = self
            .workspace_layout
            .run_info_dir
            .join(format!("flink-taskmanager-{}.out.txt", run_id));
        let task_manager_stderr_path = self
            .workspace_layout
            .run_info_dir
            .join(format!("flink-taskmanager-{}.err.txt", run_id));

        let submit_stdout_path = self
            .workspace_layout
            .run_info_dir
            .join(format!("flink-submit-{}.out.txt", run_id));
        let submit_stderr_path = self
            .workspace_layout
            .run_info_dir
            .join(format!("flink-submit-{}.err.txt", run_id));

        let job_manager = Container::new(
            DockerRunArgs {
                image: self.image.clone(),
                network: Some(network_name.clone()),
                container_name: Some("jobmanager".to_owned()),
                hostname: Some("jobmanager".to_owned()),
                args: vec!["jobmanager".to_owned()],
                environment_vars: vec![(
                    "JOB_MANAGER_RPC_ADDRESS".to_owned(),
                    "jobmanager".to_owned(),
                )],
                expose_ports: vec![6123, 8081],
                volume_map: vec![
                    (in_out_dir.to_owned(), self.in_out_dir_in_container()),
                    (
                        self.workspace_layout.local_volume_dir.clone(),
                        self.volume_dir_in_container(),
                    ),
                ],
                ..DockerRunArgs::default()
            },
            &job_manager_stdout_path,
            &job_manager_stderr_path,
            self.logger.new(o!("name" => "jobmanager")),
        )?;

        let _task_manager = Container::new(
            DockerRunArgs {
                image: self.image.clone(),
                network: Some(network_name.clone()),
                container_name: Some("taskmanager".to_owned()),
                hostname: Some("taskmanager".to_owned()),
                args: vec!["taskmanager".to_owned()],
                environment_vars: vec![(
                    "JOB_MANAGER_RPC_ADDRESS".to_owned(),
                    "jobmanager".to_owned(),
                )],
                expose_ports: vec![6121, 6122],
                volume_map: vec![(
                    self.workspace_layout.local_volume_dir.clone(),
                    self.volume_dir_in_container(),
                )],
                ..DockerRunArgs::default()
            },
            &task_manager_stdout_path,
            &task_manager_stderr_path,
            self.logger.new(o!("name" => "taskmanager")),
        )?;

        docker
            .wait_for_host_port(job_manager.name(), 8081, Duration::from_secs(15))
            .map_err(|e| EngineError::internal(e))?;

        let prev_savepoint = self.get_prev_savepoint(checkpoints_dir)?;
        let savepoint_args = prev_savepoint
            .as_ref()
            .map(|p| self.to_container_path(&p))
            .map(|p| format!("-s {}", p.display()))
            .unwrap_or_default();

        cfg_if::cfg_if! {
            if #[cfg(unix)] {
                let chown = format!(
                    "; chown -R {}:{} {}",
                    users::get_current_uid(),
                    users::get_current_gid(),
                    self.volume_dir_in_container().display()
                );
            } else {
                let chown = "".to_owned();
            }
        };

        let mut run_cmd = docker.exec_shell_cmd(
            ExecArgs::default(),
            job_manager.name(),
            &[
                "flink".to_owned(),
                "run".to_owned(),
                savepoint_args,
                "/opt/engine/bin/engine.flink.jar".to_owned(),
                chown,
            ],
        );

        info!(self.logger, "Running Flink job"; "command" => ?run_cmd);

        let run_status = run_cmd
            .stdout(Stdio::from(File::create(&submit_stdout_path)?))
            .stderr(Stdio::from(File::create(&submit_stderr_path)?))
            .status()?;

        if run_status.success() {
            self.commit_savepoint(prev_savepoint)?;
        }

        match run_status.code() {
            Some(code) if code == 0 => Ok(()),
            _ => Err(ProcessError::new(
                run_status.code(),
                Some(submit_stdout_path),
                Some(submit_stderr_path),
            )
            .into()),
        }
    }

    // TODO: Atomicity
    fn get_prev_savepoint(
        &self,
        checkpoints_dir: &Path,
    ) -> Result<Option<PathBuf>, std::io::Error> {
        if !checkpoints_dir.exists() {
            return Ok(None);
        }

        let mut checkpoints = std::fs::read_dir(checkpoints_dir)?
            .filter_map(|res| match res {
                Ok(e) => {
                    let path = e.path();
                    if path.is_dir() {
                        Some(Ok(path))
                    } else {
                        None
                    }
                }
                Err(err) => Some(Err(err)),
            })
            .collect::<Result<Vec<_>, std::io::Error>>()?;

        if checkpoints.len() > 1 {
            panic!("Multiple checkpoints found: {:?}", checkpoints)
        }

        Ok(checkpoints.pop())
    }

    // TODO: Atomicity
    fn commit_savepoint(&self, old_savepoint: Option<PathBuf>) -> Result<(), std::io::Error> {
        if let Some(path) = old_savepoint {
            std::fs::remove_dir_all(path)?
        }
        Ok(())
    }

    fn write_request<T>(
        &self,
        in_out_dir: &Path,
        request: T,
        resource_name: &str,
    ) -> Result<(), EngineError>
    where
        T: ::serde::ser::Serialize + std::fmt::Debug,
    {
        let path = in_out_dir.join("request.yaml");

        let manifest = Manifest {
            api_version: 1,
            kind: resource_name.to_owned(),
            content: request,
        };
        info!(self.logger, "Writing request"; "request" => ?manifest, "path" => ?path);

        let file = File::create(&path)?;
        serde_yaml::to_writer(file, &manifest).map_err(|e| EngineError::internal(e))?;

        Ok(())
    }

    fn read_response<T>(&self, in_out_dir: &Path, resource_name: &str) -> Result<T, EngineError>
    where
        T: ::serde::de::DeserializeOwned + std::fmt::Debug,
    {
        let path = in_out_dir.join("result.yaml");

        if !path.exists() {
            return Err(
                ContractError::new("Engine did not write a response file", None, None).into(),
            );
        }

        let file = File::open(path)?;

        let manifest: Manifest<T> =
            serde_yaml::from_reader(file).map_err(|e| EngineError::internal(e))?;

        info!(self.logger, "Read response"; "response" => ?manifest);
        assert_eq!(manifest.kind, resource_name);

        Ok(manifest.content)
    }
}

impl Engine for FlinkEngine {
    fn ingest(&self, _request: IngestRequest) -> Result<IngestResponse, EngineError> {
        unimplemented!();
    }

    fn transform(&self, request: ExecuteQueryRequest) -> Result<ExecuteQueryResponse, EngineError> {
        let in_out_dir = tempfile::Builder::new()
            .prefix("kamu-transform-")
            .tempdir()?;

        let mut run_id = String::with_capacity(10);
        run_id.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(10),
        );

        // Remove data_dir if it exists but empty as it will confuse Spark
        let output_dir = request.data_dirs.get(&request.dataset_id).unwrap();
        let _ = std::fs::remove_dir(&output_dir);
        let checkpoints_dir = request.checkpoints_dir.clone();

        let request_adj = ExecuteQueryRequest {
            data_dirs: request
                .data_dirs
                .into_iter()
                .map(|(id, path)| (id, self.to_container_path(&path)))
                .collect(),
            checkpoints_dir: self.to_container_path(&request.checkpoints_dir),
            ..request
        };

        self.write_request(in_out_dir.path(), request_adj, "ExecuteQueryRequest")?;

        self.submit(&run_id, in_out_dir.path(), &checkpoints_dir)?;

        self.read_response(in_out_dir.path(), "ExecuteQueryResult")
    }
}

struct Container {
    docker: DockerClient,
    name: String,
    child: Child,
    logger: Logger,
}

impl Container {
    fn new(
        args: DockerRunArgs,
        stdout_path: &Path,
        stderr_path: &Path,
        logger: Logger,
    ) -> Result<Self, std::io::Error> {
        let docker = DockerClient::new();
        let name = args.container_name.as_ref().unwrap().clone();

        let mut cmd = docker.run_cmd(args);

        info!(logger, "Spawning container"; "command" => ?cmd);

        let child = cmd
            .stdout(Stdio::from(File::create(stdout_path)?))
            .stderr(Stdio::from(File::create(stderr_path)?))
            .spawn()?;

        Ok(Self {
            docker: docker,
            name: name,
            child: child,
            logger: logger,
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kill(&mut self) -> Result<(), std::io::Error> {
        info!(self.logger, "Sending kill to container");
        self.docker
            .kill_cmd(&self.name)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        Ok(())
    }

    fn join(&mut self) -> Result<std::process::ExitStatus, std::io::Error> {
        info!(self.logger, "Waiting for container to stop");
        let res = self.child.wait();
        info!(self.logger, "Container stopped");
        res
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = self.kill();
        let _ = self.join();
    }
}
