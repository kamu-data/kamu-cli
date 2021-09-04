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
    container_runtime: DockerClient,
    image: String,
    workspace_layout: WorkspaceLayout,
    logger: Logger,
}

struct RunInfo {
    run_id: String,
    in_out_dir: PathBuf,
    prev_checkpoint_dir: Option<PathBuf>,
    job_manager_stdout_path: PathBuf,
    job_manager_stderr_path: PathBuf,
    task_manager_stdout_path: PathBuf,
    task_manager_stderr_path: PathBuf,
    submit_stdout_path: PathBuf,
    submit_stderr_path: PathBuf,
}

impl RunInfo {
    fn new(
        run_id: String,
        in_out_dir: PathBuf,
        prev_checkpoint_dir: Option<PathBuf>,
        logs_dir: &Path,
    ) -> Self {
        let job_manager_stdout_path =
            logs_dir.join(format!("flink-jobmanager-{}.out.txt", &run_id));
        let job_manager_stderr_path =
            logs_dir.join(format!("flink-jobmanager-{}.err.txt", &run_id));
        let task_manager_stdout_path =
            logs_dir.join(format!("flink-taskmanager-{}.out.txt", &run_id));
        let task_manager_stderr_path =
            logs_dir.join(format!("flink-taskmanager-{}.err.txt", &run_id));
        let submit_stdout_path = logs_dir.join(format!("flink-submit-{}.out.txt", &run_id));
        let submit_stderr_path = logs_dir.join(format!("flink-submit-{}.err.txt", &run_id));

        Self {
            run_id: run_id,
            in_out_dir: in_out_dir,
            prev_checkpoint_dir: prev_checkpoint_dir,
            job_manager_stdout_path: job_manager_stdout_path,
            job_manager_stderr_path: job_manager_stderr_path,
            task_manager_stdout_path: task_manager_stdout_path,
            task_manager_stderr_path: task_manager_stderr_path,
            submit_stdout_path: submit_stdout_path,
            submit_stderr_path: submit_stderr_path,
        }
    }

    fn get_log_files(&self) -> Vec<PathBuf> {
        vec![
            self.submit_stdout_path.clone(),
            self.submit_stderr_path.clone(),
            self.task_manager_stdout_path.clone(),
            self.task_manager_stderr_path.clone(),
            self.job_manager_stdout_path.clone(),
            self.job_manager_stderr_path.clone(),
        ]
    }
}

impl FlinkEngine {
    pub fn new(
        container_runtime: DockerClient,
        image: &str,
        workspace_layout: &WorkspaceLayout,
        logger: Logger,
    ) -> Self {
        Self {
            container_runtime: container_runtime,
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

    fn submit(&self, run_info: &RunInfo) -> Result<(), EngineError> {
        // TODO: For podman-in-docker scenarios we cannot use `network` and `hostname` parameter
        // perhaps in future we can hide the difference in ContainerRuntime itself
        let is_localns = self.container_runtime.config.network_ns == NetworkNamespaceType::Host;

        let (network_name, _network) = if !is_localns {
            let network_name = format!("kamu-flink-{}", &run_info.run_id);
            let network = self.container_runtime.create_network(&network_name);
            (Some(network_name), Some(network))
        } else {
            (None, None)
        };

        let job_manager = Container::new(
            self.container_runtime.clone(),
            DockerRunArgs {
                image: self.image.clone(),
                network: network_name.clone(),
                container_name: Some("jobmanager".to_owned()),
                hostname: if !is_localns {
                    Some("jobmanager".to_owned())
                } else {
                    None
                },
                args: vec!["jobmanager".to_owned()],
                environment_vars: vec![(
                    "JOB_MANAGER_RPC_ADDRESS".to_owned(),
                    if !is_localns {
                        "jobmanager"
                    } else {
                        "localhost"
                    }
                    .to_owned(),
                )],
                expose_ports: vec![6123, 8081],
                volume_map: vec![
                    (run_info.in_out_dir.clone(), self.in_out_dir_in_container()),
                    (
                        self.workspace_layout.local_volume_dir.clone(),
                        self.volume_dir_in_container(),
                    ),
                ],
                ..DockerRunArgs::default()
            },
            &run_info.job_manager_stdout_path,
            &run_info.job_manager_stderr_path,
            self.logger.new(o!("name" => "jobmanager")),
        )?;

        let _task_manager = Container::new(
            self.container_runtime.clone(),
            DockerRunArgs {
                image: self.image.clone(),
                network: network_name.clone(),
                container_name: Some("taskmanager".to_owned()),
                hostname: if !is_localns {
                    Some("taskmanager".to_owned())
                } else {
                    None
                },
                args: vec!["taskmanager".to_owned()],
                environment_vars: vec![(
                    "JOB_MANAGER_RPC_ADDRESS".to_owned(),
                    if !is_localns {
                        "jobmanager"
                    } else {
                        "localhost"
                    }
                    .to_owned(),
                )],
                expose_ports: vec![6121, 6122],
                volume_map: vec![(
                    self.workspace_layout.local_volume_dir.clone(),
                    self.volume_dir_in_container(),
                )],
                ..DockerRunArgs::default()
            },
            &run_info.task_manager_stdout_path,
            &run_info.task_manager_stderr_path,
            self.logger.new(o!("name" => "taskmanager")),
        )?;

        self.container_runtime
            .wait_for_host_port(job_manager.name(), 8081, Duration::from_secs(15))
            .map_err(|e| EngineError::internal(e))?;

        let savepoint_args = run_info
            .prev_checkpoint_dir
            .as_ref()
            .map(|p| self.get_savepoint(p).unwrap())
            .map(|p| self.to_container_path(&p))
            .map(|p| format!("-s {}", p.display()))
            .unwrap_or_default();

        // TODO: chown hides exit status
        cfg_if::cfg_if! {
            if #[cfg(unix)] {
                let chown = if self.container_runtime.config.runtime == ContainerRuntimeType::Docker {
                    format!(
                        "; chown -R {}:{} {}",
                        users::get_current_uid(),
                        users::get_current_gid(),
                        self.volume_dir_in_container().display()
                    )
                } else {
                    "".to_owned()
                };
            } else {
                let chown = "".to_owned();
            }
        };

        let mut run_cmd = self.container_runtime.exec_shell_cmd(
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
            .stdout(Stdio::from(File::create(&run_info.submit_stdout_path)?))
            .stderr(Stdio::from(File::create(&run_info.submit_stderr_path)?))
            .status()?;

        match run_status.code() {
            Some(code) if code == 0 => Ok(()),
            _ => Err(ProcessError::new(run_status.code(), run_info.get_log_files()).into()),
        }
    }

    // TODO: Atomicity
    // TODO: Error handling
    fn get_savepoint(&self, checkpoint_dir: &Path) -> Result<PathBuf, std::io::Error> {
        let mut savepoints = std::fs::read_dir(checkpoint_dir)?
            .filter_map(|res| match res {
                Ok(e) => {
                    let path = e.path();
                    let name = path.file_name().unwrap().to_string_lossy();
                    if path.is_dir() && name.starts_with("savepoint-") {
                        Some(Ok(path))
                    } else {
                        None
                    }
                }
                Err(err) => Some(Err(err)),
            })
            .collect::<Result<Vec<_>, std::io::Error>>()?;

        if savepoints.len() != 1 {
            panic!(
                "Could not find a savepoint in checkpoint location: {}",
                checkpoint_dir.display()
            )
        }

        Ok(savepoints.pop().unwrap())
    }

    fn write_request<T>(
        &self,
        run_info: &RunInfo,
        request: T,
        resource_name: &str,
    ) -> Result<(), EngineError>
    where
        T: ::serde::ser::Serialize + std::fmt::Debug,
    {
        let path = run_info.in_out_dir.join("request.yaml");

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

    fn read_response<T>(&self, run_info: &RunInfo, resource_name: &str) -> Result<T, EngineError>
    where
        T: ::serde::de::DeserializeOwned + std::fmt::Debug,
    {
        let path = run_info.in_out_dir.join("result.yaml");

        if !path.exists() {
            return Err(ContractError::new(
                "Engine did not write a response file",
                run_info.get_log_files(),
            )
            .into());
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
        let run_id: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();

        let in_out_dir = self
            .workspace_layout
            .run_info_dir
            .join(format!("transform-{}", &run_id));

        std::fs::create_dir_all(&in_out_dir)?;

        let run_info = RunInfo::new(
            run_id,
            in_out_dir.to_owned(),
            request.prev_checkpoint_dir.clone(),
            &self.workspace_layout.run_info_dir,
        );

        let input_slices_adj = request
            .input_slices
            .into_iter()
            .map(|(id, slice)| {
                (
                    id,
                    InputDataSlice {
                        data_paths: slice
                            .data_paths
                            .iter()
                            .map(|p| self.to_container_path(p))
                            .collect(),
                        schema_file: self.to_container_path(&slice.schema_file),
                        ..slice
                    },
                )
            })
            .collect();

        let request_adj = ExecuteQueryRequest {
            input_slices: input_slices_adj,
            prev_checkpoint_dir: request
                .prev_checkpoint_dir
                .map(|p| self.to_container_path(&p)),
            new_checkpoint_dir: self.to_container_path(&request.new_checkpoint_dir),
            out_data_path: self.to_container_path(&request.out_data_path),
            ..request
        };

        self.write_request(&run_info, request_adj, "ExecuteQueryRequest")?;

        self.submit(&run_info)?;

        self.read_response(&run_info, "ExecuteQueryResult")
    }
}

struct Container {
    container_runtime: DockerClient,
    name: String,
    child: Child,
    logger: Logger,
}

impl Container {
    fn new(
        container_runtime: DockerClient,
        args: DockerRunArgs,
        stdout_path: &Path,
        stderr_path: &Path,
        logger: Logger,
    ) -> Result<Self, std::io::Error> {
        let name = args.container_name.as_ref().unwrap().clone();

        let mut cmd = container_runtime.run_cmd(args);

        info!(logger, "Spawning container"; "command" => ?cmd);

        let child = cmd
            .stdout(Stdio::from(File::create(stdout_path)?))
            .stderr(Stdio::from(File::create(stderr_path)?))
            .spawn()?;

        Ok(Self {
            container_runtime: container_runtime,
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
        self.container_runtime
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
