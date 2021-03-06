use crate::domain::*;
use crate::infra::utils::docker_client::*;
use crate::infra::*;

use rand::Rng;
use slog::{info, Logger};
use std::fs::File;
use std::path::{Path, PathBuf};

pub struct SparkEngine {
    container_runtime: DockerClient,
    image: String,
    workspace_layout: WorkspaceLayout,
    logger: Logger,
}

struct RunInfo {
    in_out_dir: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl RunInfo {
    fn new(in_out_dir: &Path, workspace_layout: &WorkspaceLayout) -> Self {
        let run_id: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();

        Self {
            in_out_dir: in_out_dir.to_owned(),
            stdout_path: workspace_layout
                .run_info_dir
                .join(format!("spark-{}.out.txt", run_id)),
            stderr_path: workspace_layout
                .run_info_dir
                .join(format!("spark-{}.err.txt", run_id)),
        }
    }
}

impl SparkEngine {
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

    fn submit(&self, app_class: &str, run_info: &RunInfo) -> Result<(), EngineError> {
        let volume_map = vec![
            (run_info.in_out_dir.clone(), self.in_out_dir_in_container()),
            (
                self.workspace_layout.local_volume_dir.clone(),
                self.volume_dir_in_container(),
            ),
        ];

        let stdout_file = std::fs::File::create(&run_info.stdout_path)?;
        let stderr_file = std::fs::File::create(&run_info.stderr_path)?;

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

        let mut cmd = self.container_runtime.run_shell_cmd(
            DockerRunArgs {
                image: self.image.clone(),
                volume_map: volume_map,
                user: Some("root".to_owned()),
                ..DockerRunArgs::default()
            },
            &[
                format!(
                    "/opt/spark/bin/spark-submit \
                        --master=local[4] \
                        --driver-memory=2g \
                        --conf spark.jars.ivy=/tmp/.ivy \
                        --conf spark.sql.warehouse.dir=/opt/spark-warehouse \
                        --class={} \
                        /opt/engine/bin/engine.spark.jar",
                    app_class,
                ),
                chown,
            ],
        );

        info!(self.logger, "Running Spark job"; "command" => ?cmd);

        let status = cmd
            .stdout(std::process::Stdio::from(stdout_file))
            .stderr(std::process::Stdio::from(stderr_file))
            .status()
            .map_err(|e| EngineError::internal(e))?;

        match status.code() {
            Some(code) if code == 0 => Ok(()),
            _ => Err(ProcessError::new(
                status.code(),
                vec![run_info.stdout_path.clone(), run_info.stderr_path.clone()],
            )
            .into()),
        }
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
                vec![run_info.stdout_path.clone(), run_info.stderr_path.clone()],
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

impl Engine for SparkEngine {
    fn ingest(&self, request: IngestRequest) -> Result<IngestResponse, EngineError> {
        let in_out_dir = tempfile::Builder::new().prefix("kamu-ingest-").tempdir()?;
        let run_info = RunInfo::new(in_out_dir.path(), &self.workspace_layout);

        // Remove data_dir if it exists but empty as it will confuse Spark
        let _ = std::fs::remove_dir(&request.data_dir);

        let request_adj = IngestRequest {
            ingest_path: self.to_container_path(&request.ingest_path),
            prev_checkpoint_dir: request
                .prev_checkpoint_dir
                .map(|p| self.to_container_path(&p)),
            new_checkpoint_dir: self.to_container_path(&request.new_checkpoint_dir),
            data_dir: self.to_container_path(&request.data_dir),
            out_data_path: self.to_container_path(&request.out_data_path),
            ..request
        };

        self.write_request(&run_info, request_adj, "IngestRequest")?;

        self.submit("dev.kamu.engine.spark.ingest.IngestApp", &run_info)?;

        self.read_response(&run_info, "IngestResult")
    }

    fn transform(&self, request: ExecuteQueryRequest) -> Result<ExecuteQueryResponse, EngineError> {
        let in_out_dir = tempfile::Builder::new()
            .prefix("kamu-transform-")
            .tempdir()?;

        let run_info = RunInfo::new(in_out_dir.path(), &self.workspace_layout);

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

        self.submit("dev.kamu.engine.spark.transform.TransformApp", &run_info)?;

        self.read_response(&run_info, "ExecuteQueryResult")
    }
}
