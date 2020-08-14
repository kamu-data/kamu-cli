use crate::domain::*;
use crate::infra::serde::yaml::*;
use crate::infra::utils::docker_client::*;
use crate::infra::*;

use rand::Rng;
use std::fs::File;
use std::path::{Path, PathBuf};

pub struct SparkEngine {
    image: String,
    workspace_layout: WorkspaceLayout,
}

struct RunInfo {
    in_out_dir: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl RunInfo {
    fn new(in_out_dir: &Path, workspace_layout: &WorkspaceLayout) -> Self {
        let mut run_id = String::with_capacity(10);
        run_id.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(10),
        );

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
    pub fn new(image: &str, workspace_layout: &WorkspaceLayout) -> Self {
        Self {
            image: image.to_owned(),
            workspace_layout: workspace_layout.clone(),
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
        self.volume_dir_in_container().join(rel)
    }

    fn submit(&self, app_class: &str, run_info: &RunInfo) -> Result<(), EngineError> {
        let docker = DockerClient::new();

        let volume_map = vec![
            (run_info.in_out_dir.clone(), self.in_out_dir_in_container()),
            (
                self.workspace_layout.local_volume_dir.clone(),
                self.volume_dir_in_container(),
            ),
        ];

        let stdout_file = std::fs::File::create(&run_info.stdout_path)?;
        let stderr_file = std::fs::File::create(&run_info.stderr_path)?;

        let status = docker
            .run_shell_cmd(
                DockerRunArgs {
                    image: self.image.clone(),
                    volume_map: volume_map,
                    ..DockerRunArgs::default()
                },
                &[
                    format!(
                        "/opt/spark/bin/spark-submit \
                        --master=local[4] \
                        --driver-memory=2g \
                        --conf spark.sql.warehouse.dir=/opt/spark-warehouse \
                        --class={} \
                        /opt/engine/bin/engine.spark.jar",
                        app_class,
                    ),
                    format!(
                        "; chown -R {}:{} {}",
                        users::get_current_uid(),
                        users::get_current_gid(),
                        self.volume_dir_in_container().display()
                    ),
                ],
            )
            .stdout(std::process::Stdio::from(stdout_file))
            .stderr(std::process::Stdio::from(stderr_file))
            .status()
            .map_err(|e| EngineError::internal(e))?;

        match status.code() {
            Some(code) if code == 0 => Ok(()),
            _ => Err(ProcessError::new(
                status.code(),
                Some(run_info.stdout_path.clone()),
                Some(run_info.stderr_path.clone()),
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
        T: ::serde::ser::Serialize,
    {
        let path = run_info.in_out_dir.join("request.yaml");

        let file = File::create(&path)?;

        serde_yaml::to_writer(
            file,
            &Manifest {
                api_version: 1,
                kind: resource_name.to_owned(),
                content: request,
            },
        )
        .map_err(|e| EngineError::internal(e))?;

        Ok(())
    }

    fn read_response<T>(&self, run_info: &RunInfo, resource_name: &str) -> Result<T, EngineError>
    where
        T: ::serde::de::DeserializeOwned,
    {
        let path = run_info.in_out_dir.join("result.yaml");

        if !path.exists() {
            return Err(ContractError::new(
                "Engine did not write a response file",
                Some(run_info.stdout_path.clone()),
                Some(run_info.stderr_path.clone()),
            )
            .into());
        }

        let file = File::open(path)?;

        let manifest: Manifest<T> =
            serde_yaml::from_reader(file).map_err(|e| EngineError::internal(e))?;
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
            data_dir: self.to_container_path(&request.data_dir),
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

        // Remove data_dir if it exists but empty as it will confuse Spark
        let output_dir = request.data_dirs.get(&request.dataset_id).unwrap();
        let _ = std::fs::remove_dir(&output_dir);

        let request_adj = ExecuteQueryRequest {
            data_dirs: request
                .data_dirs
                .into_iter()
                .map(|(id, path)| (id, self.to_container_path(&path)))
                .collect(),
            checkpoints_dir: self.to_container_path(&request.checkpoints_dir),
            ..request
        };

        self.write_request(&run_info, request_adj, "ExecuteQueryRequest")?;

        self.submit("dev.kamu.engine.spark.transform.TransformApp", &run_info)?;

        self.read_response(&run_info, "ExecuteQueryResult")
    }
}
