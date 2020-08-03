use super::docker_client::*;
use crate::domain::*;
use crate::infra::serde::yaml::*;
use crate::infra::*;

use std::fs::File;
use std::path::{Path, PathBuf};

pub struct SparkEngine {
    image: String,
    workspace_layout: WorkspaceLayout,
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

    fn to_container_path(&self, path: &Path, volume_dir: &Path) -> PathBuf {
        assert!(path.is_absolute());
        assert!(volume_dir.is_absolute());
        let rel = path.strip_prefix(volume_dir).unwrap();
        self.volume_dir_in_container().join(rel)
    }

    fn submit(&self, app_class: &str, in_out_dir: &Path) -> Result<(), EngineError> {
        let docker = DockerClient::new();

        let volume_map = vec![
            (in_out_dir.to_owned(), self.in_out_dir_in_container()),
            (
                self.workspace_layout.local_volume_dir.clone(),
                self.volume_dir_in_container(),
            ),
        ];

        let status = docker
            .run_shell_cmd(
                DockerRunArgs {
                    image: self.image.clone(),
                    interactive: true,
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
            .stdout(std::process::Stdio::null()) // TODO: logging
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| EngineError::internal(e))?;

        match status.code() {
            Some(code) if code == 0 => Ok(()),
            _ => Err(ProcessError::new(status.code()).into()),
        }
    }

    fn write_ingest_request(&self, path: &Path, request: IngestRequest) -> Result<(), EngineError> {
        let file = File::create(path)?;

        serde_yaml::to_writer(
            file,
            &Manifest {
                api_version: 1,
                kind: "IngestRequest".to_owned(),
                content: request,
            },
        )
        .map_err(|e| EngineError::internal(e))?;

        Ok(())
    }

    fn read_ingest_response(&self, path: &Path) -> Result<IngestResponse, EngineError> {
        let file = File::open(path)?;

        let manifest: Manifest<IngestResponse> =
            serde_yaml::from_reader(file).map_err(|e| EngineError::internal(e))?;
        assert_eq!(manifest.kind, "IngestResult"); // Note naming difference

        Ok(manifest.content)
    }
}

impl Engine for SparkEngine {
    fn ingest(&self, request: IngestRequest) -> Result<IngestResponse, EngineError> {
        let in_out_dir = tempfile::tempdir()?;

        // Remove data_dir if it exists but empty as it will confuse Spark
        let _ = std::fs::remove_dir(&request.data_dir);

        let request_adj = IngestRequest {
            ingest_path: self.to_container_path(
                &request.ingest_path,
                &self.workspace_layout.local_volume_dir,
            ),
            data_dir: self
                .to_container_path(&request.data_dir, &self.workspace_layout.local_volume_dir),
            ..request
        };

        self.write_ingest_request(&in_out_dir.path().join("request.yaml"), request_adj)?;

        self.submit("dev.kamu.engine.spark.ingest.IngestApp", in_out_dir.path())?;

        self.read_ingest_response(&in_out_dir.path().join("result.yaml"))
    }
}
