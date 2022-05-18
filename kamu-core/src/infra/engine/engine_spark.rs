// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::engine::IngestRequest;
use crate::domain::*;
use crate::infra::*;

use container_runtime::*;
use opendatafabric::engine::ExecuteQueryError;
use opendatafabric::serde::yaml::YamlEngineProtocol;
use opendatafabric::serde::EngineProtocolDeserializer;
use opendatafabric::{ExecuteQueryResponse, ExecuteQueryResponseSuccess};
use rand::Rng;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

pub struct SparkEngine {
    container_runtime: ContainerRuntime,
    image: String,
    workspace_layout: Arc<WorkspaceLayout>,
}

struct RunInfo {
    in_out_dir: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl RunInfo {
    fn new(workspace_layout: &WorkspaceLayout, operation: &str) -> Self {
        let run_id: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();

        let in_out_dir = workspace_layout
            .run_info_dir
            .join(format!("{}-{}", operation, &run_id));

        std::fs::create_dir_all(&in_out_dir).expect("Failed to create in-out directory");

        Self {
            in_out_dir: in_out_dir,
            stdout_path: workspace_layout
                .run_info_dir
                .join(format!("spark-{}.out.txt", run_id)),
            stderr_path: workspace_layout
                .run_info_dir
                .join(format!("spark-{}.err.txt", run_id)),
        }
    }

    pub fn log_files(&self) -> Vec<PathBuf> {
        vec![self.stdout_path.clone(), self.stderr_path.clone()]
    }
}

impl SparkEngine {
    pub fn new(
        container_runtime: ContainerRuntime,
        image: &str,
        workspace_layout: Arc<WorkspaceLayout>,
    ) -> Self {
        Self {
            container_runtime: container_runtime,
            image: image.to_owned(),
            workspace_layout,
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
        assert!(self.workspace_layout.datasets_dir.is_absolute());
        let rel = path
            .strip_prefix(&self.workspace_layout.datasets_dir)
            .unwrap();
        let joined = self.volume_dir_in_container().join(rel);
        let unix_path = joined.to_str().unwrap().replace("\\", "/");
        PathBuf::from(unix_path)
    }

    async fn ingest_impl(
        &self,
        run_info: RunInfo,
        request: IngestRequest,
    ) -> Result<ExecuteQueryResponseSuccess, EngineError> {
        let request_path = run_info.in_out_dir.join("request.yaml");
        let response_path = run_info.in_out_dir.join("response.yaml");

        {
            info!(request = ?request, path = ?request_path, "Writing request");
            let file = File::create(&request_path)?;
            serde_yaml::to_writer(file, &request)
                .map_err(|e| EngineError::internal(e, Vec::new()))?;
        }

        let volume_map = vec![
            (run_info.in_out_dir.clone(), self.in_out_dir_in_container()),
            (
                self.workspace_layout.datasets_dir.clone(),
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
                        "; chown -R {}:{} {} {}",
                        users::get_current_uid(),
                        users::get_current_gid(),
                        self.volume_dir_in_container().display(),
                        self.in_out_dir_in_container().display()
                    )
                } else {
                    "".to_owned()
                };
            } else {
                let chown = "".to_owned();
            }
        };

        let mut cmd = self.container_runtime.run_shell_cmd(
            RunArgs {
                image: self.image.clone(),
                volume_map: volume_map,
                user: Some("root".to_owned()),
                ..RunArgs::default()
            },
            &[
                indoc::indoc!(
                    "/opt/bitnami/spark/bin/spark-submit \
                    --master=local[4] \
                    --driver-memory=2g \
                    --class=dev.kamu.engine.spark.ingest.IngestApp \
                    /opt/engine/bin/engine.spark.jar"
                )
                .to_owned(),
                chown,
            ],
        );

        info!(command = ?cmd, "Running Spark job");

        let status = tokio::task::spawn_blocking(move || {
            cmd.stdout(std::process::Stdio::from(stdout_file))
                .stderr(std::process::Stdio::from(stderr_file))
                .status()
        })
        .await
        .map_err(|e| EngineError::internal(e, run_info.log_files()))?
        .map_err(|e| EngineError::internal(e, run_info.log_files()))?;

        if response_path.exists() {
            let data = std::fs::read_to_string(&response_path)?;
            let response = YamlEngineProtocol
                .read_execute_query_response(data.as_bytes())
                .map_err(|e| EngineError::internal(e, run_info.log_files()))?;

            info!(response = ?response, "Read response");

            match response {
                ExecuteQueryResponse::Progress => unreachable!(),
                ExecuteQueryResponse::Success(s) => Ok(s),
                ExecuteQueryResponse::InvalidQuery(e) => {
                    Err(EngineError::invalid_query(e.message, run_info.log_files()))
                }
                ExecuteQueryResponse::InternalError(e) => Err(EngineError::internal(
                    ExecuteQueryError::from(e),
                    run_info.log_files(),
                )),
            }
        } else if !status.success() {
            Err(EngineError::process_error(
                status.code(),
                run_info.log_files(),
            ))
        } else {
            Err(EngineError::contract_error(
                "Engine did not write a response file",
                run_info.log_files(),
            ))
        }
    }

    fn unpack_checkpoint(
        prev_checkpoint_path: &Path,
        target_dir: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!(
            ?prev_checkpoint_path,
            ?target_dir,
            "Unpacking previous checkpoint"
        );
        std::fs::create_dir(target_dir)?;
        let mut archive = tar::Archive::new(std::fs::File::open(prev_checkpoint_path)?);
        archive.unpack(target_dir)?;
        Ok(())
    }

    fn pack_checkpoint(
        source_dir: &Path,
        new_checkpoint_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!(?source_dir, ?new_checkpoint_path, "Packing new checkpoint");
        let mut ar = tar::Builder::new(std::fs::File::create(new_checkpoint_path)?);
        ar.follow_symlinks(false);
        ar.append_dir_all(".", source_dir)?;
        ar.finish()?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl IngestEngine for SparkEngine {
    async fn ingest(
        &self,
        request: IngestRequest,
    ) -> Result<ExecuteQueryResponseSuccess, EngineError> {
        let run_info = RunInfo::new(&self.workspace_layout, "ingest");

        // Remove data_dir if it exists but empty as it will confuse Spark
        let _ = std::fs::remove_dir(&request.data_dir);

        // Checkpoints are required to be files but for now Spark engine uses directories
        // So we untar old checkpoints before processing and pack new ones after
        let new_checkpoint_path_unpacked = run_info.in_out_dir.join("new-checkpoint");
        let prev_checkpoint_path_unpacked = if let Some(p) = &request.prev_checkpoint_path {
            let target_dir = run_info.in_out_dir.join("prev-checkpoint");
            Self::unpack_checkpoint(p, &target_dir).expect("Failed to untar previous checkpoint");
            Some(target_dir)
        } else {
            None
        };

        let request_adj = IngestRequest {
            ingest_path: self.to_container_path(&request.ingest_path),
            prev_checkpoint_path: prev_checkpoint_path_unpacked
                .as_ref()
                .map(|_| self.in_out_dir_in_container().join("prev-checkpoint")),
            new_checkpoint_path: self.in_out_dir_in_container().join("new-checkpoint"),
            data_dir: self.to_container_path(&request.data_dir),
            out_data_path: self.to_container_path(&request.out_data_path),
            ..request
        };

        let response = self.ingest_impl(run_info, request_adj).await;

        if response.is_ok() {
            // Pack checkpoints and clean up
            if new_checkpoint_path_unpacked.exists()
                && new_checkpoint_path_unpacked
                    .read_dir()
                    .unwrap()
                    .next()
                    .is_some()
            {
                Self::pack_checkpoint(&new_checkpoint_path_unpacked, &request.new_checkpoint_path)
                    .expect("Failed to tar new checkpoint");
                std::fs::remove_dir_all(new_checkpoint_path_unpacked)?;
            }
            if let Some(prev) = prev_checkpoint_path_unpacked {
                std::fs::remove_dir_all(prev)?;
            }
        }

        response
    }
}
