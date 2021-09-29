// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use crate::infra::*;

use container_runtime::*;
use opendatafabric::engine::ExecuteQueryError;
use opendatafabric::serde::yaml::YamlEngineProtocol;
use opendatafabric::serde::EngineProtocolDeserializer;
use opendatafabric::{ExecuteQueryResponse, ExecuteQueryResponseSuccess};
use rand::Rng;
use slog::{info, Logger};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct SparkEngine {
    container_runtime: ContainerRuntime,
    image: String,
    workspace_layout: Arc<WorkspaceLayout>,
    logger: Logger,
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
        logger: Logger,
    ) -> Self {
        Self {
            container_runtime: container_runtime,
            image: image.to_owned(),
            workspace_layout,
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
            RunArgs {
                image: self.image.clone(),
                volume_map: volume_map,
                user: Some("root".to_owned()),
                ..RunArgs::default()
            },
            &[
                format!(
                    "/opt/bitnami/spark/bin/spark-submit \
                        --master=local[4] \
                        --driver-memory=2g \
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
            .map_err(|e| EngineError::internal(e, run_info.log_files()))?;

        match status.code() {
            Some(code) if code == 0 => Ok(()),
            _ => Err(EngineError::process_error(
                status.code(),
                run_info.log_files(),
            )),
        }
    }

    fn write_request(&self, run_info: &RunInfo, request: IngestRequest) -> Result<(), EngineError> {
        let path = run_info.in_out_dir.join("request.yaml");
        info!(self.logger, "Writing request"; "request" => ?request, "path" => ?path);

        let file = File::create(&path)?;
        serde_yaml::to_writer(file, &request).map_err(|e| EngineError::internal(e, Vec::new()))?;

        Ok(())
    }

    fn read_response(
        &self,
        run_info: &RunInfo,
    ) -> Result<ExecuteQueryResponseSuccess, EngineError> {
        let path = run_info.in_out_dir.join("response.yaml");

        if !path.exists() {
            return Err(EngineError::contract_error(
                "Engine did not write a response file",
                run_info.log_files(),
            ));
        }

        let data = std::fs::read_to_string(path)?;
        let response = YamlEngineProtocol
            .read_execute_query_response(data.as_bytes())
            .map_err(|e| EngineError::internal(e, run_info.log_files()))?;

        info!(self.logger, "Read response"; "response" => ?response);

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
    }
}

impl IngestEngine for SparkEngine {
    fn ingest(&self, request: IngestRequest) -> Result<ExecuteQueryResponseSuccess, EngineError> {
        let run_info = RunInfo::new(&self.workspace_layout, "ingest");

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

        self.write_request(&run_info, request_adj)?;

        self.submit("dev.kamu.engine.spark.ingest.IngestApp", &run_info)?;

        self.read_response(&run_info)
    }
}
