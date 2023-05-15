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
use opendatafabric::*;
use rand::Rng;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

    fn workspace_dir_in_container(&self) -> PathBuf {
        PathBuf::from("/opt/engine/workspace")
    }

    fn in_out_dir_in_container(&self) -> PathBuf {
        PathBuf::from("/opt/engine/in-out")
    }

    fn to_container_path(&self, path: &Path) -> PathBuf {
        assert!(path.is_absolute());
        assert!(self.workspace_layout.root_dir.is_absolute());
        let rel = path.strip_prefix(&self.workspace_layout.root_dir).unwrap();
        let joined = self.workspace_dir_in_container().join(rel);
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
            tracing::info!(request = ?request, path = ?request_path, "Writing request");
            let file = File::create(&request_path)?;
            serde_yaml::to_writer(file, &request)
                .map_err(|e| EngineError::internal(e, Vec::new()))?;
        }

        let volume_map = vec![
            (run_info.in_out_dir.clone(), self.in_out_dir_in_container()),
            (
                self.workspace_layout.root_dir.clone(),
                self.workspace_dir_in_container(),
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
                        self.workspace_dir_in_container().display(),
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
                    "/opt/bitnami/spark/bin/spark-submit --master=local[4] --driver-memory=2g \
                     --class=dev.kamu.engine.spark.ingest.IngestApp \
                     /opt/engine/bin/engine.spark.jar"
                )
                .to_owned(),
                chown,
            ],
        );

        tracing::info!(command = ?cmd, "Running Spark job");

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

            tracing::info!(response = ?response, "Read response");

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

        let request_adj = IngestRequest {
            input_data_path: self.to_container_path(&request.input_data_path),
            // TODO: Not passing any checkpoint currently as Spark ingest doesn't use them
            prev_checkpoint_path: None,
            new_checkpoint_path: self.in_out_dir_in_container().join("new-checkpoint"),
            data_dir: self.to_container_path(&request.data_dir),
            output_data_path: self.to_container_path(&request.output_data_path),
            // TODO: We are stripping out the "fetch step because URL can contain templating
            // that will fail to parse in the engine.
            // In future engine should only receive the query part of the request.
            source: SetPollingSource {
                fetch: FetchStep::Url(FetchStepUrl {
                    url: "http://localhost/".to_owned(),
                    event_time: None,
                    cache: None,
                    headers: None,
                }),
                ..request.source
            },
            ..request
        };

        self.ingest_impl(run_info, request_adj).await
    }
}
