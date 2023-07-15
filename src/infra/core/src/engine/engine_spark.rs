// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use container_runtime::*;
use kamu_core::*;
use opendatafabric::engine::ExecuteQueryError;
use opendatafabric::serde::yaml::YamlEngineProtocol;
use opendatafabric::serde::EngineProtocolDeserializer;
use opendatafabric::*;

pub struct SparkEngine {
    container_runtime: ContainerRuntime,
    image: String,
    run_info_dir: Arc<Path>,
    dataset_repo: Arc<dyn DatasetRepository>,
}

struct RunInfo {
    in_out_dir: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl RunInfo {
    fn new(in_out_dir: PathBuf, logs_dir: &Path) -> Self {
        Self {
            in_out_dir,
            stdout_path: logs_dir.join("spark.out.txt"),
            stderr_path: logs_dir.join("spark.err.txt"),
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
        run_info_dir: Arc<Path>,
        dataset_repo: Arc<dyn DatasetRepository>,
    ) -> Self {
        Self {
            container_runtime,
            image: image.to_owned(),
            run_info_dir,
            dataset_repo,
        }
    }

    async fn materialize_request(
        &self,
        request: IngestRequest,
        operation_dir: &Path,
    ) -> Result<MaterializedEngineRequest, InternalError> {
        let dataset = self
            .dataset_repo
            .get_dataset(&request.dataset_handle.as_local_ref())
            .await
            .int_err()?;

        let host_in_out_dir = operation_dir.join("in-out");
        std::fs::create_dir(&host_in_out_dir).int_err()?;

        let host_out_dir = operation_dir.join("out");
        let host_out_data_path = host_out_dir.join("data");
        let host_out_checkpoint_path = host_out_dir.join("checkpoint");
        std::fs::create_dir(&host_out_dir).int_err()?;

        let container_out_dir = PathBuf::from("/opt/engine/out");
        let container_inout_dir = PathBuf::from("/opt/engine/in-out");
        let container_in_raw_data_path = PathBuf::from("/opt/engine/in/raw-data");
        let container_in_prev_data_path = PathBuf::from("/opt/engine/in/prev-data");
        let container_out_data_path = PathBuf::from("/opt/engine/out/data");
        let container_out_checkpoint_path = PathBuf::from("/opt/engine/out/checkpoint");

        let mut volumes = vec![
            (host_out_dir, container_out_dir, VolumeAccess::ReadWrite).into(),
            (
                host_in_out_dir,
                container_inout_dir,
                VolumeAccess::ReadWrite,
            )
                .into(),
            (
                request.input_data_path,
                &container_in_raw_data_path,
                VolumeAccess::ReadOnly,
            )
                .into(),
        ];

        // Get existing data directory
        // TODO: This seems dirty
        let some_random_hash: Multihash = Multihash::from_digest_sha3_256(b"");
        let dataset_data_dir = kamu_data_utils::data::local_url::into_local_path(
            dataset
                .as_data_repo()
                .get_internal_url(&some_random_hash)
                .await,
        )
        .int_err()?
        .parent()
        .unwrap()
        .to_path_buf();

        // Don't mount existing data directory if it's empty as it will confuse Spark
        if request.next_offset != 0 {
            volumes.push(
                (
                    dataset_data_dir,
                    &container_in_prev_data_path,
                    VolumeAccess::ReadOnly,
                )
                    .into(),
            );
        }

        assert!(request.prev_checkpoint.is_none());

        Ok(MaterializedEngineRequest {
            engine_request: IngestRequestRaw {
                dataset_id: request.dataset_handle.id,
                dataset_name: request.dataset_handle.alias.dataset_name,
                input_data_path: container_in_raw_data_path,
                output_data_path: container_out_data_path,
                system_time: request.system_time,
                event_time: request.event_time,
                offset: request.next_offset,
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
                    ..request.polling_source.unwrap()
                },
                dataset_vocab: request.vocab,
                // TODO: Not passing any checkpoint currently as Spark ingest doesn't use them
                prev_checkpoint_path: None,
                new_checkpoint_path: container_out_checkpoint_path,
                prev_watermark: request.prev_watermark,
                data_dir: container_in_prev_data_path,
            },
            out_data_path: host_out_data_path,
            out_checkpoint_path: host_out_checkpoint_path,
            volumes,
        })
    }

    async fn materialize_response(
        &self,
        engine_response: ExecuteQueryResponseSuccess,
        next_offset: i64,
        out_data_path: PathBuf,
        out_checkpoint_path: PathBuf,
    ) -> Result<IngestResponse, EngineError> {
        let out_data = if let Some(data_interval) = &engine_response.data_interval {
            if data_interval.end < data_interval.start || data_interval.start != next_offset {
                return Err(EngineError::contract_error(
                    "Engine returned an output slice with invalid data inverval",
                    Vec::new(),
                )
                .into());
            }
            if !out_data_path.exists() {
                return Err(EngineError::contract_error(
                    "Engine did not write a response data file",
                    Vec::new(),
                )
                .into());
            }
            if out_data_path.is_symlink() || !out_data_path.is_file() {
                return Err(EngineError::contract_error(
                    "Engine wrote data not as a plain file",
                    Vec::new(),
                )
                .into());
            }
            Some(OwnedFile::new(out_data_path).into())
        } else {
            if out_data_path.exists() {
                return Err(EngineError::contract_error(
                    "Engine wrote data file while the ouput slice is empty",
                    Vec::new(),
                )
                .into());
            }
            None
        };

        let out_checkpoint = if out_checkpoint_path.exists() {
            if out_checkpoint_path.is_symlink() || !out_checkpoint_path.is_file() {
                return Err(EngineError::contract_error(
                    "Engine wrote checkpoint not as a plain file",
                    Vec::new(),
                )
                .into());
            }
            Some(OwnedFile::new(out_checkpoint_path).into())
        } else {
            None
        };

        Ok(IngestResponse {
            data_interval: engine_response.data_interval,
            output_watermark: engine_response.output_watermark,
            out_checkpoint,
            out_data,
        })
    }

    async fn ingest_impl(
        &self,
        run_info: RunInfo,
        request: IngestRequestRaw,
        volumes: Vec<VolumeSpec>,
    ) -> Result<ExecuteQueryResponseSuccess, EngineError> {
        let request_path = run_info.in_out_dir.join("request.yaml");
        let response_path = run_info.in_out_dir.join("response.yaml");

        {
            tracing::info!(?request, path = ?request_path, "Writing request");
            let file = File::create(&request_path)?;
            serde_yaml::to_writer(file, &request)
                .map_err(|e| EngineError::internal(e, Vec::new()))?;
        }

        let stdout_file = std::fs::File::create(&run_info.stdout_path)?;
        let stderr_file = std::fs::File::create(&run_info.stderr_path)?;

        let status = self
            .container_runtime
            .run_attached(&self.image)
            .volumes(volumes.clone())
            .user("root")
            .shell_cmd(
                "/opt/bitnami/spark/bin/spark-submit --master=local[4] --driver-memory=2g \
                 --class=dev.kamu.engine.spark.ingest.IngestApp /opt/engine/bin/engine.spark.jar",
            )
            .stdout(stdout_file)
            .stderr(stderr_file)
            .status()
            .await
            .map_err(|e| EngineError::internal(e, run_info.log_files()))?;

        // Fix permissions
        if self.container_runtime.config.runtime == ContainerRuntimeType::Docker {
            cfg_if::cfg_if! {
                if #[cfg(unix)] {
                    self.container_runtime
                        .run_attached(&self.image)
                        .shell_cmd(format!(
                            "chown -Rf {}:{} /opt/engine/in-out /opt/engine/out",
                            unsafe { libc::geteuid() },
                            unsafe { libc::getegid() },
                        ))
                        .user("root")
                        .volumes(volumes)
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                        .await
                        .map_err(|e| EngineError::internal(e, Vec::new()))?;
                }
            }
        }

        if response_path.exists() {
            let data = std::fs::read_to_string(&response_path)?;
            let response = YamlEngineProtocol
                .read_execute_query_response(data.as_bytes())
                .map_err(|e| EngineError::internal(e, run_info.log_files()))?;

            tracing::info!(?response, "Read response");

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
    async fn ingest(&self, request: IngestRequest) -> Result<IngestResponse, EngineError> {
        let operation_dir = self
            .run_info_dir
            .join(format!("ingest-{}", &request.operation_id));
        let logs_dir = operation_dir.join("logs");
        std::fs::create_dir(&operation_dir).map_err(|e| EngineError::internal(e, Vec::new()))?;
        std::fs::create_dir(&logs_dir).map_err(|e| EngineError::internal(e, Vec::new()))?;

        let next_offset = request.next_offset;

        let materialized_request = self
            .materialize_request(request, &operation_dir)
            .await
            .map_err(|e| EngineError::internal(e, Vec::new()))?;

        let run_info = RunInfo::new(operation_dir.join("in-out"), &logs_dir);

        let engine_response = self
            .ingest_impl(
                run_info,
                materialized_request.engine_request,
                materialized_request.volumes,
            )
            .await?;

        let response = self
            .materialize_response(
                engine_response,
                next_offset,
                materialized_request.out_data_path,
                materialized_request.out_checkpoint_path,
            )
            .await?;

        Ok(response)
    }
}

struct MaterializedEngineRequest {
    engine_request: IngestRequestRaw,
    out_data_path: PathBuf,
    out_checkpoint_path: PathBuf,
    volumes: Vec<VolumeSpec>,
}
