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

use ::serde::{Deserialize, Serialize};
use ::serde_with::skip_serializing_none;
use chrono::{DateTime, Utc};
use container_runtime::*;
use kamu_core::engine::{IngestResponse, *};
use kamu_core::*;
use opendatafabric::engine::ExecuteQueryError;
use opendatafabric::serde::yaml::{YamlEngineProtocol, *};
use opendatafabric::serde::EngineProtocolDeserializer;
use opendatafabric::*;

use crate::ObjectRepositoryLocalFS;

///////////////////////////////////////////////////////////////////////////////

pub struct SparkEngine {
    container_runtime: ContainerRuntime,
    image: String,
    run_info_dir: Arc<Path>,
    dataset_repo: Arc<dyn DatasetRepository>,
}

///////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////////

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
        dataset: &dyn Dataset,
        request: IngestRequest,
        operation_dir: &Path,
    ) -> Result<MaterializedEngineRequest, InternalError> {
        match dataset.as_data_repo().protocol() {
            ObjectRepositoryProtocol::LocalFs { .. } => {
                self.materialize_request_local(dataset, request, operation_dir)
                    .await
            }
            ObjectRepositoryProtocol::Memory
            | ObjectRepositoryProtocol::Http
            | ObjectRepositoryProtocol::S3 => {
                self.materialize_request_remote(dataset, request, operation_dir)
                    .await
            }
        }
    }

    async fn materialize_request_local(
        &self,
        dataset: &dyn Dataset,
        request: IngestRequest,
        operation_dir: &Path,
    ) -> Result<MaterializedEngineRequest, InternalError> {
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

        let dataset_data_dir = match dataset.as_data_repo().protocol() {
            ObjectRepositoryProtocol::LocalFs { base_dir } => base_dir,
            _ => unreachable!(),
        };

        // Don't mount existing data directory if it's empty as it will confuse Spark
        if request.prev_offset.is_some() {
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
                offset: request.prev_offset.map(|v| v + 1).unwrap_or(0),
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
                    ..request.polling_source
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

    async fn materialize_request_remote(
        &self,
        dataset: &dyn Dataset,
        request: IngestRequest,
        operation_dir: &Path,
    ) -> Result<MaterializedEngineRequest, InternalError> {
        use futures::StreamExt;

        // Download ALL data files
        // TODO: This is awful, slow, and expensive, but will be removed once ingest can
        // work directly over S3
        let host_in_data_dir = operation_dir.join("in").join("data");
        std::fs::create_dir_all(&host_in_data_dir).int_err()?;
        let in_data_repo =
            ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(host_in_data_dir.clone());

        let data_repo = dataset.as_data_repo();
        let mut blocks = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_data_stream_blocks();

        while let Some(block) = blocks.next().await {
            let (_, block) = block.int_err()?;
            if let Some(data_slice) = block.event.new_data {
                let src_url = data_repo.get_internal_url(&data_slice.physical_hash).await;

                tracing::info!(
                    %src_url,
                    size = %data_slice.size,
                    "Downloading remote data file locally",
                );

                let stream = data_repo
                    .get_stream(&data_slice.physical_hash)
                    .await
                    .int_err()?;

                in_data_repo
                    .insert_stream(
                        stream,
                        InsertOpts {
                            precomputed_hash: Some(&data_slice.physical_hash),
                            expected_hash: Some(&data_slice.physical_hash),
                            size_hint: Some(data_slice.size as usize),
                        },
                    )
                    .await
                    .int_err()?;
            }
        }

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

        // Don't mount existing data directory if it's empty as it will confuse Spark
        if request.prev_offset.is_some() {
            volumes.push(
                (
                    host_in_data_dir,
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
                offset: request.prev_offset.map(|v| v + 1).unwrap_or(0),
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
                    ..request.polling_source
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
            serde_yaml::to_writer(file, &request).int_err()?;
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
                        .int_err()?;
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
                ExecuteQueryResponse::Progress(_) => unreachable!(),
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

    pub async fn materialize_response(
        &self,
        engine_response: ExecuteQueryResponseSuccess,
        new_data_path: PathBuf,
        new_checkpoint_path: PathBuf,
    ) -> Result<IngestResponse, EngineError> {
        let new_data = if engine_response.new_offset_interval.is_some() {
            if !new_data_path.exists() {
                return Err(EngineError::contract_error(
                    "Engine did not write a response data file",
                    Vec::new(),
                )
                .into());
            }
            if new_data_path.is_symlink() || !new_data_path.is_file() {
                return Err(EngineError::contract_error(
                    "Engine wrote data not as a plain file",
                    Vec::new(),
                )
                .into());
            }
            Some(OwnedFile::new(new_data_path).into())
        } else {
            if new_data_path.exists() {
                return Err(EngineError::contract_error(
                    "Engine wrote data file while the ouput slice is empty",
                    Vec::new(),
                )
                .into());
            }
            None
        };

        let new_checkpoint = if new_checkpoint_path.exists() {
            if new_checkpoint_path.is_symlink() || !new_checkpoint_path.is_file() {
                return Err(EngineError::contract_error(
                    "Engine wrote checkpoint not as a plain file",
                    Vec::new(),
                )
                .into());
            }
            Some(OwnedFile::new(new_checkpoint_path).into())
        } else {
            None
        };

        Ok(IngestResponse {
            new_offset_interval: engine_response.new_offset_interval,
            new_watermark: engine_response.new_watermark,
            new_checkpoint,
            new_data,
        })
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl IngestEngine for SparkEngine {
    async fn ingest(&self, request: IngestRequest) -> Result<IngestResponse, EngineError> {
        let operation_dir = self
            .run_info_dir
            .join(format!("ingest-{}", &request.operation_id));
        let logs_dir = operation_dir.join("logs");
        std::fs::create_dir(&operation_dir).int_err()?;
        std::fs::create_dir(&logs_dir).int_err()?;

        let dataset = self
            .dataset_repo
            .get_dataset(&request.dataset_handle.as_local_ref())
            .await
            .int_err()?;

        let materialized_request = self
            .materialize_request(dataset.as_ref(), request, &operation_dir)
            .await
            .int_err()?;

        let run_info = RunInfo::new(operation_dir.join("in-out"), &logs_dir);

        let engine_response = self
            .ingest_impl(
                run_info,
                materialized_request.engine_request,
                materialized_request.volumes,
            )
            .await?;

        self.materialize_response(
            engine_response,
            materialized_request.out_data_path,
            materialized_request.out_checkpoint_path,
        )
        .await
    }
}

///////////////////////////////////////////////////////////////////////////////

struct MaterializedEngineRequest {
    engine_request: IngestRequestRaw,
    out_data_path: PathBuf,
    out_checkpoint_path: PathBuf,
    volumes: Vec<VolumeSpec>,
}

///////////////////////////////////////////////////////////////////////////////

// Has to be in sync with kamu-engine-spark repo
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IngestRequestRaw {
    #[serde(rename = "datasetID")]
    pub dataset_id: DatasetID,
    pub dataset_name: DatasetName,
    pub input_data_path: PathBuf,
    pub output_data_path: PathBuf,
    #[serde(with = "datetime_rfc3339")]
    pub system_time: DateTime<Utc>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub event_time: Option<DateTime<Utc>>,
    pub offset: u64,
    #[serde(with = "SetPollingSourceDef")]
    pub source: SetPollingSource,
    #[serde(with = "DatasetVocabularyDef")]
    pub dataset_vocab: DatasetVocabulary,
    pub prev_checkpoint_path: Option<PathBuf>,
    pub new_checkpoint_path: PathBuf,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub prev_watermark: Option<DateTime<Utc>>,
    pub data_dir: PathBuf,
}
