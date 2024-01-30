// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use container_runtime::*;
use kamu_core::engine::*;
use kamu_core::*;
use odf::engine::{EngineGrpcClient, ExecuteRawQueryError, ExecuteTransformError};
use odf::TransformResponseSuccess;
use opendatafabric as odf;

use super::engine_container::{EngineContainer, LogsConfig};
use super::engine_io_strategy::*;
use super::ODFEngineConfig;

pub struct ODFEngine {
    container_runtime: Arc<ContainerRuntime>,
    engine_config: ODFEngineConfig,
    image: String,
    run_info_dir: Arc<Path>,
    dataset_repo: Arc<dyn DatasetRepository>,
}

impl ODFEngine {
    pub fn new(
        container_runtime: Arc<ContainerRuntime>,
        engine_config: ODFEngineConfig,
        image: &str,
        run_info_dir: Arc<Path>,
        dataset_repo: Arc<dyn DatasetRepository>,
    ) -> Self {
        Self {
            container_runtime,
            engine_config,
            image: image.to_owned(),
            run_info_dir,
            dataset_repo,
        }
    }

    // TODO: Currently we are always proxying remote inputs, but in future we should
    // have a capabilities mechanism for engines to declare that they can work
    // with some remote storages directly without us needing to proxy data.
    async fn get_io_strategy(
        &self,
        request: &TransformRequestExt,
    ) -> Result<Arc<dyn EngineIoStrategy>, InternalError> {
        let dataset = self
            .dataset_repo
            .get_dataset(&request.dataset_handle.as_local_ref())
            .await
            .int_err()?;

        match dataset.as_data_repo().protocol() {
            ObjectRepositoryProtocol::LocalFs { .. } => Ok(Arc::new(
                EngineIoStrategyLocalVolume::new(self.dataset_repo.clone()),
            )),
            ObjectRepositoryProtocol::Memory
            | ObjectRepositoryProtocol::Http
            | ObjectRepositoryProtocol::S3 => Ok(Arc::new(EngineIoStrategyRemoteProxy::new(
                self.dataset_repo.clone(),
            ))),
        }
    }

    #[tracing::instrument(level = "info", skip_all, fields(container_name = engine_container.container_name()))]
    async fn execute_raw_query(
        &self,
        engine_container: &EngineContainer,
        engine_client: &mut EngineGrpcClient,
        request: odf::RawQueryRequest,
    ) -> Result<odf::RawQueryResponseSuccess, EngineError> {
        tracing::info!(?request, "Performing engine operation");

        let output_data_path = request.output_data_path.clone();

        let response = engine_client.execute_raw_query(request).await;

        tracing::info!(?response, "Operation response");

        cfg_if::cfg_if! {
            if #[cfg(unix)] {
                if self.container_runtime.config.runtime == ContainerRuntimeType::Docker {
                    tracing::info!("Fixing up file permissions");

                    engine_container
                        .exec_shell_cmd(
                            ExecArgs::default(),
                            format!(
                                "chown -Rf {}:{} {}",
                                unsafe { libc::geteuid() },
                                unsafe { libc::getegid() },
                                output_data_path.display(),
                            )
                        )
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                        .await?;
                }
            }
        }

        response.map_err(|e| match e {
            ExecuteRawQueryError::InvalidQuery(e) => {
                EngineError::invalid_query(e.message, engine_container.log_files())
            }
            e @ ExecuteRawQueryError::EngineInternalError(_) => {
                EngineError::internal(e, engine_container.log_files())
            }
            ExecuteRawQueryError::InternalError(e) => {
                EngineError::internal(e, engine_container.log_files())
            }
        })
    }

    #[tracing::instrument(level = "info", skip_all, fields(container_name = engine_container.container_name()))]
    async fn execute_transform(
        &self,
        engine_container: &EngineContainer,
        engine_client: &mut EngineGrpcClient,
        request: odf::TransformRequest,
    ) -> Result<odf::TransformResponseSuccess, EngineError> {
        tracing::info!(?request, "Performing engine operation");

        let new_checkpoint_path = request.new_checkpoint_path.clone();
        let new_data_path = request.new_data_path.clone();

        let response = engine_client.execute_transform(request).await;

        tracing::info!(?response, "Operation response");

        cfg_if::cfg_if! {
            if #[cfg(unix)] {
                if self.container_runtime.config.runtime == ContainerRuntimeType::Docker {
                    tracing::info!("Fixing up file permissions");

                    engine_container
                        .exec_shell_cmd(
                            ExecArgs::default(),
                            format!(
                                "chown -Rf {}:{} {} {}",
                                unsafe { libc::geteuid() },
                                unsafe { libc::getegid() },
                                new_checkpoint_path.display(),
                                new_data_path.display(),
                            )
                        )
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                        .await?;
                }
            }
        }

        response.map_err(|e| match e {
            ExecuteTransformError::InvalidQuery(e) => {
                EngineError::invalid_query(e.message, engine_container.log_files())
            }
            e @ ExecuteTransformError::EngineInternalError(_) => {
                EngineError::internal(e, engine_container.log_files())
            }
            ExecuteTransformError::InternalError(e) => {
                EngineError::internal(e, engine_container.log_files())
            }
        })
    }

    pub fn materialize_response(
        &self,
        engine_response: TransformResponseSuccess,
        new_data_path: PathBuf,
        new_checkpoint_path: PathBuf,
    ) -> Result<TransformResponseExt, EngineError> {
        let new_data = if engine_response.new_offset_interval.is_some() {
            if !new_data_path.exists() {
                return Err(EngineError::contract_error(
                    "Engine did not write a response data file",
                    Vec::new(),
                ));
            }
            if new_data_path.is_symlink() || !new_data_path.is_file() {
                return Err(EngineError::contract_error(
                    "Engine wrote data not as a plain file",
                    Vec::new(),
                ));
            }
            Some(OwnedFile::new(new_data_path))
        } else {
            if new_data_path.exists() {
                return Err(EngineError::contract_error(
                    "Engine wrote data file while the output slice is empty",
                    Vec::new(),
                ));
            }
            None
        };

        let new_checkpoint = if new_checkpoint_path.exists() {
            if new_checkpoint_path.is_symlink() || !new_checkpoint_path.is_file() {
                return Err(EngineError::contract_error(
                    "Engine wrote checkpoint not as a plain file",
                    Vec::new(),
                ));
            }
            Some(OwnedFile::new(new_checkpoint_path))
        } else {
            None
        };

        Ok(TransformResponseExt {
            new_offset_interval: engine_response.new_offset_interval,
            new_watermark: engine_response.new_watermark,
            new_checkpoint,
            new_data,
        })
    }
}

#[async_trait::async_trait]
impl Engine for ODFEngine {
    async fn execute_raw_query(
        &self,
        request: RawQueryRequestExt,
    ) -> Result<RawQueryResponseExt, EngineError> {
        let operation_id = request.operation_id.clone();
        let operation_dir = self
            .run_info_dir
            .join(format!("raw-query-{}", &request.operation_id));
        let logs_dir = operation_dir.join("logs");
        std::fs::create_dir(&operation_dir).int_err()?;
        std::fs::create_dir(&logs_dir).int_err()?;

        let host_in_dir = operation_dir.join("in");
        let host_out_dir = operation_dir.join("out");
        let _ = std::fs::create_dir_all(&host_in_dir);
        let _ = std::fs::create_dir_all(&host_out_dir);

        let host_input_data_path = host_in_dir.join("input");
        let host_output_data_path = host_out_dir.join("output");

        // Note: not using `PathBuf::join()` below to ensure linux style paths
        let container_in_dir = PathBuf::from("/opt/engine/in");
        let container_out_dir = PathBuf::from("/opt/engine/out");
        let container_input_data_path = PathBuf::from("/opt/engine/in/input");
        let container_output_data_path = PathBuf::from("/opt/engine/out/output");

        let volumes = vec![
            VolumeSpec {
                source: host_in_dir.clone(),
                dest: container_in_dir,
                access: VolumeAccess::ReadOnly,
            },
            VolumeSpec {
                source: host_out_dir.clone(),
                dest: container_out_dir,
                access: VolumeAccess::ReadWrite,
            },
        ];

        // TODO: Reconsider single-file output
        request
            .input_data
            .write_parquet(
                host_input_data_path.as_os_str().to_str().unwrap(),
                datafusion::dataframe::DataFrameWriteOptions::new().with_single_file_output(true),
                Some(
                    datafusion::parquet::file::properties::WriterProperties::builder()
                        .set_writer_version(
                            datafusion::parquet::file::properties::WriterVersion::PARQUET_1_0,
                        )
                        .set_compression(datafusion::parquet::basic::Compression::SNAPPY)
                        .build(),
                ),
            )
            .await
            .int_err()?;

        let materialized_request = odf::RawQueryRequest {
            input_data_paths: vec![container_input_data_path],
            transform: request.transform,
            output_data_path: container_output_data_path.clone(),
        };

        let engine_container = EngineContainer::new(
            self.container_runtime.clone(),
            self.engine_config.clone(),
            LogsConfig::new(&logs_dir),
            &self.image,
            volumes,
            &operation_id,
        )
        .await?;

        let mut engine_client = engine_container.connect_client().await?;

        let engine_response = self
            .execute_raw_query(&engine_container, &mut engine_client, materialized_request)
            .await;

        engine_container.terminate().await?;

        let output_data = if engine_response?.num_records == 0 {
            None
        } else {
            Some(
                request
                    .ctx
                    .read_parquet(
                        host_output_data_path.as_os_str().to_str().unwrap(),
                        datafusion::execution::options::ParquetReadOptions {
                            file_extension: "",
                            ..Default::default()
                        },
                    )
                    .await
                    .int_err()?,
            )
        };

        Ok(RawQueryResponseExt { output_data })
    }

    async fn execute_transform(
        &self,
        request: TransformRequestExt,
    ) -> Result<TransformResponseExt, EngineError> {
        let dataset = self
            .dataset_repo
            .get_dataset(&request.dataset_handle.as_local_ref())
            .await
            .int_err()?;

        let operation_id = request.operation_id.clone();
        let operation_dir = self
            .run_info_dir
            .join(format!("transform-{}", &request.operation_id));
        let logs_dir = operation_dir.join("logs");
        std::fs::create_dir(&operation_dir).int_err()?;
        std::fs::create_dir(&logs_dir).int_err()?;

        let io_strategy = self.get_io_strategy(&request).await.int_err()?;

        let materialized_request = io_strategy
            .materialize_request(dataset.as_ref(), request, &operation_dir)
            .await
            .int_err()?;

        let engine_container = EngineContainer::new(
            self.container_runtime.clone(),
            self.engine_config.clone(),
            LogsConfig::new(&logs_dir),
            &self.image,
            materialized_request.volumes,
            &operation_id,
        )
        .await?;

        let mut engine_client = engine_container.connect_client().await?;

        let engine_response = self
            .execute_transform(
                &engine_container,
                &mut engine_client,
                materialized_request.engine_request,
            )
            .await;

        engine_container.terminate().await?;

        self.materialize_response(
            engine_response?,
            materialized_request.out_data_path,
            materialized_request.out_checkpoint_path,
        )
    }
}
