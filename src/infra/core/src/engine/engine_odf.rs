// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use container_runtime::*;
use datafusion::config::{ParquetOptions, TableParquetOptions};
use file_utils::OwnedFile;
use internal_error::ResultIntoInternal;
use kamu_core::engine::*;
use kamu_core::*;
use kamu_datasets::ResolvedDatasetsMap;
use odf::metadata::engine::{EngineGrpcClient, ExecuteRawQueryError, ExecuteTransformError};

use super::ODFEngineConfig;
use super::engine_container::{EngineContainer, LogsConfig};
use super::engine_io_strategy::*;

pub struct ODFEngine {
    container_runtime: Arc<ContainerRuntime>,
    engine_config: ODFEngineConfig,
    image: String,
    run_info_dir: Arc<RunInfoDir>,
}

impl ODFEngine {
    pub fn new(
        container_runtime: Arc<ContainerRuntime>,
        engine_config: ODFEngineConfig,
        image: &str,
        run_info_dir: Arc<RunInfoDir>,
    ) -> Self {
        Self {
            container_runtime,
            engine_config,
            image: image.to_owned(),
            run_info_dir,
        }
    }

    // TODO: Currently we are always proxying remote inputs, but in future we should
    // have a capabilities mechanism for engines to declare that they can work
    // with some remote storages directly without us needing to proxy data.
    fn get_io_strategy(&self, target_dataset: &dyn odf::Dataset) -> Arc<dyn EngineIoStrategy> {
        use odf::storage::ObjectRepositoryProtocol;
        match target_dataset.as_data_repo().protocol() {
            ObjectRepositoryProtocol::LocalFs { .. } => Arc::new(EngineIoStrategyLocalVolume {}),
            ObjectRepositoryProtocol::Memory
            | ObjectRepositoryProtocol::Http
            | ObjectRepositoryProtocol::S3 => Arc::new(EngineIoStrategyRemoteProxy {}),
        }
    }

    #[tracing::instrument(level = "info", skip_all, fields(container_name = engine_container.container_name()))]
    async fn execute_raw_query(
        &self,
        engine_container: &EngineContainer,
        engine_client: &mut EngineGrpcClient,
        request: odf::metadata::RawQueryRequest,
    ) -> Result<odf::metadata::RawQueryResponseSuccess, EngineError> {
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
        request: odf::metadata::TransformRequest,
    ) -> Result<odf::metadata::TransformResponseSuccess, EngineError> {
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
        engine_response: odf::metadata::TransformResponseSuccess,
        new_data_path: PathBuf,
        new_checkpoint_path: PathBuf,
    ) -> Result<TransformResponseExt, EngineError> {
        let (new_data, output_schema) = if new_data_path.exists() {
            if new_data_path.is_symlink() || !new_data_path.is_file() {
                return Err(EngineError::contract_error(
                    "Engine wrote data not as a plain file",
                    Vec::new(),
                ));
            }

            // Read output schema
            let output_schema =
                datafusion::parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
                    &std::fs::File::open(&new_data_path).int_err()?,
                    Default::default(),
                )
                .int_err()?
                .schema()
                .clone();

            let new_data = OwnedFile::new(new_data_path);

            // Clean up the data file if it's empty - it was produced only to provide us the
            // schema
            let new_data = if engine_response.new_offset_interval.is_none() {
                None
            } else {
                Some(new_data)
            };

            (new_data, Some(output_schema))
        } else {
            if engine_response.new_offset_interval.is_some() {
                return Err(EngineError::contract_error(
                    "Engine did not write a response data file",
                    Vec::new(),
                ));
            }

            tracing::warn!(
                "Engine didn't write a data file when output slice is empty - in future this will \
                 be considered an error"
            );

            (None, None)
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
            output_schema,
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

        // FIXME: The .parquet extension is currently necessary for DataFusion to
        // respect the single-file output
        // See: https://github.com/apache/datafusion/issues/13323
        let host_input_data_path = host_in_dir.join("input.parquet");
        let host_output_data_path = host_out_dir.join("output.parquet");

        // Note: not using `PathBuf::join()` below to ensure linux style paths
        let container_in_dir = PathBuf::from("/opt/engine/in");
        let container_out_dir = PathBuf::from("/opt/engine/out");
        let container_input_data_path = PathBuf::from("/opt/engine/in/input.parquet");
        let container_output_data_path = PathBuf::from("/opt/engine/out/output.parquet");

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
                Some(TableParquetOptions {
                    global: ParquetOptions {
                        writer_version:
                            datafusion::common::parquet_config::DFParquetWriterVersion::V1_0,
                        compression: Some("snappy".into()),
                        ..Default::default()
                    },
                    column_specific_options: HashMap::new(),
                    key_value_metadata: HashMap::new(),
                    crypto: datafusion::config::ParquetEncryptionOptions::default(),
                }),
            )
            .await
            .int_err()?;

        let materialized_request = odf::metadata::RawQueryRequest {
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
                    .int_err()?
                    .into(),
            )
        };

        Ok(RawQueryResponseExt { output_data })
    }

    async fn execute_transform(
        &self,
        request: TransformRequestExt,
        datasets_map: &ResolvedDatasetsMap,
    ) -> Result<TransformResponseExt, EngineError> {
        let operation_id = request.operation_id.clone();
        let operation_dir = self
            .run_info_dir
            .join(format!("transform-{}", &request.operation_id));
        let logs_dir = operation_dir.join("logs");
        std::fs::create_dir(&operation_dir).int_err()?;
        std::fs::create_dir(&logs_dir).int_err()?;

        let target = datasets_map.get_by_handle(&request.dataset_handle);
        let io_strategy = self.get_io_strategy(target.as_ref());

        let materialized_request = io_strategy
            .materialize_request(request, datasets_map, &operation_dir)
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
