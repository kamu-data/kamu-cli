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
use kamu_core::*;
use odf::engine::{EngineGrpcClient, ExecuteQueryError};
use odf::{ExecuteQueryInput, ExecuteQueryRequest, ExecuteQueryResponseSuccess, Multihash};
use opendatafabric as odf;

use super::engine_container::{EngineContainer, LogsConfig};
use super::ODFEngineConfig;

pub struct ODFEngine {
    container_runtime: ContainerRuntime,
    engine_config: ODFEngineConfig,
    image: String,
    run_info_dir: Arc<Path>,
    dataset_repo: Arc<dyn DatasetRepository>,
}

impl ODFEngine {
    pub fn new(
        container_runtime: ContainerRuntime,
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

    #[tracing::instrument(level = "info", skip_all, fields(container_name = engine_container.container_name()))]
    async fn execute_query(
        &self,
        engine_container: &EngineContainer,
        engine_client: &mut EngineGrpcClient,
        request: odf::ExecuteQueryRequest,
    ) -> Result<odf::ExecuteQueryResponseSuccess, EngineError> {
        tracing::info!(?request, "Performing engine operation");

        let new_checkpoint_path = request.new_checkpoint_path.clone();
        let out_data_path = request.out_data_path.clone();

        let response = engine_client.execute_query(request).await;

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
                                out_data_path.display(),
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
            ExecuteQueryError::InvalidQuery(e) => {
                EngineError::invalid_query(e.message, engine_container.log_files())
            }
            e @ ExecuteQueryError::EngineInternalError(_) => {
                EngineError::internal(e, engine_container.log_files())
            }
            ExecuteQueryError::InternalError(e) => {
                EngineError::internal(e, engine_container.log_files())
            }
        })
    }

    async fn materialize_object(
        &self,
        repo: &dyn ObjectRepository,
        hash: &Multihash,
        container_in_dir: &Path,
        volumes: &mut Vec<VolumeSpec>,
    ) -> Result<PathBuf, InternalError> {
        let url = repo.get_internal_url(hash).await;
        let host_path = kamu_data_utils::data::local_url::into_local_path(url).int_err()?;
        let container_path = container_in_dir.join(hash.to_string());
        volumes.push((host_path, container_path.clone(), VolumeAccess::ReadOnly).into());
        Ok(container_path)
    }

    async fn maybe_materialize_object(
        &self,
        repo: &dyn ObjectRepository,
        hash: Option<&Multihash>,
        container_in_dir: &Path,
        volumes: &mut Vec<VolumeSpec>,
    ) -> Result<Option<PathBuf>, InternalError> {
        if let Some(hash) = hash {
            Ok(Some(
                self.materialize_object(repo, hash, container_in_dir, volumes)
                    .await?,
            ))
        } else {
            Ok(None)
        }
    }

    async fn materialize_request(
        &self,
        request: TransformRequest,
        operation_dir: &Path,
    ) -> Result<MaterializedEngineRequest, InternalError> {
        let host_out_dir = operation_dir.join("out");
        let host_out_data_path = host_out_dir.join("data");
        let host_out_checkpoint_path = host_out_dir.join("checkpoint");
        std::fs::create_dir(&host_out_dir).int_err()?;

        let container_in_dir = PathBuf::from("/opt/engine/in");
        let container_out_dir = PathBuf::from("/opt/engine/out");
        let container_out_data_path = PathBuf::from("/opt/engine/out/data");
        let container_out_checkpoint_path = PathBuf::from("/opt/engine/out/checkpoint");

        let mut volumes = vec![(host_out_dir, container_out_dir, VolumeAccess::ReadWrite).into()];

        let dataset = self
            .dataset_repo
            .get_dataset(&request.dataset_handle.as_local_ref())
            .await
            .int_err()?;

        let prev_checkpoint_path = self
            .maybe_materialize_object(
                dataset.as_checkpoint_repo(),
                request.prev_checkpoint.as_ref(),
                &container_in_dir,
                &mut volumes,
            )
            .await?;

        let mut inputs = Vec::new();
        for input in request.inputs {
            let input_dataset = self
                .dataset_repo
                .get_dataset(&input.dataset_handle.as_local_ref())
                .await
                .int_err()?;

            let mut schema_file = None;
            let mut data_paths = Vec::new();
            for hash in input.data_slices {
                let container_path = self
                    .materialize_object(
                        input_dataset.as_data_repo(),
                        &hash,
                        &container_in_dir,
                        &mut volumes,
                    )
                    .await?;

                if hash == input.schema_slice {
                    schema_file = Some(container_path.clone());
                }

                data_paths.push(container_path);
            }

            let schema_file = if let Some(schema_file) = schema_file {
                schema_file
            } else {
                self.materialize_object(
                    input_dataset.as_data_repo(),
                    &input.schema_slice,
                    &container_in_dir,
                    &mut volumes,
                )
                .await?
            };

            inputs.push(ExecuteQueryInput {
                dataset_id: input.dataset_handle.id,
                dataset_name: input.dataset_handle.alias.dataset_name,
                vocab: input.vocab,
                data_interval: input.data_interval,
                data_paths,
                schema_file,
                explicit_watermarks: input.explicit_watermarks,
            })
        }

        let engine_request = ExecuteQueryRequest {
            dataset_id: request.dataset_handle.id,
            dataset_name: request.dataset_handle.alias.dataset_name,
            system_time: request.system_time,
            offset: request.next_offset,
            vocab: request.vocab,
            transform: request.transform,
            inputs,
            prev_checkpoint_path,
            new_checkpoint_path: container_out_checkpoint_path,
            out_data_path: container_out_data_path,
        };

        Ok(MaterializedEngineRequest {
            engine_request,
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
    ) -> Result<TransformResponse, EngineError> {
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

        Ok(TransformResponse {
            data_interval: engine_response.data_interval,
            output_watermark: engine_response.output_watermark,
            out_checkpoint,
            out_data,
        })
    }
}

#[async_trait::async_trait]
impl Engine for ODFEngine {
    async fn transform(&self, request: TransformRequest) -> Result<TransformResponse, EngineError> {
        let operation_id = request.operation_id.clone();
        let operation_dir = self
            .run_info_dir
            .join(format!("transform-{}", &request.operation_id));
        let logs_dir = operation_dir.join("logs");
        std::fs::create_dir(&operation_dir).map_err(|e| EngineError::internal(e, Vec::new()))?;
        std::fs::create_dir(&logs_dir).map_err(|e| EngineError::internal(e, Vec::new()))?;

        let next_offset = request.next_offset;

        let materialized_request = self
            .materialize_request(request, &operation_dir)
            .await
            .map_err(|e| EngineError::internal(e, Vec::new()))?;

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
            .execute_query(
                &engine_container,
                &mut engine_client,
                materialized_request.engine_request,
            )
            .await;

        engine_container.terminate().await?;

        self.materialize_response(
            engine_response?,
            next_offset,
            materialized_request.out_data_path,
            materialized_request.out_checkpoint_path,
        )
        .await
    }
}

struct MaterializedEngineRequest {
    engine_request: ExecuteQueryRequest,
    out_data_path: PathBuf,
    out_checkpoint_path: PathBuf,
    volumes: Vec<VolumeSpec>,
}
