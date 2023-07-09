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
use odf::{ExecuteQueryInput, ExecuteQueryRequest, Multihash};
use opendatafabric as odf;

use super::engine_container::{EngineContainer, LogsConfig};
use super::ODFEngineConfig;
use crate::WorkspaceLayout;

pub struct ODFEngine {
    container_runtime: ContainerRuntime,
    engine_config: ODFEngineConfig,
    image: String,
    workspace_layout: Arc<WorkspaceLayout>,
    dataset_repo: Arc<dyn DatasetRepository>,
}

impl ODFEngine {
    pub fn new(
        container_runtime: ContainerRuntime,
        engine_config: ODFEngineConfig,
        image: &str,
        workspace_layout: Arc<WorkspaceLayout>,
        dataset_repo: Arc<dyn DatasetRepository>,
    ) -> Self {
        Self {
            container_runtime,
            engine_config,
            image: image.to_owned(),
            workspace_layout,
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

    fn workspace_dir_in_container(&self) -> PathBuf {
        PathBuf::from("/opt/engine/workspace")
    }

    fn to_container_path(&self, host_path: &Path) -> PathBuf {
        assert!(host_path.is_absolute());
        assert!(self.workspace_layout.root_dir.is_absolute());
        let rel = host_path
            .strip_prefix(&self.workspace_layout.root_dir)
            .unwrap();
        let joined = self.workspace_dir_in_container().join(rel);
        let unix_path = joined.to_str().unwrap().replace("\\", "/");
        PathBuf::from(unix_path)
    }

    async fn map_object_to_container_path(
        &self,
        repo: &dyn ObjectRepository,
        hash: &Multihash,
    ) -> Result<PathBuf, InternalError> {
        let url = repo.get_internal_url(hash).await;
        let host_path = kamu_data_utils::data::local_url::into_local_path(url).int_err()?;
        Ok(self.to_container_path(&host_path))
    }

    async fn maybe_map_object_to_container_path(
        &self,
        repo: &dyn ObjectRepository,
        hash: Option<&Multihash>,
    ) -> Result<Option<PathBuf>, InternalError> {
        if let Some(hash) = hash {
            Ok(Some(self.map_object_to_container_path(repo, hash).await?))
        } else {
            Ok(None)
        }
    }

    async fn materialize_request(
        &self,
        request: TransformRequest,
        out_data_path: &Path,
        new_checkpoint_path: &Path,
    ) -> Result<ExecuteQueryRequest, InternalError> {
        let dataset = self
            .dataset_repo
            .get_dataset(&request.dataset_handle.as_local_ref())
            .await
            .int_err()?;

        let prev_checkpoint_path = self
            .maybe_map_object_to_container_path(
                dataset.as_checkpoint_repo(),
                request.prev_checkpoint.as_ref(),
            )
            .await?;

        let mut inputs = Vec::new();
        for input in request.inputs {
            let input_dataset = self
                .dataset_repo
                .get_dataset(&input.dataset_handle.as_local_ref())
                .await
                .int_err()?;

            let mut data_paths = Vec::new();
            for hash in input.data_slices {
                data_paths.push(
                    self.map_object_to_container_path(input_dataset.as_data_repo(), &hash)
                        .await?,
                );
            }

            inputs.push(ExecuteQueryInput {
                dataset_id: input.dataset_handle.id,
                dataset_name: input.dataset_handle.alias.dataset_name,
                vocab: input.vocab,
                data_interval: input.data_interval,
                data_paths,
                schema_file: self
                    .map_object_to_container_path(input_dataset.as_data_repo(), &input.schema_slice)
                    .await?,
                explicit_watermarks: input.explicit_watermarks,
            })
        }

        let engine_request = ExecuteQueryRequest {
            dataset_id: request.dataset_handle.id,
            dataset_name: request.dataset_handle.alias.dataset_name,
            system_time: request.system_time,
            offset: request.offset,
            vocab: request.vocab,
            transform: request.transform,
            inputs,
            prev_checkpoint_path,
            new_checkpoint_path: self.to_container_path(new_checkpoint_path),
            out_data_path: self.to_container_path(out_data_path),
        };

        Ok(engine_request)
    }
}

#[async_trait::async_trait]
impl Engine for ODFEngine {
    async fn transform(&self, request: TransformRequest) -> Result<TransformResponse, EngineError> {
        let out_data_path = self
            .workspace_layout
            .run_info_dir
            .join(crate::repos::get_staging_name());

        let new_checkpoint_path = self
            .workspace_layout
            .run_info_dir
            .join(crate::repos::get_staging_name());

        let engine_request = self
            .materialize_request(request, &out_data_path, &new_checkpoint_path)
            .await
            .map_err(|e| EngineError::internal(e, Vec::new()))?;

        let engine_container = EngineContainer::new(
            self.container_runtime.clone(),
            self.engine_config.clone(),
            LogsConfig::new(&self.workspace_layout.run_info_dir),
            &self.image,
            // TODO: Avoid giving access to the entire workspace data
            // TODO: Use read-only permissions where possible
            vec![(
                self.workspace_layout.root_dir.clone(),
                self.workspace_dir_in_container(),
            )],
        )
        .await?;

        let mut engine_client = engine_container.connect_client().await?;

        let res = self
            .execute_query(&engine_container, &mut engine_client, engine_request)
            .await;

        engine_container.terminate().await?;

        let engine_response = res?;

        let out_data = if engine_response.data_interval.is_some() {
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

        Ok(TransformResponse {
            data_interval: engine_response.data_interval,
            output_watermark: engine_response.output_watermark,
            new_checkpoint,
            out_data,
        })
    }
}
