// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use container_runtime::*;
use opendatafabric::*;

use crate::domain::*;
use crate::ObjectRepositoryLocalFS;

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait EngineIoStrategy: Send + Sync {
    async fn materialize_request(
        &self,
        dataset: &dyn Dataset,
        request: TransformRequest,
        operation_dir: &Path,
    ) -> Result<MaterializedEngineRequest, InternalError>;
}

///////////////////////////////////////////////////////////////////////////////

pub struct MaterializedEngineRequest {
    pub engine_request: ExecuteQueryRequest,
    pub out_data_path: PathBuf,
    pub out_checkpoint_path: PathBuf,
    pub volumes: Vec<VolumeSpec>,
}

///////////////////////////////////////////////////////////////////////////////
// Local Volume
///////////////////////////////////////////////////////////////////////////////

/// This IO strategy materializes all inputs as local file system files and pass
/// them to the engines via mounted volumes.
pub struct EngineIoStrategyLocalVolume {
    dataset_repo: Arc<dyn DatasetRepository>,
}

impl EngineIoStrategyLocalVolume {
    pub fn new(dataset_repo: Arc<dyn DatasetRepository>) -> Self {
        Self { dataset_repo }
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
}

#[async_trait::async_trait]
impl EngineIoStrategy for EngineIoStrategyLocalVolume {
    #[tracing::instrument(skip_all)]
    async fn materialize_request(
        &self,
        dataset: &dyn Dataset,
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
}

///////////////////////////////////////////////////////////////////////////////
// Remote Proxy
///////////////////////////////////////////////////////////////////////////////

/// This IO strategy is used for engines that cannot work directly with remote
/// storage. It will download the input data and checkpoint locally and mount it
/// as files.
pub struct EngineIoStrategyRemoteProxy {
    dataset_repo: Arc<dyn DatasetRepository>,
}

impl EngineIoStrategyRemoteProxy {
    pub fn new(dataset_repo: Arc<dyn DatasetRepository>) -> Self {
        Self { dataset_repo }
    }

    async fn materialize_object(
        &self,
        repo: &dyn ObjectRepository,
        hash: &Multihash,
        host_in_dir: &Path,
        container_in_dir: &Path,
        volumes: &mut Vec<VolumeSpec>,
    ) -> Result<PathBuf, InternalError> {
        let tmp_repo =
            ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(host_in_dir.to_path_buf());

        let stream = repo.get_stream(hash).await.int_err()?;

        tmp_repo
            .insert_stream(
                stream,
                InsertOpts {
                    precomputed_hash: Some(hash),
                    expected_hash: Some(hash),
                    size_hint: None,
                },
            )
            .await
            .int_err()?;

        let host_path = host_in_dir.join(hash.to_string());
        let container_path = container_in_dir.join(hash.to_string());

        volumes.push((host_path, container_path.clone(), VolumeAccess::ReadOnly).into());
        Ok(container_path)
    }

    async fn maybe_materialize_object(
        &self,
        repo: &dyn ObjectRepository,
        hash: Option<&Multihash>,
        host_in_dir: &Path,
        container_in_dir: &Path,
        volumes: &mut Vec<VolumeSpec>,
    ) -> Result<Option<PathBuf>, InternalError> {
        if let Some(hash) = hash {
            Ok(Some(
                self.materialize_object(repo, hash, host_in_dir, container_in_dir, volumes)
                    .await?,
            ))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
impl EngineIoStrategy for EngineIoStrategyRemoteProxy {
    #[tracing::instrument(skip_all)]
    async fn materialize_request(
        &self,
        dataset: &dyn Dataset,
        request: TransformRequest,
        operation_dir: &Path,
    ) -> Result<MaterializedEngineRequest, InternalError> {
        // TODO: PERF: Parallel data transfer
        let host_in_dir = operation_dir.join("in");
        let host_out_dir = operation_dir.join("out");
        let host_out_data_path = host_out_dir.join("data");
        let host_out_checkpoint_path = host_out_dir.join("checkpoint");
        std::fs::create_dir(&host_in_dir).int_err()?;
        std::fs::create_dir(&host_out_dir).int_err()?;

        let container_in_dir = PathBuf::from("/opt/engine/in");
        let container_out_dir = PathBuf::from("/opt/engine/out");
        let container_out_data_path = PathBuf::from("/opt/engine/out/data");
        let container_out_checkpoint_path = PathBuf::from("/opt/engine/out/checkpoint");

        let mut volumes = vec![(host_out_dir, container_out_dir, VolumeAccess::ReadWrite).into()];

        let prev_checkpoint_path = self
            .maybe_materialize_object(
                dataset.as_checkpoint_repo(),
                request.prev_checkpoint.as_ref(),
                &host_in_dir,
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
                        &host_in_dir,
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
                    &host_in_dir,
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
}

///////////////////////////////////////////////////////////////////////////////
