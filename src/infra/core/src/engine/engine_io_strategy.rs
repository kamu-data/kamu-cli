// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use container_runtime::*;
use datafusion::arrow::datatypes::SchemaRef;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::engine::*;
use kamu_datasets::ResolvedDatasetsMap;
use odf::storage::lfs::ObjectRepositoryLocalFSSha3;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait EngineIoStrategy: Send + Sync {
    async fn materialize_request(
        &self,
        request: TransformRequestExt,
        datasets_map: &ResolvedDatasetsMap,
        operation_dir: &Path,
    ) -> Result<MaterializedEngineRequest, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MaterializedEngineRequest {
    pub engine_request: odf::metadata::TransformRequest,
    pub out_data_path: PathBuf,
    pub out_checkpoint_path: PathBuf,
    pub volumes: Vec<VolumeSpec>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Local Volume
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This IO strategy materializes all inputs as local file system files and pass
/// them to the engines via mounted volumes.
pub struct EngineIoStrategyLocalVolume {}

impl EngineIoStrategyLocalVolume {
    async fn materialize_object(
        &self,
        repo: &dyn odf::storage::ObjectRepository,
        hash: &odf::Multihash,
        container_in_dir: &Path,
        volumes: &mut Vec<VolumeSpec>,
    ) -> Result<PathBuf, InternalError> {
        let url = repo.get_internal_url(hash).await;
        let host_path = odf::utils::data::local_url::into_local_path(url).int_err()?;
        let container_path = container_in_dir.join(hash.to_string());
        volumes.push((host_path, container_path.clone(), VolumeAccess::ReadOnly).into());
        Ok(container_path)
    }

    async fn maybe_materialize_object(
        &self,
        repo: &dyn odf::storage::ObjectRepository,
        hash: Option<&odf::Multihash>,
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
        request: TransformRequestExt,
        datasets_map: &ResolvedDatasetsMap,
        operation_dir: &Path,
    ) -> Result<MaterializedEngineRequest, InternalError> {
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

        let target = datasets_map.get_by_handle(&request.dataset_handle);

        let prev_checkpoint_path = self
            .maybe_materialize_object(
                target.as_checkpoint_repo(),
                request.prev_checkpoint.as_ref(),
                &container_in_dir,
                &mut volumes,
            )
            .await?;

        let mut query_inputs = Vec::new();
        for input in request.inputs {
            let input_resolved = datasets_map.get_by_handle(&input.dataset_handle);

            let mut data_paths = Vec::new();
            for hash in input.data_slices {
                let container_path = self
                    .materialize_object(
                        input_resolved.as_data_repo(),
                        &hash,
                        &container_in_dir,
                        &mut volumes,
                    )
                    .await?;

                data_paths.push(container_path);
            }

            let offset_interval = if let Some(new_offset) = input.new_offset {
                Some(odf::metadata::OffsetInterval {
                    start: input.prev_offset.map_or(0, |v| v + 1),
                    end: new_offset,
                })
            } else {
                None
            };

            let schema_file = {
                // FIXME: The .parquet extension is currently necessary for DataFusion to
                // respect the single-file output
                // See: https://github.com/apache/datafusion/issues/13323
                let name = format!("schema-{}.parquet", input.dataset_handle.id.as_multibase());
                let host_path = host_in_dir.join(&name);
                let container_path = container_in_dir.join(&name);
                write_schema_file(&input.schema, &host_path).await?;

                volumes.push((host_path, container_path.clone(), VolumeAccess::ReadOnly).into());
                container_path
            };

            query_inputs.push(odf::metadata::TransformRequestInput {
                dataset_id: input.dataset_handle.id,
                dataset_alias: input.dataset_handle.alias,
                query_alias: input.alias,
                vocab: input.vocab,
                offset_interval,
                data_paths,
                schema_file,
                explicit_watermarks: input.explicit_watermarks,
            });
        }

        let engine_request = odf::metadata::TransformRequest {
            dataset_id: request.dataset_handle.id,
            dataset_alias: request.dataset_handle.alias,
            system_time: request.system_time,
            next_offset: request.prev_offset.map_or(0, |v| v + 1),
            vocab: request.vocab,
            transform: request.transform,
            query_inputs,
            prev_checkpoint_path,
            new_checkpoint_path: container_out_checkpoint_path,
            new_data_path: container_out_data_path,
        };

        Ok(MaterializedEngineRequest {
            engine_request,
            out_data_path: host_out_data_path,
            out_checkpoint_path: host_out_checkpoint_path,
            volumes,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Remote Proxy
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This IO strategy is used for engines that cannot work directly with remote
/// storage. It will download the input data and checkpoint locally and mount it
/// as files.
pub struct EngineIoStrategyRemoteProxy {}

impl EngineIoStrategyRemoteProxy {
    async fn materialize_object(
        &self,
        repo: &dyn odf::storage::ObjectRepository,
        hash: &odf::Multihash,
        host_in_dir: &Path,
        container_in_dir: &Path,
        volumes: &mut Vec<VolumeSpec>,
    ) -> Result<PathBuf, InternalError> {
        let tmp_repo = ObjectRepositoryLocalFSSha3::new(host_in_dir.to_path_buf());
        let stream = repo.get_stream(hash).await.int_err()?;

        use odf::storage::ObjectRepository;
        tmp_repo
            .insert_stream(
                stream,
                odf::storage::InsertOpts {
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
        repo: &dyn odf::storage::ObjectRepository,
        hash: Option<&odf::Multihash>,
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
        request: TransformRequestExt,
        datasets_map: &ResolvedDatasetsMap,
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

        let target = datasets_map.get_by_handle(&request.dataset_handle);

        let prev_checkpoint_path = self
            .maybe_materialize_object(
                target.as_checkpoint_repo(),
                request.prev_checkpoint.as_ref(),
                &host_in_dir,
                &container_in_dir,
                &mut volumes,
            )
            .await?;

        let mut query_inputs = Vec::new();
        for input in request.inputs {
            let input_resolved = datasets_map.get_by_handle(&input.dataset_handle);

            let mut data_paths = Vec::new();
            for hash in input.data_slices {
                let container_path = self
                    .materialize_object(
                        input_resolved.as_data_repo(),
                        &hash,
                        &host_in_dir,
                        &container_in_dir,
                        &mut volumes,
                    )
                    .await?;

                data_paths.push(container_path);
            }

            let offset_interval = if let Some(new_offset) = input.new_offset {
                Some(odf::metadata::OffsetInterval {
                    start: input.prev_offset.map_or(0, |v| v + 1),
                    end: new_offset,
                })
            } else {
                None
            };

            let schema_file = {
                // FIXME: The .parquet extension is currently necessary for DataFusion to
                // respect the single-file output
                // See: https://github.com/apache/datafusion/issues/13323
                let name = format!("schema-{}.parquet", input.dataset_handle.id.as_multibase());
                let host_path = host_in_dir.join(&name);
                let container_path = container_in_dir.join(&name);
                write_schema_file(&input.schema, &host_path).await?;

                volumes.push((host_path, container_path.clone(), VolumeAccess::ReadOnly).into());
                container_path
            };

            query_inputs.push(odf::metadata::TransformRequestInput {
                dataset_id: input.dataset_handle.id,
                dataset_alias: input.dataset_handle.alias,
                query_alias: input.alias,
                vocab: input.vocab,
                offset_interval,
                data_paths,
                schema_file,
                explicit_watermarks: input.explicit_watermarks,
            });
        }

        let engine_request = odf::metadata::TransformRequest {
            dataset_id: request.dataset_handle.id,
            dataset_alias: request.dataset_handle.alias,
            system_time: request.system_time,
            next_offset: request.prev_offset.map_or(0, |v| v + 1),
            vocab: request.vocab,
            transform: request.transform,
            query_inputs,
            prev_checkpoint_path,
            new_checkpoint_path: container_out_checkpoint_path,
            new_data_path: container_out_data_path,
        };

        Ok(MaterializedEngineRequest {
            engine_request,
            out_data_path: host_out_data_path,
            out_checkpoint_path: host_out_checkpoint_path,
            volumes,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Remove in favor of passing serialized schema
async fn write_schema_file(schema: &SchemaRef, path: &Path) -> Result<(), InternalError> {
    use datafusion::prelude::*;

    // FIXME: The  extension is currently necessary for DataFusion to
    // respect the single-file output
    // See: https://github.com/apache/datafusion/issues/13323
    assert!(
        path.extension().is_some(),
        "Ouput file name must have an extension"
    );

    let ctx = SessionContext::new();
    let df = ctx
        .read_batch(datafusion::arrow::array::RecordBatch::new_empty(
            schema.clone(),
        ))
        .int_err()?;

    // TODO: Keep in sync with `DataWriterDataFusion`
    df.write_parquet(
        path.to_str().unwrap(),
        datafusion::dataframe::DataFrameWriteOptions::new().with_single_file_output(true),
        Some(datafusion::config::TableParquetOptions {
            global: datafusion::config::ParquetOptions {
                writer_version: "1.0".into(),
                compression: Some("snappy".into()),
                ..Default::default()
            },
            ..Default::default()
        }),
    )
    .await
    .int_err()?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
