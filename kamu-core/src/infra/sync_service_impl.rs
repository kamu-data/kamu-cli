// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use crate::domain::*;
use opendatafabric::*;

use dill::*;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tracing::{debug, info, info_span};
use url::Url;

pub struct SyncServiceImpl {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    local_repo: Arc<dyn LocalDatasetRepository>,
}

#[component(pub)]
impl SyncServiceImpl {
    pub fn new(
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        local_repo: Arc<dyn LocalDatasetRepository>,
    ) -> Self {
        Self {
            remote_repo_reg,
            local_repo,
        }
    }

    /// Implements "Simple Transfer Protocol" as described in ODF spec
    async fn sync_with_simple_transfer_protocol(
        &self,
        src: &dyn Dataset,
        src_ref: DatasetRefAny,
        dst: &dyn Dataset,
        dst_ref: DatasetRefAny,
        validation: AppendValidation,
        trust_source_hashes: bool,
    ) -> Result<SyncResult, SyncError> {
        let span = info_span!("Dataset sync", %src_ref, %dst_ref);
        let _span_guard = span.enter();
        info!("Starting sync using Simple Transfer Protocol");

        let result = self
            .sync_with_simple_transfer_protocol_impl(
                src,
                src_ref,
                dst,
                dst_ref,
                validation,
                trust_source_hashes,
            )
            .await;
        info!(?result, "Sync completed");
        result
    }

    /// Implements "Simple Transfer Protocol" as described in ODF spec
    /// TODO: PERF: Parallelism opportunity for data and checkpoint downloads (need to ensure repos are Sync)
    async fn sync_with_simple_transfer_protocol_impl(
        &self,
        src: &dyn Dataset,
        src_ref: DatasetRefAny,
        dst: &dyn Dataset,
        _dst_ref: DatasetRefAny,
        validation: AppendValidation,
        trust_source_hashes: bool,
    ) -> Result<SyncResult, SyncError> {
        let src_chain = src.as_metadata_chain();
        let dst_chain = dst.as_metadata_chain();

        let src_data = src.as_data_repo();
        let dst_data = dst.as_data_repo();

        let src_checkpoints = src.as_checkpoint_repo();
        let dst_checkpoints = dst.as_checkpoint_repo();

        let src_head = match src_chain.get_ref(&BlockRef::Head).await {
            Ok(head) => Ok(head),
            Err(GetRefError::NotFound(_)) => Err(SyncError::SourceDatasetDoesNotExist {
                dataset_ref: src_ref,
            }),
            Err(GetRefError::Internal(e)) => Err(SyncError::InternalError(e.into())),
        }?;

        let dst_head = match dst_chain.get_ref(&BlockRef::Head).await {
            Ok(h) => Ok(Some(h)),
            Err(GetRefError::NotFound(_)) => Ok(None),
            Err(GetRefError::Internal(e)) => Err(SyncError::InternalError(e.into())),
        }?;

        info!(?src_head, ?dst_head, "Resolved heads");

        if Some(&src_head) == dst_head.as_ref() {
            return Ok(SyncResult::UpToDate);
        }

        // Download missing blocks
        let blocks = match src_chain
            .iter_blocks_interval(&src_head, dst_head.as_ref())
            .collect::<Result<Vec<_>, _>>()
            .await
        {
            Ok(v) => Ok(v),
            Err(IterBlocksError::BlockNotFound(e)) => Err(SyncError::Corrupted {
                message: "source metadata chain is broken".to_owned(),
                source: Some(e.into()),
            }),
            Err(IterBlocksError::InvalidInterval(e)) => Err(SyncError::DatasetsDiverged {
                src_head: e.head,
                dst_head: e.tail,
            }),
            Err(IterBlocksError::Internal(e)) => Err(SyncError::InternalError(e.into())),
        }?;

        let num_blocks = blocks.len();
        info!("Considering {} new blocks", blocks.len());

        // Download data and checkpoints
        for block in blocks
            .iter()
            .rev()
            .filter_map(|(_, b)| b.as_data_stream_block())
        {
            // Data
            if let Some(data_slice) = block.event.output_data {
                info!(hash = ?data_slice.physical_hash, "Transfering data file");

                let stream = match src_data.get_stream(&data_slice.physical_hash).await {
                    Ok(s) => Ok(s),
                    Err(GetError::NotFound(e)) => Err(SyncError::Corrupted {
                        message: "Source data file is missing".to_owned(),
                        source: Some(e.into()),
                    }),
                    Err(GetError::Internal(e)) => Err(SyncError::InternalError(e.into())),
                }?;

                match dst_data
                    .insert_stream(
                        stream,
                        InsertOpts {
                            precomputed_hash: if !trust_source_hashes { None } else { Some(&data_slice.physical_hash) },
                            expected_hash: Some(&data_slice.physical_hash),
                            size_hint: Some(data_slice.size as usize),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(InsertError::HashMismatch(e)) => Err(SyncError::Corrupted {
                        message: "Data file hash declared by the source didn't match the computed - this may be an indication of hashing algorithm mismatch or an attempted tampering".to_owned(),
                        source: Some(e.into()),
                    }),
                    Err(InsertError::Internal(e)) => Err(SyncError::InternalError(e.into())),
                }?;
            }

            // Checkpoint
            if let Some(checkpoint) = block.event.output_checkpoint {
                info!(hash = ?checkpoint.physical_hash, "Transfering checkpoint file");

                let stream = match src_checkpoints.get_stream(&checkpoint.physical_hash).await {
                    Ok(s) => Ok(s),
                    Err(GetError::NotFound(e)) => Err(SyncError::Corrupted {
                        message: "Source checkpoint file is missing".to_owned(),
                        source: Some(e.into()),
                    }),
                    Err(GetError::Internal(e)) => Err(SyncError::InternalError(e.into())),
                }?;

                match dst_checkpoints
                    .insert_stream(
                        stream,
                        InsertOpts {
                            precomputed_hash: if !trust_source_hashes { None } else { Some(&checkpoint.physical_hash) },
                            expected_hash: Some(&checkpoint.physical_hash),
                            size_hint: Some(checkpoint.size as usize),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(InsertError::HashMismatch(e)) => Err(SyncError::Corrupted {
                        message: "Checkpoint file hash declared by the source didn't match the computed - this may be an indication of hashing algorithm mismatch or an attempted tampering".to_owned(),
                        source: Some(e.into()),
                    }),
                    Err(InsertError::Internal(e)) => Err(SyncError::InternalError(e.into())),
                }?;
            }
        }

        // Commit blocks
        for (hash, block) in blocks.into_iter().rev() {
            debug!(?hash, "Appending block");

            match dst_chain.append(
                block,
                AppendOpts {
                    validation,
                    update_ref: None, // We will update head once, after sync is complete
                    precomputed_hash: if !trust_source_hashes { None } else { Some(&hash) },
                    expected_hash: Some(&hash),
                    ..Default::default()
                },
            ).await {
                Ok(_) => Ok(()),
                Err(AppendError::InvalidBlock(AppendValidationError::HashMismatch(e))) => {
                    Err(SyncError::Corrupted {
                        message: "Block hash declared by the source didn't match the computed - this may be an indication of hashing algorithm mismatch or an attempted tampering".to_owned(),
                        source: Some(e.into()),
                    })
                }
                Err(AppendError::InvalidBlock(e)) => {
                    Err(SyncError::Corrupted {
                        message: "Source metadata chain is logically inconsistent".to_owned(),
                        source: Some(e.into()),
                    })
                }
                Err(AppendError::Internal(e)) => Err(SyncError::InternalError(e.into())),
                Err(AppendError::RefNotFound(_) | AppendError::RefCASFailed(_)) => unreachable!(),
            }?;
        }

        // Update reference, atomically commiting the sync operation
        // Any failures before this point may result in dangling files but will keep the destination dataset in its original logical state
        dst_chain
            .set_ref(&BlockRef::Head, &src_head)
            .await
            .map_err(|e| SyncError::InternalError(e.into()))?;

        Ok(SyncResult::Updated {
            old_head: dst_head,
            new_head: src_head,
            num_blocks,
        })
    }

    fn get_remote_dataset(&self, url: Url) -> Result<Arc<dyn Dataset>, SyncError> {
        let ds: Arc<dyn Dataset> = match url.scheme() {
            "file" => Arc::new(
                DatasetRepoFactory::get_or_create_local_fs(url.to_file_path().unwrap())
                    .map_err(|e| SyncError::InternalError(e.into()))?,
            ),
            "http" => Arc::new(
                DatasetRepoFactory::get_http(url)
                    .map_err(|e| SyncError::InternalError(e.into()))?,
            ),
            "s3" | "s3+http" | "s3+https" => Arc::new(
                DatasetRepoFactory::get_s3(url).map_err(|e| SyncError::InternalError(e.into()))?,
            ),
            s => {
                return Err(SyncError::InternalError(
                    RepositoryFactoryError::unsupported_protocol(s).into(),
                ))
            }
        };
        Ok(ds)
    }

    async fn do_sync_from(
        &self,
        remote_ref: &DatasetRefRemote,
        local_name: &DatasetName,
        _options: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        // TODO: REMOTE ID
        let remote_name = match remote_ref {
            DatasetRefRemote::ID(_) => {
                unimplemented!("Syncing remote dataset by ID is not yet supported")
            }
            DatasetRefRemote::RemoteName(name) => name,
            DatasetRefRemote::RemoteHandle(hdl) => &hdl.name,
            DatasetRefRemote::Url(_) => {
                unimplemented!("Syncing remote dataset by Url is not yet supported")
            }
        };

        let local_dataset = match self
            .local_repo
            .get_or_create_dataset(&local_name.as_local_ref())
            .await
        {
            Ok(ds) => Ok(ds),
            Err(GetDatasetError::NotFound(e)) => Err(SyncError::DestinationDatasetDoesNotExist {
                dataset_ref: e.dataset_ref.into(),
            }),
            Err(GetDatasetError::Internal(e)) => Err(SyncError::InternalError(e.into())),
        }?;

        let repo = self
            .remote_repo_reg
            .get_repository(remote_name.repository())
            .map_err(|e| match e {
                DomainError::DoesNotExist { .. } => SyncError::RepositoryDoesNotExist {
                    repo_name: remote_name.repository().clone(),
                },
                _ => SyncError::InternalError(e.into()),
            })?;

        assert!(
            repo.url.path().ends_with('/'),
            "Repo url does not have a trailing slash: {}",
            repo.url
        );
        let remote_dataset_url = repo
            .url
            .join(&format!("{}/", remote_name.as_name_with_owner()))
            .unwrap();

        let remote_dataset = self.get_remote_dataset(remote_dataset_url)?;

        match self
            .sync_with_simple_transfer_protocol(
                remote_dataset.as_ref(),
                remote_ref.into(),
                local_dataset.as_dataset(),
                local_name.into(),
                AppendValidation::Full,
                false,
            )
            .await
        {
            Ok(res) => match local_dataset.finish().await {
                Ok(_) => Ok(res),
                Err(e) => Err(SyncError::InternalError(e.into())),
            },
            Err(e) => {
                local_dataset
                    .discard()
                    .await
                    .map_err(|e| SyncError::InternalError(e.into()))?;
                Err(e)
            }
        }
    }

    async fn do_sync_to(
        &self,
        local_ref: &DatasetRefLocal,
        remote_name: &RemoteDatasetName,
        _options: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        let local_dataset = match self.local_repo.get_dataset(local_ref).await {
            Ok(ds) => Ok(ds),
            Err(GetDatasetError::NotFound(_)) => Err(SyncError::SourceDatasetDoesNotExist {
                dataset_ref: local_ref.into(),
            }),
            Err(GetDatasetError::Internal(e)) => Err(SyncError::InternalError(e.into())),
        }?;

        let repo = self
            .remote_repo_reg
            .get_repository(remote_name.repository())
            .map_err(|e| match e {
                DomainError::DoesNotExist { .. } => SyncError::RepositoryDoesNotExist {
                    repo_name: remote_name.repository().clone(),
                },
                _ => SyncError::InternalError(e.into()),
            })?;

        let remote_dataset_url = repo
            .url
            .join(&format!("{}/", remote_name.as_name_with_owner()))
            .unwrap();

        assert!(
            repo.url.path().ends_with('/'),
            "Repo url does not have a trailing slash: {}",
            repo.url
        );
        let remote_dataset = self.get_remote_dataset(remote_dataset_url)?;

        self.sync_with_simple_transfer_protocol(
            local_dataset.as_ref(),
            local_ref.into(),
            remote_dataset.as_ref(),
            remote_name.into(),
            AppendValidation::None,
            true,
        )
        .await
    }
}

#[async_trait::async_trait(?Send)]
impl SyncService for SyncServiceImpl {
    async fn sync_from(
        &self,
        remote_ref: &DatasetRefRemote,
        local_name: &DatasetName,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let listener = listener.unwrap_or(Arc::new(NullSyncListener));
        listener.begin();

        match self.do_sync_from(remote_ref, local_name, options).await {
            Ok(result) => {
                listener.success(&result);
                Ok(result)
            }
            Err(err) => {
                listener.error(&err);
                Err(err)
            }
        }
    }

    // TODO: Parallelism
    async fn sync_from_multi(
        &self,
        datasets: &mut dyn Iterator<Item = (DatasetRefRemote, DatasetName)>,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<(
        (DatasetRefRemote, DatasetName),
        Result<SyncResult, SyncError>,
    )> {
        let mut results = Vec::new();

        for (remote_ref, local_name) in datasets {
            let res = self
                .sync_from(
                    &remote_ref,
                    &local_name,
                    options.clone(),
                    listener
                        .as_ref()
                        .and_then(|l| l.begin_sync(&local_name.as_local_ref(), &remote_ref)),
                )
                .await;

            results.push(((remote_ref, local_name), res));
        }

        results
    }

    async fn sync_to(
        &self,
        local_ref: &DatasetRefLocal,
        remote_name: &RemoteDatasetName,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let listener = listener.unwrap_or(Arc::new(NullSyncListener));
        listener.begin();

        match self.do_sync_to(local_ref, remote_name, options).await {
            Ok(result) => {
                listener.success(&result);
                Ok(result)
            }
            Err(err) => {
                listener.error(&err);
                Err(err)
            }
        }
    }

    // TODO: Parallelism
    async fn sync_to_multi(
        &self,
        datasets: &mut dyn Iterator<Item = (DatasetRefLocal, RemoteDatasetName)>,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<(
        (DatasetRefLocal, RemoteDatasetName),
        Result<SyncResult, SyncError>,
    )> {
        let mut results = Vec::new();

        for (local_ref, remote_name) in datasets {
            let res = self
                .sync_to(
                    &local_ref,
                    &remote_name,
                    options.clone(),
                    listener
                        .as_ref()
                        .and_then(|l| l.begin_sync(&local_ref, &remote_name.as_remote_ref())),
                )
                .await;

            results.push(((local_ref, remote_name), res));
        }

        results
    }
}
