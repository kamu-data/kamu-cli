// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::sync_service::DatasetNotFoundError;
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
    dataset_factory: Arc<dyn DatasetFactory>,
}

#[component(pub)]
impl SyncServiceImpl {
    pub fn new(
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        local_repo: Arc<dyn LocalDatasetRepository>,
        dataset_factory: Arc<dyn DatasetFactory>,
    ) -> Self {
        Self {
            remote_repo_reg,
            local_repo,
            dataset_factory,
        }
    }

    /// Implements "Simple Transfer Protocol" as described in ODF spec
    async fn sync_with_simple_transfer_protocol(
        &self,
        src: &dyn Dataset,
        src_ref: &DatasetRefAny,
        dst: &dyn Dataset,
        dst_ref: &DatasetRefAny,
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
        src_ref: &DatasetRefAny,
        dst: &dyn Dataset,
        _dst_ref: &DatasetRefAny,
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
            Err(GetRefError::NotFound(_)) => Err(DatasetNotFoundError {
                dataset_ref: src_ref.clone(),
            }
            .into()),
            Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        let dst_head = match dst_chain.get_ref(&BlockRef::Head).await {
            Ok(h) => Ok(Some(h)),
            Err(GetRefError::NotFound(_)) => Ok(None),
            Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
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
            Err(IterBlocksError::BlockNotFound(e)) => Err(CorruptedSourceError {
                message: "source metadata chain is broken".to_owned(),
                source: Some(e.into()),
            }
            .into()),
            Err(IterBlocksError::InvalidInterval(e)) => Err(DatasetsDivergedError {
                src_head: e.head,
                dst_head: e.tail,
            }
            .into()),
            Err(IterBlocksError::Internal(e)) => Err(SyncError::Internal(e)),
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
                    Err(GetError::NotFound(e)) => Err(CorruptedSourceError {
                        message: "Source data file is missing".to_owned(),
                        source: Some(e.into()),
                    }
                    .into()),
                    Err(GetError::Internal(e)) => Err(SyncError::Internal(e)),
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
                    Err(InsertError::HashMismatch(e)) => Err(CorruptedSourceError {
                        message: "Data file hash declared by the source didn't match the computed - this may be an indication of hashing algorithm mismatch or an attempted tampering".to_owned(),
                        source: Some(e.into()),
                    }.into()),
                    Err(InsertError::Internal(e)) => Err(SyncError::Internal(e)),
                }?;
            }

            // Checkpoint
            if let Some(checkpoint) = block.event.output_checkpoint {
                info!(hash = ?checkpoint.physical_hash, "Transfering checkpoint file");

                let stream = match src_checkpoints.get_stream(&checkpoint.physical_hash).await {
                    Ok(s) => Ok(s),
                    Err(GetError::NotFound(e)) => Err(CorruptedSourceError {
                        message: "Source checkpoint file is missing".to_owned(),
                        source: Some(e.into()),
                    }
                    .into()),
                    Err(GetError::Internal(e)) => Err(SyncError::Internal(e)),
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
                    Err(InsertError::HashMismatch(e)) => Err(CorruptedSourceError {
                        message: "Checkpoint file hash declared by the source didn't match the computed - this may be an indication of hashing algorithm mismatch or an attempted tampering".to_owned(),
                        source: Some(e.into()),
                    }.into()),
                    Err(InsertError::Internal(e)) => Err(SyncError::Internal(e)),
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
                    Err(CorruptedSourceError {
                        message: "Block hash declared by the source didn't match the computed - this may be an indication of hashing algorithm mismatch or an attempted tampering".to_owned(),
                        source: Some(e.into()),
                    }.into())
                }
                Err(AppendError::InvalidBlock(e)) => {
                    Err(CorruptedSourceError {
                        message: "Source metadata chain is logically inconsistent".to_owned(),
                        source: Some(e.into()),
                    }.into())
                }
                Err(AppendError::Internal(e)) => Err(SyncError::Internal(e.into())),
                Err(AppendError::RefNotFound(_) | AppendError::RefCASFailed(_)) => unreachable!(),
            }?;
        }

        // Update reference, atomically commiting the sync operation
        // Any failures before this point may result in dangling files but will keep the destination dataset in its original logical state
        match dst_chain
            .set_ref(
                &BlockRef::Head,
                &src_head,
                SetRefOpts {
                    validate_block_present: false,
                    check_ref_is: Some(dst_head.as_ref()),
                },
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(SetRefError::CASFailed(e)) => Err(SyncError::UpdatedConcurrently(e.into())),
            Err(SetRefError::Internal(e)) => Err(SyncError::Internal(e)),
            Err(SetRefError::BlockNotFound(_)) => unreachable!(),
        }?;

        Ok(SyncResult::Updated {
            old_head: dst_head,
            new_head: src_head,
            num_blocks,
        })
    }

    async fn get_remote_dataset(
        &self,
        dataset_ref: &DatasetRefAny,
        url: Url,
        ensure_exists: bool,
    ) -> Result<Arc<dyn Dataset>, SyncError> {
        let dataset = self.dataset_factory.get_dataset(url)?;

        if ensure_exists {
            match dataset.as_metadata_chain().get_ref(&BlockRef::Head).await {
                Ok(_) => Ok(()),
                Err(GetRefError::NotFound(_)) => Err(DatasetNotFoundError {
                    dataset_ref: dataset_ref.clone(),
                }
                .into()),
                Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
            }?;
        }

        Ok(dataset)
    }

    fn get_url_for_remote_dataset(&self, remote_ref: &DatasetRefRemote) -> Result<Url, SyncError> {
        // TODO: REMOTE ID
        let remote_name = match remote_ref {
            DatasetRefRemote::ID(_) => {
                unimplemented!("Syncing remote dataset by ID is not yet supported")
            }
            DatasetRefRemote::RemoteName(name) => name,
            DatasetRefRemote::RemoteHandle(hdl) => &hdl.name,
            DatasetRefRemote::Url(url) => {
                let mut dataset_url = url.as_ref().clone();
                dataset_url.ensure_trailing_slash();
                return Ok(dataset_url);
            }
        };

        let mut repo = self
            .remote_repo_reg
            .get_repository(remote_name.repository())
            .map_err(|e| match e {
                DomainError::DoesNotExist { .. } => SyncError::RepositoryDoesNotExist {
                    repo_name: remote_name.repository().clone(),
                },
                e => SyncError::Internal(e.into_internal_error()),
            })?;

        repo.url.ensure_trailing_slash();
        let dataset_url = repo
            .url
            .join(&format!("{}/", remote_name.as_name_with_owner()))
            .unwrap();

        Ok(dataset_url)
    }

    async fn sync_impl(
        &self,
        src: &DatasetRefAny,
        dst: &DatasetRefAny,
        opts: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        // Resolve source
        let (src_dataset, src_is_local) = if let Some(local_ref) = src.as_local_ref() {
            let dataset = self.local_repo.get_dataset(&local_ref).await?;
            (dataset, true)
        } else {
            let remote_ref = src.as_remote_ref().unwrap();
            let url = self.get_url_for_remote_dataset(&remote_ref)?;
            let dataset = self.get_remote_dataset(src, url, true).await?;
            (dataset, false)
        };

        // Resolve destination
        let dst_dataset_builder: Box<dyn DatasetBuilder> =
            if let Some(local_ref) = dst.as_local_ref() {
                if opts.create_if_not_exist {
                    Box::new(WrapperDatasetBuilder::new(
                        self.local_repo.get_or_create_dataset(&local_ref).await?,
                    ))
                } else {
                    Box::new(NullDatasetBuilder::new(
                        self.local_repo.get_dataset(&local_ref).await?,
                    ))
                }
            } else {
                let remote_ref = dst.as_remote_ref().unwrap();
                let url = self.get_url_for_remote_dataset(&remote_ref)?;
                let dataset = self
                    .get_remote_dataset(dst, url, !opts.create_if_not_exist)
                    .await?;
                Box::new(NullDatasetBuilder::new(dataset))
            };

        let validation = if opts.trust_source.unwrap_or(src_is_local) {
            AppendValidation::None
        } else {
            AppendValidation::Full
        };

        match self
            .sync_with_simple_transfer_protocol(
                src_dataset.as_ref(),
                src,
                dst_dataset_builder.as_dataset(),
                dst,
                validation,
                opts.trust_source.unwrap_or(src_is_local),
            )
            .await
        {
            Ok(r) => {
                dst_dataset_builder.finish().await?;
                Ok(r)
            }
            Err(e) => {
                dst_dataset_builder.discard().await?;
                Err(e)
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl SyncService for SyncServiceImpl {
    async fn sync(
        &self,
        src: &DatasetRefAny,
        dst: &DatasetRefAny,
        opts: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let listener = listener.unwrap_or(Arc::new(NullSyncListener));
        listener.begin();

        match self.sync_impl(src, dst, opts).await {
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
    async fn sync_multi(
        &self,
        src_dst: &mut dyn Iterator<Item = (DatasetRefAny, DatasetRefAny)>,
        opts: SyncOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<SyncResultMulti> {
        let mut results = Vec::new();

        for (src, dst) in src_dst {
            let listener = listener.as_ref().and_then(|l| l.begin_sync(&src, &dst));
            let result = self.sync(&src, &dst, opts.clone(), listener).await;
            results.push(SyncResultMulti { src, dst, result });
        }

        results
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
trait DatasetBuilder {
    fn as_dataset(&self) -> &dyn Dataset;
    async fn finish(&self) -> Result<(), CreateDatasetError>;
    async fn discard(&self) -> Result<(), InternalError>;
}

struct NullDatasetBuilder {
    dataset: Arc<dyn Dataset>,
}

impl NullDatasetBuilder {
    pub fn new(dataset: Arc<dyn Dataset>) -> Self {
        Self { dataset }
    }
}

#[async_trait::async_trait]
impl DatasetBuilder for NullDatasetBuilder {
    fn as_dataset(&self) -> &dyn Dataset {
        self.dataset.as_ref()
    }

    async fn finish(&self) -> Result<(), CreateDatasetError> {
        Ok(())
    }

    async fn discard(&self) -> Result<(), InternalError> {
        Ok(())
    }
}

struct WrapperDatasetBuilder {
    builder: Box<dyn crate::domain::DatasetBuilder>,
}

impl WrapperDatasetBuilder {
    fn new(builder: Box<dyn crate::domain::DatasetBuilder>) -> Self {
        Self { builder }
    }
}

#[async_trait::async_trait]
impl DatasetBuilder for WrapperDatasetBuilder {
    fn as_dataset(&self) -> &dyn Dataset {
        self.builder.as_dataset()
    }

    async fn finish(&self) -> Result<(), CreateDatasetError> {
        self.builder.finish().await?;
        Ok(())
    }

    async fn discard(&self) -> Result<(), InternalError> {
        self.builder.discard().await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

trait UrlExt {
    fn ensure_trailing_slash(&mut self);
}

impl UrlExt for Url {
    fn ensure_trailing_slash(&mut self) {
        if !self.path().ends_with('/') {
            self.set_path(&format!("{}/", self.path()));
        }
    }
}
