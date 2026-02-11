// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use chrono::Utc;
use file_utils::OwnedFile;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use odf_dataset::*;
use odf_metadata::*;
use odf_storage::*;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetImpl<MetaChain, DataRepo, CheckpointRepo, InfoRepo> {
    metadata_chain: MetaChain,
    data_repo: DataRepo,
    checkpoint_repo: CheckpointRepo,
    info_repo: InfoRepo,
    storage_internal_url: Url,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<MetaChain, DataRepo, CheckpointRepo, InfoRepo>
    DatasetImpl<MetaChain, DataRepo, CheckpointRepo, InfoRepo>
where
    MetaChain: MetadataChain + Sync + Send + 'static,
    DataRepo: ObjectRepository + Sync + Send,
    CheckpointRepo: ObjectRepository + Sync + Send,
    InfoRepo: NamedObjectRepository + Sync + Send,
{
    pub fn new(
        metadata_chain: MetaChain,
        data_repo: DataRepo,
        checkpoint_repo: CheckpointRepo,
        info_repo: InfoRepo,
        storage_internal_url: Url,
    ) -> Self {
        Self {
            metadata_chain,
            data_repo,
            checkpoint_repo,
            info_repo,
            storage_internal_url,
        }
    }

    async fn prepare_objects(
        &self,
        offset_interval: Option<OffsetInterval>,
        data: Option<&OwnedFile>,
        checkpoint: Option<&CheckpointRef>,
    ) -> Result<(Option<DataSlice>, Option<Checkpoint>), InternalError> {
        let data_slice = if let Some(offset_interval) = offset_interval {
            let data = data.unwrap();
            let path = data.as_path().to_path_buf();
            let logical_hash = tokio::task::spawn_blocking(move || {
                odf_data_utils::data::hash::get_parquet_logical_hash(&path)
            })
            .await
            .int_err()?
            .int_err()?;

            let path = data.as_path().to_path_buf();
            let physical_hash = tokio::task::spawn_blocking(move || {
                odf_data_utils::data::hash::get_file_physical_hash(&path)
            })
            .await
            .int_err()?
            .int_err()?;

            Some(DataSlice {
                logical_hash,
                physical_hash,
                offset_interval,
                size: std::fs::metadata(data.as_path()).int_err()?.len(),
            })
        } else {
            assert!(data.is_none());
            None
        };

        let checkpoint = if let Some(checkpoint) = checkpoint {
            match checkpoint {
                CheckpointRef::Existed(hash) => {
                    let size = self.as_checkpoint_repo().get_size(hash).await.int_err()?;
                    Some(Checkpoint {
                        physical_hash: hash.clone(),
                        size,
                    })
                }
                CheckpointRef::New(checkpoint_file) => {
                    let path = checkpoint_file.as_path().to_path_buf();
                    let physical_hash = tokio::task::spawn_blocking(move || {
                        odf_data_utils::data::hash::get_file_physical_hash(&path)
                    })
                    .await
                    .int_err()?
                    .int_err()?;

                    Some(Checkpoint {
                        physical_hash,
                        size: std::fs::metadata(checkpoint_file.as_path())
                            .int_err()?
                            .len(),
                    })
                }
            }
        } else {
            None
        };

        Ok((data_slice, checkpoint))
    }

    async fn commit_objects(
        &self,
        data_slice: Option<&DataSlice>,
        data: Option<OwnedFile>,
        checkpoint_meta: Option<&Checkpoint>,
        checkpoint: Option<OwnedFile>,
    ) -> Result<(), InternalError> {
        if let Some(data_slice) = data_slice {
            self.as_data_repo()
                .insert_file_move(
                    &data.unwrap().into_inner(),
                    InsertOpts {
                        precomputed_hash: Some(&data_slice.physical_hash),
                        expected_hash: None,
                        size_hint: Some(data_slice.size),
                    },
                )
                .await
                .int_err()?;
        }

        if let Some(checkpoint_meta) = checkpoint_meta
            && let Some(checkpoint) = checkpoint
        {
            self.as_checkpoint_repo()
                .insert_file_move(
                    &checkpoint.into_inner(),
                    InsertOpts {
                        precomputed_hash: Some(&checkpoint_meta.physical_hash),
                        expected_hash: None,
                        size_hint: Some(checkpoint_meta.size),
                    },
                )
                .await
                .int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<MetaChain, DataRepo, CheckpointRepo, InfoRepo> Dataset
    for DatasetImpl<MetaChain, DataRepo, CheckpointRepo, InfoRepo>
where
    MetaChain: MetadataChain + Sync + Send + 'static,
    DataRepo: ObjectRepository + Sync + Send,
    CheckpointRepo: ObjectRepository + Sync + Send,
    InfoRepo: NamedObjectRepository + Sync + Send,
{
    /// Detaches this metadata chain from any transaction references
    fn detach_from_transaction(&self) {
        self.metadata_chain.detach_from_transaction();
    }

    /// Helper function to append a generic event to metadata chain.
    ///
    /// Warning: Don't use when synchronizing blocks from another dataset.
    async fn commit_event<'a>(
        &self,
        event: MetadataEvent,
        opts: CommitOpts<'a>,
    ) -> Result<CommitResult, CommitError> {
        // Validate referential consistency
        if opts.check_object_refs
            && let Some(event) = event.as_data_stream_event()
        {
            if let Some(data_slice) = event.new_data
                && !self
                    .as_data_repo()
                    .contains(&data_slice.physical_hash)
                    .await
                    .int_err()?
            {
                return Err(ObjectNotFoundError {
                    hash: data_slice.physical_hash.clone(),
                }
                .into());
            }
            if let Some(checkpoint) = event.new_checkpoint
                && !self
                    .as_checkpoint_repo()
                    .contains(&checkpoint.physical_hash)
                    .await
                    .int_err()?
            {
                return Err(ObjectNotFoundError {
                    hash: checkpoint.physical_hash.clone(),
                }
                .into());
            }
        }

        let chain = self.as_metadata_chain();

        let prev_block_hash = if let Some(prev_block_hash) = opts.prev_block_hash {
            prev_block_hash.cloned()
        } else {
            match chain.resolve_ref(opts.block_ref).await {
                Ok(h) => Some(h),
                Err(GetRefError::NotFound(_)) => None,
                Err(e) => return Err(e.int_err().into()),
            }
        };

        let sequence_number = if let Some(prev_block_hash) = &prev_block_hash {
            chain
                .get_block(prev_block_hash)
                .await
                .int_err()?
                .sequence_number
                + 1
        } else {
            0
        };

        let block = MetadataBlock {
            prev_block_hash: prev_block_hash.clone(),
            sequence_number,
            system_time: opts.system_time.unwrap_or_else(Utc::now),
            event,
        };

        tracing::info!(?block, "Committing new block");

        let append_opts = if !opts.update_block_ref {
            AppendOpts {
                update_ref: None,
                check_ref_is_prev_block: false,
                ..AppendOpts::default()
            }
        } else {
            AppendOpts::default()
        };

        let new_head = chain.append(block, append_opts).await?;

        tracing::info!(%new_head, "Committed new block");

        Ok(CommitResult {
            old_head: prev_block_hash,
            new_head,
        })
    }

    /// Helper function to commit `AddData` event into a local dataset.
    ///
    /// Will attempt to atomically move data and checkpoint files, so those have
    /// to be on the same file system as the workspace.
    async fn commit_add_data<'a>(
        &self,
        params: AddDataParams,
        data_file: Option<OwnedFile>,
        checkpoint: Option<CheckpointRef>,
        opts: CommitOpts<'a>,
    ) -> Result<CommitResult, CommitError> {
        let (new_data, new_checkpoint) = self
            .prepare_objects(
                params.new_offset_interval,
                data_file.as_ref(),
                checkpoint.as_ref(),
            )
            .await?;

        self.commit_objects(
            new_data.as_ref(),
            data_file,
            new_checkpoint.as_ref(),
            checkpoint.and_then(std::convert::Into::into),
        )
        .await?;

        let metadata_event = AddData {
            prev_checkpoint: params.prev_checkpoint,
            prev_offset: params.prev_offset,
            new_data,
            new_checkpoint,
            new_watermark: params.new_watermark,
            new_source_state: params.new_source_state,
            extra: params
                .new_linked_objects
                .map(|v| ExtraAttributes::new().with(v)),
        };

        self.commit_event(
            metadata_event.into(),
            CommitOpts {
                check_object_refs: false, // We just added all objects
                ..opts
            },
        )
        .await
    }

    /// Helper function to commit `ExecuteTransform` event into a local dataset.
    ///
    /// Will attempt to atomically move data and checkpoint files, so those have
    /// to be on the same file system as the workspace.
    async fn commit_execute_transform<'a>(
        &self,
        execute_transform: ExecuteTransformParams,
        data: Option<OwnedFile>,
        checkpoint: Option<CheckpointRef>,
        opts: CommitOpts<'a>,
    ) -> Result<CommitResult, CommitError> {
        let event = self
            .prepare_execute_transform(execute_transform, data.as_ref(), checkpoint.as_ref())
            .await?;

        self.commit_objects(
            event.new_data.as_ref(),
            data,
            event.new_checkpoint.as_ref(),
            checkpoint.and_then(std::convert::Into::into),
        )
        .await?;

        self.commit_event(
            event.into(),
            CommitOpts {
                check_object_refs: false, // We just added all objects
                ..opts
            },
        )
        .await
    }

    async fn prepare_execute_transform<'a>(
        &self,
        params: ExecuteTransformParams,
        data_file: Option<&'a OwnedFile>,
        checkpoint: Option<&'a CheckpointRef>,
    ) -> Result<ExecuteTransform, InternalError> {
        let (new_data, new_checkpoint) = self
            .prepare_objects(params.new_offset_interval, data_file, checkpoint)
            .await?;

        Ok(ExecuteTransform {
            query_inputs: params.query_inputs,
            prev_checkpoint: params.prev_checkpoint,
            prev_offset: params.prev_offset,
            new_data,
            new_checkpoint,
            new_watermark: params.new_watermark,
        })
    }

    fn get_storage_internal_url(&self) -> &Url {
        &self.storage_internal_url
    }

    fn as_metadata_chain(&self) -> &dyn MetadataChain {
        &self.metadata_chain
    }

    fn as_data_repo(&self) -> &dyn ObjectRepository {
        &self.data_repo
    }

    fn as_checkpoint_repo(&self) -> &dyn ObjectRepository {
        &self.checkpoint_repo
    }

    fn as_info_repo(&self) -> &dyn NamedObjectRepository {
        &self.info_repo
    }
}
