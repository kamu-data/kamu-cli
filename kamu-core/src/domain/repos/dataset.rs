// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use crate::domain::*;
use opendatafabric::*;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use thiserror::Error;
use tracing::{error, info, info_span};
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait Dataset: Send + Sync {
    fn as_metadata_chain(&self) -> &dyn MetadataChain;
    fn as_data_repo(&self) -> &dyn ObjectRepository;
    fn as_checkpoint_repo(&self) -> &dyn ObjectRepository;
    fn as_cache_repo(&self) -> &dyn NamedObjectRepository;

    fn supports_smart_protocol(&self) -> bool;
    fn base_url(&self) -> &Url;

    /// Returns a brief summary of the dataset
    async fn get_summary(&self, opts: GetSummaryOpts) -> Result<DatasetSummary, GetSummaryError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct GetSummaryOpts {
    pub update_if_stale: bool,
}

impl Default for GetSummaryOpts {
    fn default() -> Self {
        Self {
            update_if_stale: true,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Helpers
/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait DatasetExt: Dataset {
    /// Helper function to append a generic event to metadata chain.
    ///
    /// Warning: Don't use when synchronizing blocks from another dataset.
    async fn commit_event(
        &self,
        event: MetadataEvent,
        opts: CommitOpts<'_>,
    ) -> Result<CommitResult, CommitError> {
        let chain = self.as_metadata_chain();

        let prev_block_hash = if let Some(prev_block_hash) = opts.prev_block_hash {
            prev_block_hash.cloned()
        } else {
            match chain.get_ref(opts.block_ref).await {
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
            system_time: opts.system_time.unwrap_or_else(|| Utc::now()),
            event,
        };

        info!(?block, "Committing new block");

        let new_head = chain
            .append(
                block,
                AppendOpts {
                    update_ref: Some(opts.block_ref),
                    check_ref_is_prev_block: true,
                    ..AppendOpts::default()
                },
            )
            .await?;

        info!(%new_head, "Committed new block");

        Ok(CommitResult {
            old_head: prev_block_hash,
            new_head,
        })
    }

    /// Helper function to commit AddData event into a local dataset.
    ///
    /// Will attempt to atomically move data and checkpoint files, so those have to be on the same file system as the workspace.
    async fn commit_add_data<P: AsRef<Path> + Send + Sync>(
        &self,
        input_checkpoint: Option<Multihash>,
        data_interval: Option<OffsetInterval>,
        movable_data_file: Option<P>,
        movable_checkpoint_file: Option<P>,
        watermark: Option<DateTime<Utc>>,
        opts: CommitOpts<'_>,
    ) -> Result<CommitResult, CommitError> {
        // New block might not contain anything new, so we check for data
        // and watermark differences to see if commit should be skipped
        let metadata_event = if let Some(data_interval) = data_interval {
            let span = info_span!("Computing data hashes");
            let _span_guard = span.enter();

            let from_data_path = movable_data_file.unwrap();

            let output_data = DataSlice {
                logical_hash: crate::infra::utils::data_utils::get_parquet_logical_hash(
                    from_data_path.as_ref(),
                )
                .int_err()?,
                physical_hash: crate::infra::utils::data_utils::get_file_physical_hash(
                    from_data_path.as_ref(),
                )
                .int_err()?,
                interval: data_interval,
                size: std::fs::metadata(&from_data_path).int_err()?.len() as i64,
            };

            // Commit data
            self.as_data_repo()
                .insert_file_move(
                    from_data_path.as_ref(),
                    InsertOpts {
                        precomputed_hash: Some(&output_data.physical_hash),
                        expected_hash: None,
                        size_hint: Some(output_data.size as usize),
                    },
                )
                .await
                .int_err()?;

            // Commit checkpoint
            //
            // TODO: Should checkpoint be committed even when there is no data?
            // Postpone changes until we revisit ingest checkpoints
            let output_checkpoint = if let Some(from_checkpoint_path) = movable_checkpoint_file {
                let physical_hash = crate::infra::utils::data_utils::get_file_physical_hash(
                    from_checkpoint_path.as_ref(),
                )
                .int_err()?;

                let size = std::fs::metadata(&from_checkpoint_path).int_err()?.len() as i64;

                self.as_checkpoint_repo()
                    .insert_file_move(
                        from_checkpoint_path.as_ref(),
                        InsertOpts {
                            precomputed_hash: Some(&physical_hash),
                            expected_hash: None,
                            size_hint: Some(size as usize),
                        },
                    )
                    .await
                    .int_err()?;

                Some(Checkpoint {
                    physical_hash,
                    size,
                })
            } else {
                None
            };

            MetadataEvent::AddData(AddData {
                input_checkpoint,
                output_data,
                output_checkpoint,
                output_watermark: watermark,
            })
        } else {
            let prev_watermark = self
                .as_metadata_chain()
                .iter_blocks()
                .filter_data_stream_blocks()
                .filter_map_ok(|(_, b)| b.event.output_watermark)
                .try_first()
                .await
                .int_err()?;

            if watermark.is_none() || watermark == prev_watermark {
                info!(concat!(
                    "Skipping commit of new block as ",
                    "it neither has new data nor an updated watermark"
                ));
                return Err(CommitError::EmptyCommit);
            }

            // TODO: Should this be here?
            MetadataEvent::SetWatermark(SetWatermark {
                output_watermark: watermark.unwrap(),
            })
        };

        self.commit_event(metadata_event, opts).await
    }
}

impl<T> DatasetExt for T where T: Dataset + ?Sized {}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct CommitOpts<'a> {
    /// Which reference to advance upon commit
    pub block_ref: &'a BlockRef,
    /// Override system time of the new block
    pub system_time: Option<DateTime<Utc>>,
    /// Compare-and-swap semantics to ensure there were no concurrent updates
    pub prev_block_hash: Option<Option<&'a Multihash>>,
}

impl<'a> Default for CommitOpts<'a> {
    fn default() -> Self {
        Self {
            block_ref: &BlockRef::Head,
            system_time: None,
            prev_block_hash: None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct CommitResult {
    pub old_head: Option<Multihash>,
    pub new_head: Multihash,
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Expected object of type {expected} but got {actual}")]
pub struct InvalidObjectKind {
    pub expected: String,
    pub actual: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetSummaryError {
    #[error("Dataset is empty")]
    EmptyDataset,
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CommitError {
    #[error("Empty commit")]
    EmptyCommit,
    #[error(transparent)]
    MetadataAppendError(#[from] AppendError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}
