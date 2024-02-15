// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures::{stream, Future, StreamExt, TryStreamExt};
use kamu_core::sync_service::DatasetNotFoundError;
use kamu_core::utils::metadata_chain_comparator::*;
use kamu_core::*;
use opendatafabric::*;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub const ENV_VAR_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS: &str =
    "SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS";
const DEFAULT_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS: usize = 10;

/////////////////////////////////////////////////////////////////////////////////////////

type BoxedCreateDatasetFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<CreateDatasetResult, CreateDatasetError>> + Send>,
>;

pub type DatasetFactoryFn =
    Box<dyn FnOnce(MetadataBlockTyped<Seed>) -> BoxedCreateDatasetFuture + Send>;

#[derive(Debug, Eq, PartialEq)]
pub struct SimpleProtocolTransferOptions {
    pub max_parallel_transfers: usize,
}

impl Default for SimpleProtocolTransferOptions {
    fn default() -> Self {
        let max_parallel_transfers =
            match std::env::var(ENV_VAR_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS) {
                Ok(string_value) => string_value
                    .parse::<usize>()
                    .unwrap_or(DEFAULT_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS),
                _ => DEFAULT_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS,
            };
        Self {
            max_parallel_transfers,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

/// Implements "Simple Transfer Protocol" as described in ODF spec
pub struct SimpleTransferProtocol;

/////////////////////////////////////////////////////////////////////////////////////////

impl SimpleTransferProtocol {
    pub async fn sync(
        &self,
        src_ref: &DatasetRefAny,
        src: Arc<dyn Dataset>,
        maybe_dst: Option<Arc<dyn Dataset>>,
        dst_factory: Option<DatasetFactoryFn>,
        validation: AppendValidation,
        trust_source_hashes: bool,
        force: bool,
        listener: Arc<dyn SyncListener + 'static>,
    ) -> Result<SyncResult, SyncError> {
        listener.begin();

        let empty_chain = MetadataChainImpl::new(
            MetadataBlockRepositoryImpl::new(ObjectRepositoryInMemory::new()),
            ReferenceRepositoryImpl::new(NamedObjectRepositoryInMemory::new()),
        );

        let src_chain = src.as_metadata_chain();
        let src_head = self.get_src_head(src_ref, src_chain).await?;

        let (dst_chain, dst_head) = if let Some(dst) = &maybe_dst {
            let dst_chain = dst.as_metadata_chain();
            let dst_head = self.get_dest_head(dst_chain).await?;
            (dst_chain, dst_head)
        } else {
            (&empty_chain as &dyn MetadataChain, None)
        };

        tracing::info!(?src_head, ?dst_head, "Resolved heads");

        let listener_adapter = CompareChainsListenerAdapter::new(listener.clone());

        let chains_comparison = MetadataChainComparator::compare_chains(
            src_chain,
            &src_head,
            dst_chain,
            dst_head.as_ref(),
            &listener_adapter,
        )
        .await?;

        match chains_comparison {
            CompareChainsResult::Equal => return Ok(SyncResult::UpToDate),
            CompareChainsResult::LhsAhead { .. } => { /* Skip */ }
            CompareChainsResult::LhsBehind {
                ref rhs_ahead_blocks,
            } => {
                if !force {
                    return Err(SyncError::DestinationAhead(DestinationAheadError {
                        src_head,
                        dst_head: dst_head.unwrap(),
                        dst_ahead_size: rhs_ahead_blocks.len(),
                    }));
                }
            }
            CompareChainsResult::Divergence {
                uncommon_blocks_in_lhs: uncommon_blocks_in_src,
                uncommon_blocks_in_rhs: uncommon_blocks_in_dst,
            } => {
                if !force {
                    return Err(SyncError::DatasetsDiverged(DatasetsDivergedError {
                        src_head,
                        dst_head: dst_head.unwrap(),
                        uncommon_blocks_in_dst,
                        uncommon_blocks_in_src,
                    }));
                }
            }
        };

        let mut blocks = match chains_comparison {
            CompareChainsResult::Equal => unreachable!(),
            CompareChainsResult::LhsAhead {
                lhs_ahead_blocks: src_ahead_blocks,
            } => src_ahead_blocks,
            CompareChainsResult::LhsBehind { .. } | CompareChainsResult::Divergence { .. } => {
                // Load all source blocks from head to tail
                assert!(force);
                src_chain
                    .iter_blocks()
                    .try_collect()
                    .await
                    .map_err(Self::map_block_iteration_error)?
            }
        };

        let old_head = dst_head.clone();
        let num_blocks = blocks.len();

        // Create dataset if necessary using the source Seed block
        let (dst, dst_head) = if let Some(dst) = maybe_dst {
            (dst, dst_head)
        } else {
            let (_, first_block) = blocks.pop().unwrap();
            let seed_block = first_block
                .into_typed()
                .ok_or_else(|| CorruptedSourceError {
                    message: "First metadata block is not Seed".to_owned(),
                    source: None,
                })?;
            let create_result = dst_factory.unwrap()(seed_block).await?;
            (create_result.dataset, Some(create_result.head))
        };

        let new_watermark = blocks
            .iter()
            .rev()
            .map(|(_, block)| match &block.event {
                MetadataEvent::AddData(add_data) => add_data.new_watermark,
                MetadataEvent::ExecuteTransform(execute_transform) => {
                    execute_transform.new_watermark
                }
                _ => None,
            })
            .find(Option::is_some)
            .flatten();

        let stats = self
            .synchronize_blocks(
                blocks,
                src.as_ref(),
                dst.as_ref(),
                &src_head,
                dst_head.as_ref(),
                validation,
                trust_source_hashes,
                listener,
                listener_adapter.into_status(),
            )
            .await?;

        Ok(SyncResult::Updated {
            old_head,
            new_head: src_head,
            num_blocks: num_blocks as u64,
            num_records: stats.dst.data_records_written,
            new_watermark,
        })
    }

    async fn get_src_head(
        &self,
        src_ref: &DatasetRefAny,
        src_chain: &dyn MetadataChain,
    ) -> Result<Multihash, SyncError> {
        match src_chain.resolve_ref(&BlockRef::Head).await {
            Ok(head) => Ok(head),
            Err(GetRefError::NotFound(_)) => Err(DatasetNotFoundError {
                dataset_ref: src_ref.clone(),
            }
            .into()),
            Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
        }
    }

    async fn get_dest_head(
        &self,
        dst_chain: &dyn MetadataChain,
    ) -> Result<Option<Multihash>, SyncError> {
        match dst_chain.resolve_ref(&BlockRef::Head).await {
            Ok(h) => Ok(Some(h)),
            Err(GetRefError::NotFound(_)) => Ok(None),
            Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
        }
    }

    fn map_block_iteration_error(e: IterBlocksError) -> SyncError {
        match e {
            IterBlocksError::RefNotFound(e) => SyncError::Internal(e.int_err()),
            IterBlocksError::BlockNotFound(e) => CorruptedSourceError {
                message: "Source metadata chain is broken".to_owned(),
                source: Some(e.into()),
            }
            .into(),
            IterBlocksError::BlockVersion(e) => CorruptedSourceError {
                message: "Source metadata chain is broken".to_owned(),
                source: Some(e.into()),
            }
            .into(),
            IterBlocksError::BlockMalformed(e) => CorruptedSourceError {
                message: "Source metadata chain is broken".to_owned(),
                source: Some(e.into()),
            }
            .into(),
            IterBlocksError::InvalidInterval(_) => unreachable!(),
            IterBlocksError::Access(e) => SyncError::Access(e),
            IterBlocksError::Internal(e) => SyncError::Internal(e),
        }
    }

    async fn download_block_data<'a>(
        &'a self,
        src: &'a dyn Dataset,
        dst: &'a dyn Dataset,
        data_slice: &DataSlice,
        trust_source_hashes: bool,
        listener: Arc<dyn SyncListener>,
        arc_stats: Arc<Mutex<SyncStats>>,
    ) -> Result<(), SyncError> {
        tracing::info!(hash = ?data_slice.physical_hash, "Transferring data file");

        let stream = match src
            .as_data_repo()
            .get_stream(&data_slice.physical_hash)
            .await
        {
            Ok(s) => Ok(s),
            Err(GetError::NotFound(e)) => Err(CorruptedSourceError {
                message: "Source data file is missing".to_owned(),
                source: Some(e.into()),
            }
            .into()),
            Err(GetError::Access(e)) => Err(SyncError::Access(e)),
            Err(GetError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        match dst
            .as_data_repo()
            .insert_stream(
                stream,
                InsertOpts {
                    precomputed_hash: if !trust_source_hashes {
                        None
                    } else {
                        Some(&data_slice.physical_hash)
                    },
                    expected_hash: Some(&data_slice.physical_hash),
                    size_hint: Some(data_slice.size),
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(InsertError::HashMismatch(e)) => Err(CorruptedSourceError {
                message: concat!(
                    "Data file hash declared by the source didn't match ",
                    "the computed - this may be an indication of hashing ",
                    "algorithm mismatch or an attempted tampering",
                )
                .to_owned(),
                source: Some(e.into()),
            }
            .into()),
            Err(InsertError::Access(e)) => Err(SyncError::Access(e)),
            Err(InsertError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        let mut stats = arc_stats.lock().unwrap();

        stats.src.data_slices_read += 1;
        stats.dst.data_slices_written += 1;
        stats.src.data_records_read += data_slice.num_records();
        stats.dst.data_records_written += data_slice.num_records();
        stats.src.bytes_read += data_slice.size;
        stats.dst.bytes_written += data_slice.size;

        listener.on_status(SyncStage::TransferData, &stats);

        Ok(())
    }

    async fn download_block_checkpoint<'a>(
        &'a self,
        src: &'a dyn Dataset,
        dst: &'a dyn Dataset,
        checkpoint: &Checkpoint,
        trust_source_hashes: bool,
        listener: Arc<dyn SyncListener>,
        arc_stats: Arc<Mutex<SyncStats>>,
    ) -> Result<(), SyncError> {
        tracing::info!(hash = ?checkpoint.physical_hash, "Transferring checkpoint file");

        let stream = match src
            .as_checkpoint_repo()
            .get_stream(&checkpoint.physical_hash)
            .await
        {
            Ok(s) => Ok(s),
            Err(GetError::NotFound(e)) => Err(CorruptedSourceError {
                message: "Source checkpoint file is missing".to_owned(),
                source: Some(e.into()),
            }
            .into()),
            Err(GetError::Access(e)) => Err(SyncError::Access(e)),
            Err(GetError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        match dst
            .as_checkpoint_repo()
            .insert_stream(
                stream,
                InsertOpts {
                    precomputed_hash: if !trust_source_hashes {
                        None
                    } else {
                        Some(&checkpoint.physical_hash)
                    },
                    expected_hash: Some(&checkpoint.physical_hash),
                    // This hint is necessary only for S3 implementation that does not
                    // currently support streaming uploads
                    // without knowing Content-Length. We should remove it in future.
                    size_hint: Some(checkpoint.size),
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(InsertError::HashMismatch(e)) => Err(CorruptedSourceError {
                message: concat!(
                    "Checkpoint file hash declared by the source didn't ",
                    "match the computed - this may be an indication of hashing ",
                    "algorithm mismatch or an attempted tampering",
                )
                .to_owned(),
                source: Some(e.into()),
            }
            .into()),
            Err(InsertError::Access(e)) => Err(SyncError::Access(e)),
            Err(InsertError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        let mut stats = arc_stats.lock().unwrap();

        stats.src.checkpoints_read += 1;
        stats.dst.checkpoints_written += 1;
        stats.src.bytes_read += checkpoint.size;
        stats.dst.bytes_written += checkpoint.size;

        listener.on_status(SyncStage::TransferData, &stats);

        Ok(())
    }

    async fn synchronize_blocks<'a>(
        &'a self,
        blocks: Vec<HashedMetadataBlock>,
        src: &'a dyn Dataset,
        dst: &'a dyn Dataset,
        src_head: &'a Multihash,
        dst_head: Option<&'a Multihash>,
        validation: AppendValidation,
        trust_source_hashes: bool,
        listener: Arc<dyn SyncListener>,
        mut stats: SyncStats,
    ) -> Result<SyncStats, SyncError> {
        // Update stats estimates based on metadata
        stats.dst_estimated.metadata_blocks_written += blocks.len() as u64;

        for block in blocks.iter().filter_map(|(_, b)| b.as_data_stream_block()) {
            if let Some(data_slice) = block.event.new_data {
                stats.src_estimated.data_slices_read += 1;
                stats.src_estimated.bytes_read += data_slice.size;
                stats.src_estimated.data_records_read += data_slice.num_records();

                stats.dst_estimated.data_slices_written += 1;
                stats.dst_estimated.bytes_written += data_slice.size;
                stats.dst_estimated.data_records_written += data_slice.num_records();
            }
            if let Some(checkpoint) = block.event.new_checkpoint {
                stats.src_estimated.checkpoints_read += 1;
                stats.src_estimated.bytes_read += checkpoint.size;

                stats.dst_estimated.checkpoints_written += 1;
                stats.dst_estimated.bytes_written += checkpoint.size;
            }
        }

        tracing::info!(?stats, "Considering {} new blocks", blocks.len());
        listener.on_status(SyncStage::TransferData, &stats);

        // Download data and checkpoints
        let arc_stats = Arc::new(Mutex::new(stats.clone()));
        let mut block_download_tasks = vec![];
        blocks.iter().rev().for_each(|(_, b)| {
            if let Some(block_stream) = b.as_data_stream_block() {
                if let Some(data_slice) = block_stream.event.new_data {
                    // Each function return unique future
                    // cast future to next type to allow storing them in vector
                    block_download_tasks.push(Box::pin(self.download_block_data(
                        src,
                        dst,
                        data_slice,
                        trust_source_hashes,
                        listener.clone(),
                        arc_stats.clone(),
                    ))
                        as Pin<Box<dyn Future<Output = Result<(), SyncError>> + Send>>);
                }
                if let Some(checkpoint) = block_stream.event.new_checkpoint {
                    block_download_tasks.push(Box::pin(self.download_block_checkpoint(
                        src,
                        dst,
                        checkpoint,
                        trust_source_hashes,
                        listener.clone(),
                        arc_stats.clone(),
                    )));
                }
            }
        });

        stream::iter(block_download_tasks)
            .map(Ok)
            .try_for_each_concurrent(
                SimpleProtocolTransferOptions::default().max_parallel_transfers,
                |future| future,
            )
            .await?;

        // Commit blocks
        for (hash, block) in blocks.into_iter().rev() {
            tracing::debug!(?hash, "Appending block");
            let sequence_number = block.sequence_number;

            match dst
                .as_metadata_chain()
                .append(
                    block,
                    AppendOpts {
                        validation,
                        update_ref: None, // We will update head once, after sync is complete
                        precomputed_hash: if !trust_source_hashes {
                            None
                        } else {
                            Some(&hash)
                        },
                        expected_hash: Some(&hash),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(AppendError::InvalidBlock(append_validation_error)) => {
                    let message = match append_validation_error {
                        AppendValidationError::HashMismatch(ref e) => format!(
                            concat!(
                                "Block hash declared by the source {} didn't match ",
                                "the computed {} at block {} - this may be an indication ",
                                "of hashing algorithm mismatch or an attempt to tamper data",
                            ),
                            e.actual, e.expected, sequence_number
                        ),
                        _ => format!(
                            "Source metadata chain is logically inconsistent at block \
                             {hash}[{sequence_number}]"
                        ),
                    };

                    Err(CorruptedSourceError {
                        message,
                        source: Some(append_validation_error.into()),
                    }
                    .into())
                }
                Err(AppendError::RefNotFound(_) | AppendError::RefCASFailed(_)) => unreachable!(),
                Err(AppendError::Access(e)) => Err(SyncError::Access(e)),
                Err(AppendError::Internal(e)) => Err(SyncError::Internal(e)),
            }?;

            stats.dst.metadata_blocks_written += 1;
            listener.on_status(SyncStage::CommitBlocks, &stats);
        }

        // Update reference, atomically committing the sync operation
        // Any failures before this point may result in dangling files but will keep the
        // destination dataset in its original logical state
        match dst
            .as_metadata_chain()
            .set_ref(
                &BlockRef::Head,
                src_head,
                SetRefOpts {
                    validate_block_present: false,
                    check_ref_is: Some(dst_head),
                },
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(SetRefError::CASFailed(e)) => Err(SyncError::UpdatedConcurrently(e.into())),
            Err(SetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(SetRefError::Internal(e)) => Err(SyncError::Internal(e)),
            Err(SetRefError::BlockNotFound(e)) => Err(SyncError::Internal(e.int_err())),
        }?;

        Ok(stats)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct CompareChainsListenerAdapter {
    l: Arc<dyn SyncListener>,
    stats: Mutex<SyncStats>,
}

impl CompareChainsListenerAdapter {
    fn new(l: Arc<dyn SyncListener>) -> Self {
        Self {
            l,
            stats: Mutex::new(SyncStats::default()),
        }
    }

    fn into_status(self) -> SyncStats {
        self.stats.into_inner().unwrap()
    }
}

impl CompareChainsListener for CompareChainsListenerAdapter {
    fn on_lhs_expected_reads(&self, num_blocks: u64) {
        let mut s = self.stats.lock().unwrap();
        s.src_estimated.metadata_blocks_read += num_blocks;
        self.l.on_status(SyncStage::ReadMetadata, &s);
    }

    fn on_lhs_read(&self, num_blocks: u64) {
        let mut s = self.stats.lock().unwrap();
        s.src.metadata_blocks_read += num_blocks;
        self.l.on_status(SyncStage::ReadMetadata, &s);
    }

    fn on_rhs_expected_reads(&self, num_blocks: u64) {
        let mut s = self.stats.lock().unwrap();
        s.dst_estimated.metadata_blocks_read += num_blocks;
        self.l.on_status(SyncStage::ReadMetadata, &s);
    }

    fn on_rhs_read(&self, num_blocks: u64) {
        let mut s = self.stats.lock().unwrap();
        s.dst.metadata_blocks_read += num_blocks;
        self.l.on_status(SyncStage::ReadMetadata, &s);
    }
}
