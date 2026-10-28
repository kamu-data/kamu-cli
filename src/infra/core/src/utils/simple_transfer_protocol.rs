// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use futures::{StreamExt, TryStreamExt, stream};
use internal_error::ErrorIntoInternal;
use kamu_core::utils::metadata_chain_comparator::*;
use kamu_core::*;
use odf::dataset::{
    MetadataChainExt as _,
    MetadataChainImpl,
    MetadataChainReferenceRepositoryImpl,
};
use odf::storage::inmem::{NamedObjectRepositoryInMemory, ObjectRepositoryInMemory};
use odf::storage::{MetadataBlockRepositoryImpl, ReferenceRepositoryImpl};

use super::local_sync_writes::*;
use super::sync_increment::sync_increment;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const ENV_VAR_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS: &str =
    "SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS";
const DEFAULT_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS: usize = 10;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
pub struct SimpleProtocolTransferOptions {
    pub max_parallel_transfers: usize,
    pub visibility_for_created_dataset: odf::DatasetVisibility,
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
            visibility_for_created_dataset: odf::DatasetVisibility::Private,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Implements "Simple Transfer Protocol" as described in ODF spec
#[dill::component]
pub struct SimpleTransferProtocol {
    catalog: dill::Catalog,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SimpleTransferProtocol {
    /// Pushes into a dataset that is not a dataset of this node.
    /// Moves its `HEAD` by the storage's own compare-and-swap from the head
    /// read at the start
    pub async fn push(
        &self,
        src_ref: &odf::DatasetRefAny,
        src: Arc<dyn odf::Dataset>,
        dst: Arc<dyn odf::Dataset>,
        validation: odf::dataset::AppendValidation,
        trust_source_hashes: bool,
        force: bool,
        transfer_options: SimpleProtocolTransferOptions,
        listener: Arc<dyn SyncListener + 'static>,
    ) -> Result<SyncResult, SyncError> {
        let dst_head = try_resolve_head(dst.as_metadata_chain()).await?;

        let listener_adapter = CompareChainsListenerAdapter::new(listener.clone());
        let Some((src_head, blocks_desc_ordered)) = Self::collect_new_blocks(
            src_ref,
            src.as_metadata_chain(),
            dst.as_metadata_chain(),
            dst_head.as_ref(),
            force,
            &listener_adapter,
        )
        .await?
        else {
            return Ok(SyncResult::UpToDate);
        };

        let num_blocks = u64::try_from(blocks_desc_ordered.len()).unwrap();

        // Read before the destination head moves, so a failure here aborts the push
        let increment =
            sync_increment(src.as_metadata_chain(), dst_head.as_ref(), &src_head).await?;

        let stats = self
            .transfer_objects(
                &blocks_desc_ordered,
                src.as_ref(),
                dst.as_ref(),
                trust_source_hashes,
                listener.clone(),
                &transfer_options,
                listener_adapter.into_status(),
            )
            .await?;

        Self::append_to_remote(
            dst.as_ref(),
            blocks_desc_ordered,
            odf::dataset::AppendBatchOpts {
                validation,
                trust_hashes: trust_source_hashes,
            },
            dst_head.as_ref(),
        )
        .await?;
        Self::report_blocks_written(listener.as_ref(), stats, num_blocks);

        Ok(SyncResult::Updated {
            old_head: dst_head,
            new_head: src_head,
            num_blocks,
            increment,
        })
    }

    /// Writes the blocks and objects a local dataset lacks without moving its
    /// `HEAD`. A new dataset is stored from the source's `Seed`, without an
    /// entry or `HEAD`, for the commit to create
    pub async fn transfer_into_local(
        &self,
        src_ref: &odf::DatasetRefAny,
        src: Arc<dyn odf::Dataset>,
        dst: &LocalSyncTarget,
        validation: odf::dataset::AppendValidation,
        trust_source_hashes: bool,
        force: bool,
        transfer_options: SimpleProtocolTransferOptions,
        listener: Arc<dyn SyncListener + 'static>,
    ) -> Result<SyncTransfer, SyncError> {
        let empty_chain = MetadataChainImpl::new(
            MetadataBlockRepositoryImpl::new(ObjectRepositoryInMemory::new()),
            MetadataChainReferenceRepositoryImpl::new(ReferenceRepositoryImpl::new(
                NamedObjectRepositoryInMemory::new(),
            )),
        );

        let (dst_chain, dst_head) = match dst {
            LocalSyncTarget::Existing {
                dataset,
                expected_head,
            } => (dataset.as_metadata_chain(), expected_head.as_ref()),
            LocalSyncTarget::New(_) => (&empty_chain as &dyn odf::MetadataChain, None),
        };

        let listener_adapter = CompareChainsListenerAdapter::new(listener.clone());
        let Some((src_head, mut blocks_desc_ordered)) = Self::collect_new_blocks(
            src_ref,
            src.as_metadata_chain(),
            dst_chain,
            dst_head,
            force,
            &listener_adapter,
        )
        .await?
        else {
            return Ok(SyncTransfer::UpToDate);
        };

        let num_blocks = u64::try_from(blocks_desc_ordered.len()).unwrap();

        // Safety: there are blocks, oldest last
        let (dst_dataset, target) = prepare_transfer_target(
            &self.catalog,
            dst,
            transfer_options.visibility_for_created_dataset,
            || blocks_desc_ordered.pop().unwrap(),
        )
        .await?;

        let stats = self
            .transfer_objects(
                &blocks_desc_ordered,
                src.as_ref(),
                dst_dataset.as_ref(),
                trust_source_hashes,
                listener.clone(),
                &transfer_options,
                listener_adapter.into_status(),
            )
            .await?;

        append_transferred_blocks(
            dst_dataset.as_ref(),
            Box::new(blocks_desc_ordered.into_iter().rev()),
            odf::dataset::AppendBatchOpts {
                validation,
                trust_hashes: trust_source_hashes,
            },
        )
        .await?;
        Self::report_blocks_written(listener.as_ref(), stats, num_blocks);

        let increment =
            sync_increment(dst_dataset.as_metadata_chain(), dst_head, &src_head).await?;

        Ok(SyncTransfer::Transferred(TransferredSync {
            target,
            new_head: src_head,
            num_blocks,
            increment,
        }))
    }

    /// Compares the chains and returns the source head with the blocks to
    /// write, newest first, or `None` when the destination is up to date
    async fn collect_new_blocks(
        src_ref: &odf::DatasetRefAny,
        src_chain: &dyn odf::MetadataChain,
        dst_chain: &dyn odf::MetadataChain,
        dst_head: Option<&odf::Multihash>,
        force: bool,
        listener_adapter: &CompareChainsListenerAdapter,
    ) -> Result<
        Option<(
            odf::Multihash,
            Vec<odf::dataset::HashedMetadataBlockBytesDecoded>,
        )>,
        SyncError,
    > {
        let src_head = Self::get_src_head(src_ref, src_chain).await?;

        tracing::info!(?src_head, ?dst_head, "Resolved heads");

        let chains_comparison = MetadataChainComparator::compare_chains(
            src_chain,
            &src_head,
            dst_chain,
            dst_head,
            listener_adapter,
        )
        .await?;

        let blocks_desc_ordered = match chains_comparison {
            CompareChainsResult::Equal => return Ok(None),
            CompareChainsResult::LhsAhead {
                lhs_ahead_blocks: src_ahead_blocks,
            } => src_ahead_blocks,
            CompareChainsResult::LhsBehind {
                ref rhs_ahead_blocks,
            } => {
                if !force {
                    return Err(SyncError::DestinationAhead(DestinationAheadError {
                        src_head,
                        dst_head: dst_head.unwrap().clone(),
                        dst_ahead_size: rhs_ahead_blocks.len(),
                    }));
                }
                Self::load_all_blocks(src_chain).await?
            }
            CompareChainsResult::Divergence {
                uncommon_blocks_in_lhs: uncommon_blocks_in_src,
                uncommon_blocks_in_rhs: uncommon_blocks_in_dst,
            } => {
                if !force {
                    return Err(SyncError::DatasetsDiverged(DatasetsDivergedError {
                        src_head,
                        dst_head: dst_head.unwrap().clone(),
                        detail: Some(DatasetsDivergedErrorDetail {
                            uncommon_blocks_in_src,
                            uncommon_blocks_in_dst,
                        }),
                    }));
                }
                Self::load_all_blocks(src_chain).await?
            }
            CompareChainsResult::DifferentDatasets {
                lhs_dataset_id: src_dataset_id,
                rhs_dataset_id: dst_dataset_id,
            } => {
                // Even a forced sync must not replace a dataset with another one
                return Err(SyncError::DatasetIdMismatch(DatasetIdMismatchError {
                    src_dataset_id,
                    dst_dataset_id,
                }));
            }
        };

        // Decode blocks, as we need to access events to scan for data chunks etc.
        let blocks_desc_ordered = blocks_desc_ordered
            .into_iter()
            .map(|(hash, _, bytes)| bytes.decode().map(|block| (hash, block, bytes)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| CorruptedSourceError {
                message: "Unable to decode metadata block".into(),
                source: Some(e.into()),
            })?;

        Ok(Some((src_head, blocks_desc_ordered)))
    }

    /// The whole source chain, newest first, which a forced sync writes over a
    /// destination that is ahead or has diverged
    async fn load_all_blocks(
        src_chain: &dyn odf::MetadataChain,
    ) -> Result<
        Vec<(
            odf::Multihash,
            odf::MetadataBlockHeader,
            odf::MetadataBlockBytes,
        )>,
        SyncError,
    > {
        src_chain
            .iter_blocks_bytes_interval((&odf::BlockRef::Head).into(), None, false)
            .try_collect()
            .await
            .map_err(Self::map_block_iteration_error)
    }

    async fn get_src_head(
        src_ref: &odf::DatasetRefAny,
        src_chain: &dyn odf::MetadataChain,
    ) -> Result<odf::Multihash, SyncError> {
        match src_chain.resolve_ref(&odf::BlockRef::Head).await {
            Ok(head) => Ok(head),
            Err(odf::GetRefError::NotFound(_)) => Err(DatasetAnyRefUnresolvedError {
                dataset_ref: src_ref.clone(),
            }
            .into()),
            Err(odf::GetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(odf::GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
        }
    }

    /// A source chain that cannot be read through is corrupted
    pub(crate) fn map_block_iteration_error(e: odf::IterBlocksError) -> SyncError {
        const METADATA_CHAIN_BROKEN_MESSAGE: &str = "Source metadata chain is broken";

        match e {
            odf::IterBlocksError::RefNotFound(e) => SyncError::Internal(e.int_err()),
            odf::IterBlocksError::BlockNotFound(e) => CorruptedSourceError {
                message: METADATA_CHAIN_BROKEN_MESSAGE.to_owned(),
                source: Some(e.into()),
            }
            .into(),
            odf::IterBlocksError::BlockVersion(e) => CorruptedSourceError {
                message: METADATA_CHAIN_BROKEN_MESSAGE.to_owned(),
                source: Some(e.into()),
            }
            .into(),
            odf::IterBlocksError::BlockMalformed(e) => CorruptedSourceError {
                message: METADATA_CHAIN_BROKEN_MESSAGE.to_owned(),
                source: Some(e.into()),
            }
            .into(),
            odf::IterBlocksError::InvalidInterval(_) => unreachable!(),
            odf::IterBlocksError::Access(e) => SyncError::Access(e),
            e @ odf::IterBlocksError::Internal(_) => SyncError::Internal(e.int_err()),
        }
    }

    /// The `HEAD` of a remote repository is not a dataset reference of this
    /// node, so the chain moves it by its own compare-and-set rather than
    /// through `DatasetHeadCommitter`
    async fn append_to_remote(
        dst: &dyn odf::Dataset,
        blocks_desc_ordered: Vec<odf::dataset::HashedMetadataBlockBytesDecoded>,
        append_opts: odf::dataset::AppendBatchOpts,
        expected_head: Option<&odf::Multihash>,
    ) -> Result<(), SyncError> {
        use odf::dataset::{AppendBatchAndSetHeadError as E, SetChainRefError};

        dst.as_metadata_chain()
            .append_batch_and_set_head(
                Box::new(blocks_desc_ordered.into_iter().rev()),
                append_opts,
                expected_head,
            )
            .await
            .map_err(|e| match e {
                E::Append(e) => map_append_batch_error(e),
                E::SetHead(SetChainRefError::CASFailed(e)) => {
                    SyncError::UpdatedConcurrently(e.into())
                }
                E::SetHead(SetChainRefError::Access(e)) => SyncError::Access(e),
                E::SetHead(
                    e @ (SetChainRefError::Internal(_) | SetChainRefError::BlockNotFound(_)),
                ) => SyncError::Internal(e.int_err()),
            })?;

        Ok(())
    }

    fn report_blocks_written(listener: &dyn SyncListener, mut stats: SyncStats, num_blocks: u64) {
        stats.dst.metadata_blocks_written += num_blocks;
        listener.on_status(SyncStage::CommitBlocks, &stats);
    }

    async fn copy_object(
        &self,
        src: &dyn odf::Dataset,
        dst: &dyn odf::Dataset,
        object: BlockObject<'_>,
        trust_source_hashes: bool,
        listener: Arc<dyn SyncListener>,
        arc_stats: Arc<Mutex<SyncStats>>,
    ) -> Result<(), SyncError> {
        let hash = object.physical_hash();
        tracing::info!(?hash, "Transferring {} file", object.name());

        let stream = match object.repo(src).get_stream(hash).await {
            Ok(s) => Ok(s),
            Err(odf::storage::GetError::NotFound(e)) => Err(CorruptedSourceError {
                message: format!("Source {} file is missing", object.name()),
                source: Some(e.into()),
            }
            .into()),
            Err(odf::storage::GetError::Access(e)) => Err(SyncError::Access(e)),
            Err(odf::storage::GetError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        match object
            .repo(dst)
            .insert_stream(
                stream,
                odf::storage::InsertOpts {
                    precomputed_hash: trust_source_hashes.then_some(hash),
                    expected_hash: Some(hash),
                    // S3 cannot stream an upload without knowing its length
                    size_hint: Some(object.size()),
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(odf::storage::InsertError::HashMismatch(e)) => Err(CorruptedSourceError {
                message: format!(
                    "Source {} file hash didn't match the computed - this may be an indication of \
                     hashing algorithm mismatch or an attempted tampering",
                    object.name()
                ),
                source: Some(e.into()),
            }
            .into()),
            Err(odf::storage::InsertError::Access(e)) => Err(SyncError::Access(e)),
            Err(odf::storage::InsertError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        let mut stats = arc_stats.lock().unwrap();
        object.count(&mut stats);
        listener.on_status(SyncStage::TransferData, &stats);

        Ok(())
    }

    /// Copies the data and checkpoint files the new blocks reference, before
    /// any block that references them is written. Returns the stats so far
    async fn transfer_objects<'a>(
        &'a self,
        blocks_desc_ordered: &'a [odf::dataset::HashedMetadataBlockBytesDecoded],
        src: &'a dyn odf::Dataset,
        dst: &'a dyn odf::Dataset,
        trust_source_hashes: bool,
        listener: Arc<dyn SyncListener>,
        transfer_options: &SimpleProtocolTransferOptions,
        mut stats: SyncStats,
    ) -> Result<SyncStats, SyncError> {
        // Update stats estimates based on metadata
        stats.dst_estimated.metadata_blocks_written +=
            u64::try_from(blocks_desc_ordered.len()).unwrap();

        use odf::metadata::IntoDataStreamBlock;
        for block in blocks_desc_ordered
            .iter()
            .filter_map(|(_, b, _)| b.as_data_stream_block())
        {
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

        tracing::info!(
            ?stats,
            "Considering {} new blocks",
            blocks_desc_ordered.len()
        );
        listener.on_status(SyncStage::TransferData, &stats);

        // Download data and checkpoints
        let arc_stats = Arc::new(Mutex::new(stats));
        let mut block_download_tasks = vec![];
        blocks_desc_ordered.iter().rev().for_each(|(_, b, _)| {
            if let Some(block_stream) = b.as_data_stream_block() {
                let objects = [
                    block_stream.event.new_data.map(BlockObject::Data),
                    block_stream
                        .event
                        .new_checkpoint
                        .map(BlockObject::Checkpoint),
                ];
                for object in objects.into_iter().flatten() {
                    block_download_tasks.push(self.copy_object(
                        src,
                        dst,
                        object,
                        trust_source_hashes,
                        listener.clone(),
                        arc_stats.clone(),
                    ));
                }
            }
        });

        stream::iter(block_download_tasks)
            .map(Ok)
            .try_for_each_concurrent(transfer_options.max_parallel_transfers, |future| future)
            .await?;

        Ok(Arc::into_inner(arc_stats).unwrap().into_inner().unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

/// An object a block refers to, copied along with it
#[derive(Clone, Copy)]
enum BlockObject<'a> {
    Data(&'a odf::DataSlice),
    Checkpoint(&'a odf::Checkpoint),
}

impl<'a> BlockObject<'a> {
    fn name(self) -> &'static str {
        match self {
            Self::Data(_) => "data",
            Self::Checkpoint(_) => "checkpoint",
        }
    }

    fn physical_hash(self) -> &'a odf::Multihash {
        match self {
            Self::Data(data_slice) => &data_slice.physical_hash,
            Self::Checkpoint(checkpoint) => &checkpoint.physical_hash,
        }
    }

    fn size(self) -> u64 {
        match self {
            Self::Data(data_slice) => data_slice.size,
            Self::Checkpoint(checkpoint) => checkpoint.size,
        }
    }

    fn repo(self, dataset: &dyn odf::Dataset) -> &dyn odf::storage::ObjectRepository {
        match self {
            Self::Data(_) => dataset.as_data_repo(),
            Self::Checkpoint(_) => dataset.as_checkpoint_repo(),
        }
    }

    fn count(self, stats: &mut SyncStats) {
        match self {
            Self::Data(data_slice) => {
                stats.src.data_slices_read += 1;
                stats.dst.data_slices_written += 1;
                stats.src.data_records_read += data_slice.num_records();
                stats.dst.data_records_written += data_slice.num_records();
            }
            Self::Checkpoint(_) => {
                stats.src.checkpoints_read += 1;
                stats.dst.checkpoints_written += 1;
            }
        }
        stats.src.bytes_read += self.size();
        stats.dst.bytes_written += self.size();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
