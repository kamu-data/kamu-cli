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
use kamu_core::*;
use odf::metadata::IntoDataStreamBlock;

use super::protocol::{SimpleProtocolTransferOptions, SimpleTransferProtocol};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SimpleTransferProtocol {
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
    /// any block that references them is written. `num_blocks` counts every
    /// block the sync writes, a `Seed` stored apart included. Returns the
    /// stats so far
    pub(super) async fn transfer_objects<'a>(
        &'a self,
        blocks_desc_ordered: &'a [odf::dataset::HashedMetadataBlockBytesDecoded],
        src: &'a dyn odf::Dataset,
        dst: &'a dyn odf::Dataset,
        trust_source_hashes: bool,
        listener: Arc<dyn SyncListener>,
        transfer_options: &SimpleProtocolTransferOptions,
        num_blocks: u64,
        mut stats: SyncStats,
    ) -> Result<SyncStats, SyncError> {
        // Update stats estimates based on metadata
        stats.dst_estimated.metadata_blocks_written += num_blocks;

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
