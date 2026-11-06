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
        let stats = &mut *stats;
        object.count(&mut stats.src, &mut stats.dst);
        listener.on_status(SyncStage::TransferData, stats);

        Ok(())
    }

    /// Objects are stored by hash, so one the destination has, such as from a
    /// sync that failed later, needs no copy
    async fn objects_missing_at<'a>(
        dst: &dyn odf::Dataset,
        objects: Vec<BlockObject<'a>>,
        max_parallel_checks: usize,
    ) -> Result<Vec<BlockObject<'a>>, SyncError> {
        // Zero means no limit, as for the copies. `buffered(0)` would never poll
        // its input, not even to see that it is empty
        let max_parallel_checks = match max_parallel_checks {
            0 => objects.len().max(1),
            limit => limit,
        };
        let checks: Vec<_> = objects
            .into_iter()
            .map(|object| Self::check_stored(dst, object))
            .collect();
        let presence: Vec<(BlockObject<'a>, bool)> = stream::iter(checks)
            .buffered(max_parallel_checks)
            .try_collect()
            .await?;

        Ok(presence
            .into_iter()
            .filter_map(|(object, stored)| {
                if stored {
                    tracing::info!(
                        hash = ?object.physical_hash(),
                        "Destination already has the {} file",
                        object.name()
                    );
                    None
                } else {
                    Some(object)
                }
            })
            .collect())
    }

    async fn check_stored<'a>(
        dst: &dyn odf::Dataset,
        object: BlockObject<'a>,
    ) -> Result<(BlockObject<'a>, bool), SyncError> {
        match object.repo(dst).contains(object.physical_hash()).await {
            Ok(stored) => Ok((object, stored)),
            Err(odf::storage::ContainsError::Access(e)) => Err(SyncError::Access(e)),
            Err(odf::storage::ContainsError::Internal(e)) => Err(SyncError::Internal(e)),
        }
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
        let referenced_objects = blocks_desc_ordered
            .iter()
            .rev()
            .filter_map(|(_, b, _)| b.as_data_stream_block())
            .flat_map(|block| {
                [
                    block.event.new_data.map(BlockObject::Data),
                    block.event.new_checkpoint.map(BlockObject::Checkpoint),
                ]
            })
            .flatten()
            .collect();
        let objects = Self::objects_missing_at(
            dst,
            referenced_objects,
            transfer_options.max_parallel_transfers,
        )
        .await?;

        // Estimates count every block, but only the files to copy
        stats.dst_estimated.metadata_blocks_written += num_blocks;
        for object in &objects {
            object.count(&mut stats.src_estimated, &mut stats.dst_estimated);
        }

        tracing::info!(
            ?stats,
            "Considering {} new blocks",
            blocks_desc_ordered.len()
        );
        listener.on_status(SyncStage::TransferData, &stats);

        // Download data and checkpoints
        let arc_stats = Arc::new(Mutex::new(stats));
        let block_download_tasks: Vec<_> = objects
            .into_iter()
            .map(|object| {
                self.copy_object(
                    src,
                    dst,
                    object,
                    trust_source_hashes,
                    listener.clone(),
                    arc_stats.clone(),
                )
            })
            .collect();

        stream::iter(block_download_tasks)
            .map(Ok)
            .try_for_each_concurrent(transfer_options.max_parallel_transfers, |future| future)
            .await?;

        Ok(Arc::into_inner(arc_stats).unwrap().into_inner().unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An object a block refers to, copied along with it
#[derive(Clone, Copy, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
enum BlockObject<'a> {
    Data(&'a odf::DataSlice),
    Checkpoint(&'a odf::Checkpoint),
}

impl<'a> BlockObject<'a> {
    fn name(self) -> &'static str {
        self.into()
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

    /// Adds the copy of this object to the stats of both parties
    fn count(self, src: &mut SyncPartyStats, dst: &mut SyncPartyStats) {
        match self {
            Self::Data(data_slice) => {
                src.data_slices_read += 1;
                dst.data_slices_written += 1;
                src.data_records_read += data_slice.num_records();
                dst.data_records_written += data_slice.num_records();
            }
            Self::Checkpoint(_) => {
                src.checkpoints_read += 1;
                dst.checkpoints_written += 1;
            }
        }
        src.bytes_read += self.size();
        dst.bytes_written += self.size();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
