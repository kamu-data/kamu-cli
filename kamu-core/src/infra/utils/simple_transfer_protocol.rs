// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use crate::domain::sync_service::DatasetNotFoundError;
use opendatafabric::*;

use tracing::*;
use futures::TryStreamExt;

/// Implements "Simple Transfer Protocol" as described in ODF spec
pub struct SimpleTransferProtocol;

impl SimpleTransferProtocol {

    // TODO: PERF: Parallelism opportunity for data and checkpoint downloads (need to ensure repos are Sync)
    pub async fn sync(
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
            Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        let dst_head = match dst_chain.get_ref(&BlockRef::Head).await {
            Ok(h) => Ok(Some(h)),
            Err(GetRefError::NotFound(_)) => Ok(None),
            Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        info!(?src_head, ?dst_head, "Resolved heads");

        if Some(&src_head) == dst_head.as_ref() {
            return Ok(SyncResult::UpToDate);
        }

        // Download missing blocks
        let blocks: Vec<_> = match src_chain
            .iter_blocks_interval(&src_head, dst_head.as_ref())
            .try_collect()
            .await
        {
            Ok(v) => Ok(v),
            Err(IterBlocksError::RefNotFound(e)) => Err(SyncError::Internal(e.int_err())),
            Err(IterBlocksError::BlockNotFound(e)) => Err(CorruptedSourceError {
                message: "Source metadata chain is broken".to_owned(),
                source: Some(e.into()),
            }
            .into()),
            Err(IterBlocksError::InvalidInterval(e)) => Err(DatasetsDivergedError {
                src_head: e.head,
                dst_head: e.tail,
            }
            .into()),
            Err(IterBlocksError::Access(e)) => Err(SyncError::Access(e)),
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
                    Err(GetError::Access(e)) => Err(SyncError::Access(e)),
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
                    Err(InsertError::Access(e)) => Err(SyncError::Access(e)),
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
                    Err(GetError::Access(e)) => Err(SyncError::Access(e)),
                    Err(GetError::Internal(e)) => Err(SyncError::Internal(e)),
                }?;

                match dst_checkpoints
                    .insert_stream(
                        stream,
                        InsertOpts {
                            precomputed_hash: if !trust_source_hashes { None } else { Some(&checkpoint.physical_hash) },
                            expected_hash: Some(&checkpoint.physical_hash),
                            // This hint is necessary only for S3 implementation that does not currently support
                            // streaming uploads without knowing Content-Length. We should remove it in future.
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
                    Err(InsertError::Access(e)) => Err(SyncError::Access(e)),
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
                Err(AppendError::RefNotFound(_) | AppendError::RefCASFailed(_)) => unreachable!(),
                Err(AppendError::Access(e)) => Err(SyncError::Access(e)),
                Err(AppendError::Internal(e)) => Err(SyncError::Internal(e)),
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
            Err(SetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(SetRefError::Internal(e)) => Err(SyncError::Internal(e)),
            Err(SetRefError::BlockNotFound(e)) => Err(SyncError::Internal(e.int_err())),
        }?;

        Ok(SyncResult::Updated {
            old_head: dst_head,
            new_head: src_head,
            num_blocks,
        })
    }
}