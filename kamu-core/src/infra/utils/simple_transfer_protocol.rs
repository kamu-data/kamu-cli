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

use futures::TryStreamExt;
use opendatafabric::dynamic::MetadataBlock;
use tracing::*;

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
        force: bool,
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

        // By this point we know the heads are different.

        // Get sequence numbers of heads in both chains
        let src_block_sequence_number = src_chain.get_block(&src_head).await?.sequence_number();
        let dst_block_sequence_number = if dst_head.is_some() {
            dst_chain
                .get_block(dst_head.as_ref().unwrap())
                .await?
                .sequence_number()
        } else {
            -1
        };

        // Compute how many blocks ahead is the source over destination.
        // Note: the number could be 0 or negative, this means dataset have diverged for sure.
        // If the difference is positive, we don't know if they've diverged without iterating
        let src_ahead_size: usize = (src_block_sequence_number - dst_block_sequence_number)
            .try_into()
            .unwrap_or(0);

        // Extract source blocks that are ahead of destination, if any
        let (src_ahead_blocks, diverged) = if src_ahead_size > 0 {
            // Read N blocks, N = difference between head sequence numbers
            let ahead_blocks = self.map_block_iteration_errors(
                src_chain.take_n_blocks(&src_head, src_ahead_size).await,
            )?;

            // If last read block points to the previous hash that is identical to destination head, there is no divergence
            let boundary_ahead_block_data = ahead_blocks.last().map(|el| &(el.1)).unwrap();
            let boundary_block_prev_hash = boundary_ahead_block_data.prev_block_hash();
            let diverged = dst_head.is_some() && { dst_head.as_ref() != boundary_block_prev_hash };
            (ahead_blocks, diverged)
        } else {
            // Source is not ahead of dest, destination has diverged
            (vec![], true)
        };

        // Divergence is an error unless --force mode is requested
        if diverged && !force {
            return Err(SyncError::DatasetsDiverged(DatasetsDivergedError {
                src_head: src_head,
                dst_head: dst_head.unwrap(),
            }));
        }

        // Collect blocks to synchronize:
        //  - diverged && force: collect full chain
        //  - non-diverged: take only the source ahead part
        let blocks = if diverged {
            // Divergence with some source blocks ahead
            if src_ahead_size > 0 {
                // Anything preceeding already loaded blocks in the source chain?
                let boundary_ahead_block_data = src_ahead_blocks.last().unwrap();
                let boundary_block_prev_hash = boundary_ahead_block_data.1.prev_block_hash();
                if boundary_block_prev_hash.is_some() {
                    // Append what's remaining until the tail to what is already loaded.
                    let remaining_blocks = self.map_block_iteration_errors(
                        src_chain
                            .iter_blocks_interval(&boundary_ahead_block_data.0, None, false)
                            .try_collect()
                            .await,
                    )?;
                    src_ahead_blocks
                        .into_iter()
                        .chain(remaining_blocks.into_iter())
                        .collect()
                } else {
                    // Nothing there, only source ahead
                    src_ahead_blocks
                }
            } else {
                // Load all source blocks from head to tail
                self.map_block_iteration_errors(
                    src_chain
                        .iter_blocks_interval(&src_head, None, false)
                        .try_collect()
                        .await,
                )?
            }
        } else {
            // No divergence, simply synchronize source ahead blocks
            src_ahead_blocks
        };

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
                            precomputed_hash: if !trust_source_hashes {
                                None
                            } else {
                                Some(&data_slice.physical_hash)
                            },
                            expected_hash: Some(&data_slice.physical_hash),
                            size_hint: Some(data_slice.size as usize),
                            ..Default::default()
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
                            precomputed_hash: if !trust_source_hashes {
                                None
                            } else {
                                Some(&checkpoint.physical_hash)
                            },
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
            }
        }

        // Commit blocks
        for (hash, block) in blocks.into_iter().rev() {
            debug!(?hash, "Appending block");

            match dst_chain
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
                Err(AppendError::InvalidBlock(AppendValidationError::HashMismatch(e))) => {
                    Err(CorruptedSourceError {
                        message: concat!(
                            "Block hash declared by the source didn't match ",
                            "the computed - this may be an indication of hashing ",
                            "algorithm mismatch or an attempted tampering",
                        )
                        .to_owned(),
                        source: Some(e.into()),
                    }
                    .into())
                }
                Err(AppendError::InvalidBlock(e)) => Err(CorruptedSourceError {
                    message: "Source metadata chain is logically inconsistent".to_owned(),
                    source: Some(e.into()),
                }
                .into()),
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

        // Download kamu-specific cache files (if exist)
        // TODO: This is not part of the ODF spec and should be revisited.
        // See also: IPFS sync procedure.
        // Ticket: https://www.notion.so/Where-to-store-ingest-checkpoints-4d48e8db656042168f94a8ab2793daef
        let cache_files = ["fetch.yaml", "prep.yaml", "read.yaml", "commit.yaml"];
        for name in &cache_files {
            use crate::domain::repos::named_object_repository::GetError;

            match src.as_cache_repo().get(name).await {
                Ok(data) => {
                    dst.as_cache_repo()
                        .set(name, data.as_ref())
                        .await
                        .int_err()?;
                    Ok(())
                }
                Err(GetError::NotFound(_)) => Ok(()),
                Err(GetError::Access(e)) => Err(SyncError::Access(e)),
                Err(GetError::Internal(e)) => Err(SyncError::Internal(e)),
            }?;
        }

        Ok(SyncResult::Updated {
            old_head: dst_head,
            new_head: src_head,
            num_blocks,
        })
    }

    fn map_block_iteration_errors<T>(
        &self,
        iteration_result: Result<Vec<T>, IterBlocksError>,
    ) -> Result<Vec<T>, SyncError> {
        match iteration_result {
            Ok(v) => Ok(v),
            Err(IterBlocksError::RefNotFound(e)) => Err(SyncError::Internal(e.int_err())),
            Err(IterBlocksError::BlockNotFound(e)) => Err(CorruptedSourceError {
                message: "Source metadata chain is broken".to_owned(),
                source: Some(e.into()),
            }
            .into()),
            Err(IterBlocksError::BlockVersion(e)) => Err(CorruptedSourceError {
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
        }
    }
}
