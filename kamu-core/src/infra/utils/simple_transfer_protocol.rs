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
use opendatafabric::MetadataBlock;
use tracing::*;

/////////////////////////////////////////////////////////////////////////////////////////

/// Implements "Simple Transfer Protocol" as described in ODF spec
pub struct SimpleTransferProtocol;

/////////////////////////////////////////////////////////////////////////////////////////

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

        let src_head = self.get_src_head(src_ref, src_chain).await?;
        let dst_head = self.get_dest_head(dst_chain).await?;

        info!(?src_head, ?dst_head, "Resolved heads");

        let chains_comparison = MetadataChainComparator::compare_chains(
            src_chain,
            &src_head,
            dst_chain,
            dst_head.as_ref(),
        )
        .await?;

        match chains_comparison {
            ChainsComparison::Equal => return Ok(SyncResult::UpToDate),
            ChainsComparison::LhsAhead { .. } => { /* Skip */ }
            ChainsComparison::RhsAhead {
                ref rhs_ahead_blocks,
            } => {
                if !force {
                    return Err(SyncError::DestinationAhead(DestinationAheadError {
                        src_head: src_head,
                        dst_head: dst_head.unwrap(),
                        dst_ahead_size: rhs_ahead_blocks.len(),
                    }));
                }
            }
            ChainsComparison::Divergence {
                uncommon_blocks_in_lhs: uncommon_blocks_in_src,
                uncommon_blocks_in_rhs: uncommon_blocks_in_dst,
            } => {
                if !force {
                    return Err(SyncError::DatasetsDiverged(DatasetsDivergedError {
                        src_head: src_head,
                        dst_head: dst_head.unwrap(),
                        uncommon_blocks_in_dst,
                        uncommon_blocks_in_src,
                    }));
                }
            }
        };

        let blocks = match chains_comparison {
            ChainsComparison::Equal => unreachable!(),
            ChainsComparison::LhsAhead {
                lhs_ahead_blocks: src_ahead_blocks,
            } => src_ahead_blocks,
            ChainsComparison::RhsAhead { .. } | ChainsComparison::Divergence { .. } => {
                // Load all source blocks from head to tail
                assert!(force);
                self.map_block_iteration_errors(src_chain.iter_blocks().try_collect().await)?
            }
        };

        let num_blocks = blocks.len();

        self.synchronize_blocks(
            blocks,
            src,
            dst,
            &src_head,
            dst_head.as_ref(),
            validation,
            trust_source_hashes,
        )
        .await?;

        Ok(SyncResult::Updated {
            old_head: dst_head,
            new_head: src_head,
            num_blocks,
        })
    }

    async fn get_src_head(
        &self,
        src_ref: &DatasetRefAny,
        src_chain: &dyn MetadataChain,
    ) -> Result<Multihash, SyncError> {
        match src_chain.get_ref(&BlockRef::Head).await {
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
        match dst_chain.get_ref(&BlockRef::Head).await {
            Ok(h) => Ok(Some(h)),
            Err(GetRefError::NotFound(_)) => Ok(None),
            Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
        }
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
            Err(IterBlocksError::InvalidInterval(_)) => unreachable!(),
            Err(IterBlocksError::Access(e)) => Err(SyncError::Access(e)),
            Err(IterBlocksError::Internal(e)) => Err(SyncError::Internal(e)),
        }
    }

    async fn synchronize_blocks(
        &self,
        blocks: Vec<(Multihash, MetadataBlock)>,
        src: &dyn Dataset,
        dst: &dyn Dataset,
        src_head: &Multihash,
        dst_head: Option<&Multihash>,
        validation: AppendValidation,
        trust_source_hashes: bool,
    ) -> Result<(), SyncError> {
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
        match dst
            .as_metadata_chain()
            .set_ref(
                &BlockRef::Head,
                &src_head,
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
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
