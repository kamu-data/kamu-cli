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

        let src_head = self.get_src_head(src_ref, src_chain).await?;
        let dst_head = self.get_dest_head(dst_chain).await?;

        info!(?src_head, ?dst_head, "Resolved heads");

        let chains_comparison = self
            .compare_chains(src_chain, &src_head, dst_chain, dst_head.as_ref())
            .await?;

        match chains_comparison {
            ChainsComparison::Equal => return Ok(SyncResult::UpToDate),
            ChainsComparison::SourceAhead { .. } => { /* Skip */ }
            ChainsComparison::DestinationAhead { dst_ahead_size } => {
                if !force {
                    return Err(SyncError::DestinationAhead(DestinationAheadError {
                        src_head: src_head,
                        dst_head: dst_head.unwrap(),
                        dst_ahead_size: dst_ahead_size,
                    }));
                }
            }
            ChainsComparison::Divergence {
                uncommon_blocks_in_dst,
                uncommon_blocks_in_src,
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
            ChainsComparison::SourceAhead { src_ahead_blocks } => src_ahead_blocks,
            ChainsComparison::DestinationAhead { .. } | ChainsComparison::Divergence { .. } => {
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

    pub async fn compare_chains(
        &self,
        src_chain: &dyn MetadataChain,
        src_head: &Multihash,
        dst_chain: &dyn MetadataChain,
        dst_head: Option<&Multihash>,
    ) -> Result<ChainsComparison, SyncError> {
        // When source and destination point to the same block, chains are equal, no synchronization required
        if Some(&src_head) == dst_head.as_ref() {
            return Ok(ChainsComparison::Equal);
        }

        // Extract sequence number of head blocks
        let src_sequence_number = src_chain.get_block(&src_head).await?.sequence_number;
        let dst_sequence_number = if dst_head.is_some() {
            dst_chain
                .get_block(dst_head.as_ref().unwrap())
                .await?
                .sequence_number
        } else {
            -1
        };

        // If numbers are equal, it's a guaranteed divergence, as we've checked blocks for equality above
        if src_sequence_number == dst_sequence_number {
            let last_common_sequence_number = self
                .locate_last_common_block(
                    src_chain,
                    src_head,
                    src_sequence_number,
                    dst_chain,
                    dst_head,
                    dst_sequence_number,
                )
                .await?;
            return Ok(self.describe_divergence(
                dst_sequence_number,
                src_sequence_number,
                last_common_sequence_number,
            ));
        }
        // Source ahead
        else if src_sequence_number > dst_sequence_number {
            let convergence_check = self
                .detect_convergence(
                    src_sequence_number,
                    src_chain,
                    &src_head,
                    dst_sequence_number,
                    dst_chain,
                    dst_head,
                )
                .await?;
            match convergence_check {
                ConvergenceCheck::Converged { ahead_blocks } => {
                    return Ok(ChainsComparison::SourceAhead {
                        src_ahead_blocks: ahead_blocks,
                    })
                }
                ConvergenceCheck::Diverged {
                    last_common_sequence_number,
                } => {
                    return Ok(self.describe_divergence(
                        dst_sequence_number,
                        src_sequence_number,
                        last_common_sequence_number,
                    ));
                }
            }
        }
        // Destination ahead
        else {
            let convergence_check = self
                .detect_convergence(
                    dst_sequence_number,
                    dst_chain,
                    dst_head.as_ref().unwrap(),
                    src_sequence_number,
                    src_chain,
                    Some(&src_head),
                )
                .await?;
            match convergence_check {
                ConvergenceCheck::Converged { ahead_blocks } => {
                    return Ok(ChainsComparison::DestinationAhead {
                        dst_ahead_size: ahead_blocks.len(),
                    })
                }
                ConvergenceCheck::Diverged {
                    last_common_sequence_number,
                } => {
                    return Ok(self.describe_divergence(
                        dst_sequence_number,
                        src_sequence_number,
                        last_common_sequence_number,
                    ));
                }
            }
        }
    }

    async fn detect_convergence(
        &self,
        ahead_sequence_number: i32,
        ahead_chain: &dyn MetadataChain,
        ahead_head: &Multihash,
        baseline_sequence_number: i32,
        baseline_chain: &dyn MetadataChain,
        baseline_head: Option<&Multihash>,
    ) -> Result<ConvergenceCheck, SyncError> {
        let ahead_size: usize = (ahead_sequence_number - baseline_sequence_number)
            .try_into()
            .unwrap();
        let ahead_blocks = self
            .map_block_iteration_errors(ahead_chain.take_n_blocks(ahead_head, ahead_size).await)?;
        // If last read block points to the previous hash that is identical to earlier head, there is no divergence
        let boundary_ahead_block_data = ahead_blocks.last().map(|el| &(el.1)).unwrap();
        let boundary_block_prev_hash = boundary_ahead_block_data.prev_block_hash.as_ref();
        if baseline_head.is_some() && baseline_head != boundary_block_prev_hash {
            let last_common_sequence_number = self
                .locate_last_common_block(
                    ahead_chain,
                    ahead_head,
                    ahead_sequence_number - ahead_size as i32,
                    baseline_chain,
                    baseline_head,
                    baseline_sequence_number,
                )
                .await?;
            Ok(ConvergenceCheck::Diverged {
                last_common_sequence_number,
            })
        } else {
            Ok(ConvergenceCheck::Converged { ahead_blocks })
        }
    }

    fn describe_divergence(
        &self,
        dst_sequence_number: i32,
        src_sequence_number: i32,
        last_common_sequence_number: i32,
    ) -> ChainsComparison {
        ChainsComparison::Divergence {
            uncommon_blocks_in_dst: (dst_sequence_number - last_common_sequence_number)
                .try_into()
                .unwrap(),
            uncommon_blocks_in_src: (src_sequence_number - last_common_sequence_number)
                .try_into()
                .unwrap(),
        }
    }

    async fn locate_last_common_block(
        &self,
        src_chain: &dyn MetadataChain,
        src_head: &Multihash,
        src_start_block_sequence_number: i32,
        dst_chain: &dyn MetadataChain,
        dst_head: Option<&Multihash>,
        dst_start_block_sequence_number: i32,
    ) -> Result<i32, SyncError> {
        if dst_head.is_none() {
            return Ok(-1);
        }

        let mut src_stream = src_chain.iter_blocks_interval(src_head, None, false);
        let mut dst_stream = dst_chain.iter_blocks_interval(dst_head.unwrap(), None, false);

        let mut curr_src_block_sequence_number = src_start_block_sequence_number;
        while curr_src_block_sequence_number > dst_start_block_sequence_number {
            src_stream.try_next().await.int_err()?;
            curr_src_block_sequence_number -= 1;
        }

        let mut curr_dst_block_sequence_number = dst_start_block_sequence_number;
        while curr_dst_block_sequence_number > src_start_block_sequence_number {
            dst_stream.try_next().await.int_err()?;
            curr_dst_block_sequence_number -= 1;
        }

        assert_eq!(
            curr_src_block_sequence_number,
            curr_dst_block_sequence_number
        );

        let mut curr_block_sequence_number = curr_src_block_sequence_number;
        while curr_block_sequence_number >= 0 {
            let (src_block_hash, _) = src_stream.try_next().await.int_err()?.unwrap();
            let (dst_block_hash, _) = dst_stream.try_next().await.int_err()?.unwrap();
            if src_block_hash == dst_block_hash {
                return Ok(curr_block_sequence_number);
            }
            curr_block_sequence_number -= 1;
        }

        Ok(-1)
    }
}

pub enum ChainsComparison {
    Equal,
    SourceAhead {
        src_ahead_blocks: Vec<(Multihash, MetadataBlock)>,
    },
    DestinationAhead {
        dst_ahead_size: usize,
    },
    Divergence {
        uncommon_blocks_in_dst: usize,
        uncommon_blocks_in_src: usize,
    },
}

enum ConvergenceCheck {
    Converged {
        ahead_blocks: Vec<(Multihash, MetadataBlock)>,
    },
    Diverged {
        last_common_sequence_number: i32,
    },
}
