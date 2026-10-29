// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::TryStreamExt;
use internal_error::ErrorIntoInternal;
use kamu_core::utils::metadata_chain_comparator::*;
use kamu_core::*;

use super::progress::CompareChainsListenerAdapter;
use super::protocol::SimpleTransferProtocol;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SimpleTransferProtocol {
    /// Compares the chains and returns the blocks the destination lacks, or
    /// `None` when it is up to date. `dst` is the destination chain with its
    /// head, or `None` when it has no blocks
    pub(super) async fn collect_new_blocks(
        src_ref: &odf::DatasetRefAny,
        src_chain: &dyn odf::MetadataChain,
        dst: Option<ChainWithHead<'_>>,
        force: bool,
        listener_adapter: &CompareChainsListenerAdapter,
    ) -> Result<Option<NewBlocks>, SyncError> {
        let src_head = Self::get_src_head(src_ref, src_chain).await?;
        let dst_head = dst.map(|dst| dst.head);

        tracing::info!(?src_head, ?dst_head, "Resolved heads");

        let chains_comparison =
            MetadataChainComparator::compare_chains(src_chain, &src_head, dst, listener_adapter)
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
                Self::load_all_blocks(src_chain, &src_head).await?
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
                Self::load_all_blocks(src_chain, &src_head).await?
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

        Ok(Some(NewBlocks {
            src_head,
            blocks_desc_ordered,
        }))
    }

    /// The whole source chain from `src_head`, newest first, which a forced
    /// sync writes over a destination that is ahead or has diverged
    async fn load_all_blocks(
        src_chain: &dyn odf::MetadataChain,
        src_head: &odf::Multihash,
    ) -> Result<
        Vec<(
            odf::Multihash,
            odf::MetadataBlockHeader,
            odf::MetadataBlockBytes,
        )>,
        SyncError,
    > {
        src_chain
            .iter_blocks_bytes_interval(src_head.into(), None, false)
            .try_collect()
            .await
            .map_err(Self::map_block_iteration_error)
    }

    async fn get_src_head(
        src_ref: &odf::DatasetRefAny,
        src_chain: &dyn odf::MetadataChain,
    ) -> Result<odf::Multihash, SyncError> {
        try_resolve_head(src_chain).await?.ok_or_else(|| {
            DatasetAnyRefUnresolvedError {
                dataset_ref: src_ref.clone(),
            }
            .into()
        })
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The blocks a destination lacks, up to the source head they were read from
pub(super) struct NewBlocks {
    pub(super) src_head: odf::Multihash,
    /// Newest first
    pub(super) blocks_desc_ordered: Vec<odf::dataset::HashedMetadataBlockBytesDecoded>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
