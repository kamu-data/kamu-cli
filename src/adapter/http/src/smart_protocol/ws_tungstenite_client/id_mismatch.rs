// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::ResultIntoInternal;
use kamu_core::utils::metadata_chain_comparator::{
    ChainWithHead,
    CompareChainsResult,
    MetadataChainComparator,
    NullCompareChainsListener,
};
use kamu_core::*;
use kamu_datasets::ResolvedDataset;
use odf::dataset::MetadataChainExt;
use url::Url;

use super::WsSmartTransferProtocolClient;
use crate::smart_protocol::remote_dataset_id::try_fetch_remote_dataset_id;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WsSmartTransferProtocolClient {
    /// Compares the ID of a remote source with the local destination's.
    /// Returns `None` when they match or the remote ID is unavailable.
    pub(super) async fn find_pull_dataset_id_mismatch(
        http_src_url: &Url,
        maybe_access_token: Option<&str>,
        dst: &ResolvedDataset,
    ) -> Option<DatasetIdMismatchError> {
        let src_dataset_id = try_fetch_remote_dataset_id(http_src_url, maybe_access_token).await?;
        let dst_dataset_id = &dst.get_handle().id;

        (src_dataset_id != *dst_dataset_id).then(|| DatasetIdMismatchError {
            src_dataset_id,
            dst_dataset_id: dst_dataset_id.clone(),
        })
    }

    /// Compares the ID of a local source with the remote destination's.
    /// Returns `None` when they match or either ID is unavailable.
    pub(super) async fn find_push_dataset_id_mismatch(
        &self,
        src: &dyn odf::Dataset,
        http_dst_url: &Url,
    ) -> Option<DatasetIdMismatchError> {
        let src_dataset_id = match src
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSeedVisitor::new())
            .await
        {
            Ok(seed_visitor) => seed_visitor.into_event()?.dataset_id,
            Err(err) => {
                tracing::debug!(error = ?err, "Failed to read source dataset seed");
                return None;
            }
        };

        let maybe_access_token = self.access_token(http_dst_url);
        let dst_dataset_id =
            try_fetch_remote_dataset_id(http_dst_url, maybe_access_token.as_deref()).await?;

        (src_dataset_id != dst_dataset_id).then_some(DatasetIdMismatchError {
            src_dataset_id,
            dst_dataset_id,
        })
    }

    /// One side's head is missing from the other's chain, as `e` reports with
    /// the source head as `head` and the destination head as `tail`. Compares
    /// the chains, as the Simple protocol does, to tell a destination that is
    /// ahead from diverged histories and from another dataset. `None` when the
    /// remote chain cannot be read or the comparison finds no mismatch
    pub(super) async fn compare_heads_with_remote(
        &self,
        local_chain: &dyn odf::MetadataChain,
        remote_url: &Url,
        remote_is_src: bool,
        e: &odf::dataset::InvalidIntervalError,
    ) -> Option<SyncError> {
        let remote = match self
            .catalog()
            .get_one::<dyn odf::dataset::DatasetFactory>()
            .int_err()
        {
            Ok(dataset_factory) => dataset_factory
                .get_dataset(remote_url, false)
                .await
                .int_err(),
            Err(err) => Err(err),
        }
        .inspect_err(|err| tracing::debug!(error = ?err, "Failed to open the remote dataset"))
        .ok()?;

        let (src_chain, dst_chain) = if remote_is_src {
            (remote.as_metadata_chain(), local_chain)
        } else {
            (local_chain, remote.as_metadata_chain())
        };
        let comparison = MetadataChainComparator::compare_chains(
            src_chain,
            &e.head,
            Some(ChainWithHead {
                chain: dst_chain,
                head: &e.tail,
            }),
            &NullCompareChainsListener,
        )
        .await
        .inspect_err(|err| tracing::debug!(error = ?err, "Failed to compare the chains"))
        .ok()?;

        match comparison {
            CompareChainsResult::LhsBehind { rhs_ahead_blocks } => {
                Some(SyncError::DestinationAhead(DestinationAheadError {
                    src_head: e.head.clone(),
                    dst_head: e.tail.clone(),
                    dst_ahead_size: rhs_ahead_blocks.len(),
                }))
            }
            CompareChainsResult::Divergence {
                uncommon_blocks_in_lhs,
                uncommon_blocks_in_rhs,
            } => Some(SyncError::DatasetsDiverged(DatasetsDivergedError {
                src_head: e.head.clone(),
                dst_head: e.tail.clone(),
                detail: Some(DatasetsDivergedErrorDetail {
                    uncommon_blocks_in_src: uncommon_blocks_in_lhs,
                    uncommon_blocks_in_dst: uncommon_blocks_in_rhs,
                }),
            })),
            CompareChainsResult::DifferentDatasets {
                lhs_dataset_id,
                rhs_dataset_id,
            } => Some(SyncError::DatasetIdMismatch(DatasetIdMismatchError {
                src_dataset_id: lhs_dataset_id,
                dst_dataset_id: rhs_dataset_id,
            })),
            CompareChainsResult::Equal | CompareChainsResult::LhsAhead { .. } => None,
        }
    }

    /// One side's head is missing from the other's chain: the chains diverged,
    /// unless the IDs show these are different datasets
    pub(super) fn diverged_or_id_mismatch(
        e: odf::dataset::InvalidIntervalError,
        maybe_mismatch: Option<DatasetIdMismatchError>,
    ) -> SyncError {
        match maybe_mismatch {
            Some(mismatch) => mismatch.into(),
            None => SyncError::DatasetsDiverged(DatasetsDivergedError {
                src_head: e.head,
                dst_head: e.tail,
                detail: None,
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
