// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::SyncError;
use odf::dataset::MetadataChainExt as _;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The data a sync added, read from a chain that holds the new blocks. When
/// the new head does not extend the old one, as after a forced sync over a
/// divergence, the whole new chain is counted
pub async fn sync_increment(
    chain: &dyn odf::MetadataChain,
    old_head: Option<&odf::Multihash>,
    new_head: &odf::Multihash,
) -> Result<odf::dataset::MetadataChainIncrementInterval, SyncError> {
    use odf::dataset::GetIncrementError as E;

    // A source chain that diverged from the destination lacks its old head
    let old_head = match old_head {
        Some(old_head) if chain.contains_block(old_head).await.int_err()? => Some(old_head),
        Some(_) | None => None,
    };

    match chain.get_increment_for_interval(old_head, new_head).await {
        Ok(increment) => Ok(increment),
        Err(E::InvalidInterval(_)) => chain
            .get_increment_for_interval(None, new_head)
            .await
            .map_err(|e| SyncError::Internal(e.int_err())),
        Err(e @ (E::BlockNotFound(_) | E::Internal(_))) => Err(SyncError::Internal(e.int_err())),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
