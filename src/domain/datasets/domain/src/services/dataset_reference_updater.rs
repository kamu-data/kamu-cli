// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ResolvedDataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The single way to move a dataset's reference, such as `HEAD`.
///
/// The `reference_writes` lint in `kamu-repo-tools` rejects direct reference
/// writes elsewhere and lists the allowed exceptions.
#[async_trait::async_trait]
pub trait DatasetReferenceUpdater: Sync + Send {
    /// Moves `block_ref` of `target` from `expected_block_hash` to
    /// `new_block_hash`, as part of the caller's current transaction.
    /// `expected_block_hash = None` means the reference must not exist yet.
    /// The move is never unconditional.
    ///
    /// `target` must be resolved in the current transaction, not detached
    /// from it.
    ///
    /// Errors:
    /// - `CASFailed`: the reference no longer points at `expected_block_hash`. Nothing is
    ///   committed. The conflict is recoverable: a retry plans again from where the reference
    ///   points now.
    /// - `BlockNotFound`: `new_block_hash` is not in the metadata chain.
    async fn update_reference(
        &self,
        target: &ResolvedDataset,
        block_ref: &odf::BlockRef,
        expected_block_hash: Option<&odf::Multihash>,
        new_block_hash: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
