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

/// The single way to move a dataset's `HEAD` reference.
///
/// The `reference_writes` lint in `kamu-repo-tools` rejects direct reference
/// writes elsewhere and lists the allowed exceptions.
#[async_trait::async_trait]
pub trait DatasetHeadCommitter: Sync + Send {
    /// Moves `HEAD` of `target` from `expected_head` to `new_head`, as part of
    /// the caller's current transaction. `expected_head = None` means the
    /// dataset must not have a `HEAD` yet. The move is never unconditional.
    ///
    /// `target` must be resolved in the current transaction, not detached
    /// from it.
    ///
    /// Errors:
    /// - `CASFailed`: `HEAD` no longer points at `expected_head`. Nothing is committed. The
    ///   conflict is recoverable: a retry plans again from the current `HEAD`.
    /// - `BlockNotFound`: `new_head` is not in the metadata chain.
    async fn commit_head(
        &self,
        target: &ResolvedDataset,
        expected_head: Option<&odf::Multihash>,
        new_head: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
