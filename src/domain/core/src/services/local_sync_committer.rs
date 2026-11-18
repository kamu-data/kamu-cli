// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{SyncError, SyncFromTransferred, SyncResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Commits what [`crate::SyncService::sync_from`] wrote, in the caller's
/// transaction: a compare-and-swap of `HEAD` from the head the transfer
/// started from, or the creation of a new dataset with `HEAD` at the
/// transferred head
#[async_trait::async_trait]
pub trait LocalSyncCommitter: Send + Sync {
    /// `None` when the existing target was deleted since the transfer;
    /// [`SyncError::RefMoved`] when its reference moved
    async fn commit(
        &self,
        transferred: SyncFromTransferred,
    ) -> Result<Option<SyncResult>, SyncError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
