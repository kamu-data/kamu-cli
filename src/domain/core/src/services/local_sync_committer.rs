// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{SyncError, SyncResult, TransferredSync};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Commits what [`crate::SyncService::transfer_into_local`] wrote, inside the
/// caller's transaction: a compare-and-swap of `HEAD` from the head the
/// transfer started from, or the creation of a new dataset with `HEAD` at the
/// transferred head. Fails with [`SyncError::HeadMoved`] when `HEAD` moved
/// meanwhile, and with the creation errors for a new dataset
#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait LocalSyncCommitter: Send + Sync {
    async fn commit(&self, transferred: TransferredSync) -> Result<SyncResult, SyncError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
