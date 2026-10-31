// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use kamu_datasets::{DatasetHeadCommitter, ResolvedDataset};

use super::ConcurrentHeadMove;
use crate::DatasetHeadCommitterImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Commits like `DatasetHeadCommitterImpl`, but when armed first moves `HEAD`
/// as a concurrent writer would between the caller's read and its commit, in a
/// transaction of its own, or denies the commit
#[derive(Default)]
pub struct RacingDatasetHeadCommitter {
    concurrent_head_move: Mutex<Option<ConcurrentHeadMove>>,
    deny_next_commit: Mutex<bool>,
}

impl RacingDatasetHeadCommitter {
    /// Arms the next `commit_head` to move `HEAD` to `concurrent_head` first, in
    /// a transaction of its own made from `catalog`
    pub fn move_head_before_next_commit(
        &self,
        catalog: &dill::Catalog,
        concurrent_head: odf::Multihash,
    ) {
        *self.concurrent_head_move.lock().unwrap() =
            Some(ConcurrentHeadMove::new(catalog, concurrent_head));
    }

    /// Arms the next `commit_head` to fail as not allowed, moving nothing
    pub fn deny_next_commit(&self) {
        *self.deny_next_commit.lock().unwrap() = true;
    }
}

#[async_trait::async_trait]
impl DatasetHeadCommitter for RacingDatasetHeadCommitter {
    async fn commit_head(
        &self,
        target: &ResolvedDataset,
        expected_head: Option<&odf::Multihash>,
        new_head: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        if std::mem::take(&mut *self.deny_next_commit.lock().unwrap()) {
            return Err(odf::dataset::SetChainRefError::Access(
                odf::AccessError::Unauthorized("Commit denied by the test".into()),
            ));
        }

        let concurrent_head_move = self.concurrent_head_move.lock().unwrap().take();
        if let Some(concurrent_head_move) = concurrent_head_move {
            concurrent_head_move.apply(target.get_handle()).await;
        }

        DatasetHeadCommitterImpl {}
            .commit_head(target, expected_head, new_head)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
