// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use kamu_datasets::{DatasetReferenceUpdater, ResolvedDataset};

use super::ConcurrentReferenceMove;
use crate::DatasetReferenceUpdaterImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Updates like `DatasetReferenceUpdaterImpl`, but when armed first moves the
/// reference as a concurrent writer would between the caller's read and its
/// update, in a transaction of its own, or denies the update
#[derive(Default)]
pub struct RacingDatasetReferenceUpdater {
    concurrent_reference_move: Mutex<Option<ConcurrentReferenceMove>>,
    deny_next_update: Mutex<bool>,
}

impl RacingDatasetReferenceUpdater {
    /// Arms the next `update_reference` to move the reference to
    /// `concurrent_block_hash` first, in a transaction of its own made from
    /// `catalog`
    pub fn move_reference_before_next_update(
        &self,
        catalog: &dill::Catalog,
        concurrent_block_hash: odf::Multihash,
    ) {
        *self.concurrent_reference_move.lock().unwrap() =
            Some(ConcurrentReferenceMove::new(catalog, concurrent_block_hash));
    }

    /// Arms the next `update_reference` to remove the reference first, in a
    /// transaction of its own made from `catalog`
    pub fn remove_reference_before_next_update(&self, catalog: &dill::Catalog) {
        *self.concurrent_reference_move.lock().unwrap() =
            Some(ConcurrentReferenceMove::removing(catalog));
    }

    /// Arms the next `update_reference` to fail as not allowed, moving nothing
    pub fn deny_next_update(&self) {
        *self.deny_next_update.lock().unwrap() = true;
    }
}

#[async_trait::async_trait]
impl DatasetReferenceUpdater for RacingDatasetReferenceUpdater {
    async fn update_reference(
        &self,
        target: &ResolvedDataset,
        block_ref: &odf::BlockRef,
        expected_block_hash: Option<&odf::Multihash>,
        new_block_hash: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        if std::mem::take(&mut *self.deny_next_update.lock().unwrap()) {
            return Err(odf::dataset::SetChainRefError::Access(
                odf::AccessError::Unauthorized("Reference update denied by the test".into()),
            ));
        }

        let concurrent_reference_move = self.concurrent_reference_move.lock().unwrap().take();
        if let Some(concurrent_reference_move) = concurrent_reference_move {
            concurrent_reference_move
                .apply(target.get_handle(), block_ref)
                .await;
        }

        DatasetReferenceUpdaterImpl {}
            .update_reference(target, block_ref, expected_block_hash, new_block_hash)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
