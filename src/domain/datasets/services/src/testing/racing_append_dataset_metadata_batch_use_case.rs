// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use dill::{component, interface, scope};
use kamu_datasets::{
    AppendDatasetMetadataBatchUseCase,
    AppendDatasetMetadataBatchUseCaseError,
    AppendDatasetMetadataBatchUseCaseOptions,
    DatasetHeadCommitter,
    ResolvedDataset,
};

use super::ConcurrentHeadMove;
use crate::AppendDatasetMetadataBatchUseCaseImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Appends like `AppendDatasetMetadataBatchUseCaseImpl`, but when armed first
/// moves `HEAD` the way a concurrent writer would before the batch arrives, so
/// the move lands before the commit decides which head it expects.
/// `RacingDatasetHeadCommitter` moves it later, after that decision
pub struct RacingAppendDatasetMetadataBatchUseCase {
    inner: AppendDatasetMetadataBatchUseCaseImpl,
    concurrent_head_move: Mutex<Option<ConcurrentHeadMove>>,
}

#[component(pub)]
#[interface(dyn AppendDatasetMetadataBatchUseCase)]
#[scope(dill::Singleton)]
impl RacingAppendDatasetMetadataBatchUseCase {
    pub fn new(dataset_head_committer: Arc<dyn DatasetHeadCommitter>) -> Self {
        Self {
            inner: AppendDatasetMetadataBatchUseCaseImpl::new(dataset_head_committer),
            concurrent_head_move: Mutex::new(None),
        }
    }

    /// Arms the next `execute` to move `HEAD` to `concurrent_head` first, in a
    /// transaction of its own made from `catalog`
    pub fn move_head_before_next_append(
        &self,
        catalog: &dill::Catalog,
        concurrent_head: odf::Multihash,
    ) {
        *self.concurrent_head_move.lock().unwrap() =
            Some(ConcurrentHeadMove::new(catalog, concurrent_head));
    }
}

#[async_trait::async_trait]
impl AppendDatasetMetadataBatchUseCase for RacingAppendDatasetMetadataBatchUseCase {
    async fn execute(
        &self,
        target: &ResolvedDataset,
        new_blocks_into_it: Box<
            dyn Iterator<Item = odf::dataset::HashedMetadataBlockBytesDecoded> + Send,
        >,
        options: AppendDatasetMetadataBatchUseCaseOptions,
    ) -> Result<Option<odf::Multihash>, AppendDatasetMetadataBatchUseCaseError> {
        let concurrent_head_move = self.concurrent_head_move.lock().unwrap().take();
        if let Some(concurrent_head_move) = concurrent_head_move {
            concurrent_head_move.apply(target.get_handle()).await;
        }

        self.inner
            .execute(target, new_blocks_into_it, options)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
