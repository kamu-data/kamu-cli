// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use dill::{component, interface, scope};
use kamu_datasets::{
    AppendDatasetMetadataBatchUseCase,
    AppendDatasetMetadataBatchUseCaseError,
    AppendDatasetMetadataBatchUseCaseOptions,
    ResolvedDataset,
};

use super::ConcurrentReferenceMove;
use crate::AppendDatasetMetadataBatchUseCaseImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Appends like `AppendDatasetMetadataBatchUseCaseImpl`, but when armed first
/// moves `HEAD` the way a concurrent writer would before the batch arrives, so
/// the move lands before the commit decides which head it expects.
/// [`DatasetReferenceRace`](super::DatasetReferenceRace) moves it later, after
/// that decision
pub struct RacingAppendDatasetMetadataBatchUseCase {
    inner: AppendDatasetMetadataBatchUseCaseImpl,
    concurrent_reference_move: Mutex<Option<ConcurrentReferenceMove>>,
}

#[component(pub)]
#[interface(dyn AppendDatasetMetadataBatchUseCase)]
#[scope(dill::Singleton)]
impl RacingAppendDatasetMetadataBatchUseCase {
    pub fn new() -> Self {
        Self {
            inner: AppendDatasetMetadataBatchUseCaseImpl {},
            concurrent_reference_move: Mutex::new(None),
        }
    }

    /// Arms the next `execute` to move `HEAD` to `concurrent_head` first, in a
    /// transaction of its own made from `catalog`
    pub fn move_head_before_next_append(
        &self,
        catalog: &dill::Catalog,
        concurrent_head: odf::Multihash,
    ) {
        *self.concurrent_reference_move.lock().unwrap() =
            Some(ConcurrentReferenceMove::new(catalog, concurrent_head));
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
        let concurrent_reference_move = self.concurrent_reference_move.lock().unwrap().take();
        if let Some(concurrent_reference_move) = concurrent_reference_move {
            concurrent_reference_move
                .apply(target.get_id(), &odf::BlockRef::Head)
                .await;
        }

        self.inner
            .execute(target, new_blocks_into_it, options)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
