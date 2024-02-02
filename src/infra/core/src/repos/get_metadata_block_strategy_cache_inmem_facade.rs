// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use kamu_core::GetBlockError;
use opendatafabric::{MetadataBlock, Multihash};

use crate::GetMetadataBlockStrategy;

/////////////////////////////////////////////////////////////////////////////////////////

pub type HashMetadataBlockCacheMap = DashMap<Multihash, MetadataBlock>;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct GetMetadataBlockStrategyCacheInMemoryFacade {
    strategy: Box<dyn GetMetadataBlockStrategy>,
    cache: Arc<HashMetadataBlockCacheMap>,
}

impl GetMetadataBlockStrategyCacheInMemoryFacade {
    pub fn new_boxed(
        strategy: Box<dyn GetMetadataBlockStrategy>,
        cache: Arc<HashMetadataBlockCacheMap>,
    ) -> Box<Self> {
        Box::new(Self { strategy, cache })
    }
}

#[async_trait]
impl GetMetadataBlockStrategy for GetMetadataBlockStrategyCacheInMemoryFacade {
    async fn get(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError> {
        if let Some(cached_result) = self.cache.get(hash) {
            return Ok(cached_result.clone());
        };

        let get_result = self.strategy.get(hash).await;

        if let Ok(block) = &get_result {
            self.cache.insert(hash.clone(), block.clone());
        }

        get_result
    }
}
