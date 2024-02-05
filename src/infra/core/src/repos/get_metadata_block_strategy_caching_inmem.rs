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
use kamu_core::{GetBlockError, ObjectRepository};
use opendatafabric::{MetadataBlock, Multihash};

use crate::{GetMetadataBlockStrategy, GetMetadataBlockStrategyImpl};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct GetMetadataBlockStrategyCachingInMem<ObjRepo> {
    wrapped: GetMetadataBlockStrategyImpl<ObjRepo>,
    cache: DashMap<Multihash, MetadataBlock>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<ObjRepo> GetMetadataBlockStrategyCachingInMem<ObjRepo>
where
    ObjRepo: ObjectRepository + Sync + Send,
{
    pub fn new(obj_repo: Arc<ObjRepo>) -> Self {
        let wrapped = GetMetadataBlockStrategyImpl::new(obj_repo);

        Self {
            wrapped,
            cache: DashMap::new(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<ObjRepo> GetMetadataBlockStrategy for GetMetadataBlockStrategyCachingInMem<ObjRepo>
where
    ObjRepo: ObjectRepository + Sync + Send,
{
    async fn get(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError> {
        if let Some(cached_result) = self.cache.get(hash) {
            return Ok(cached_result.clone());
        };

        let get_result = self.wrapped.get(hash).await;

        if let Ok(block) = &get_result {
            self.cache.insert(hash.clone(), block.clone());
        }

        get_result
    }
}
