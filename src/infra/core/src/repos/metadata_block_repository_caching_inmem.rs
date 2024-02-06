// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use dashmap::DashMap;
use kamu_core::{
    ContainsError,
    GetBlockError,
    InsertError,
    InsertOpts,
    InsertResult,
    MetadataBlockRepository,
    ObjectRepository,
};
use opendatafabric::{MetadataBlock, Multihash};

use crate::MetadataBlockRepositoryImpl;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataBlockRepositoryCachingInMem<ObjRepo> {
    wrapped: MetadataBlockRepositoryImpl<ObjRepo>,
    cache: DashMap<Multihash, MetadataBlock>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<ObjRepo> MetadataBlockRepositoryCachingInMem<ObjRepo>
where
    ObjRepo: ObjectRepository + Sync + Send,
{
    pub fn new(obj_repo: ObjRepo) -> Self {
        let wrapped = MetadataBlockRepositoryImpl::new(obj_repo);

        Self {
            wrapped,
            cache: DashMap::new(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<ObjRepo> MetadataBlockRepository for MetadataBlockRepositoryCachingInMem<ObjRepo>
where
    ObjRepo: ObjectRepository + Sync + Send,
{
    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError> {
        if self.cache.contains_key(hash) {
            return Ok(true);
        }

        self.wrapped.contains(hash).await
    }

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

    async fn insert<'a>(
        &'a self,
        block: &MetadataBlock,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let insert_result = self.wrapped.insert(block, options).await;

        if let Ok(result) = &insert_result {
            self.cache.insert(result.hash.clone(), block.clone());
        }

        insert_result
    }

    fn as_object_repo(&self) -> &dyn ObjectRepository {
        self.wrapped.as_object_repo()
    }
}
