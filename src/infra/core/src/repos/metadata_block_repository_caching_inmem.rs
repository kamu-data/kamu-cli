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
use internal_error::ErrorIntoInternal;
use kamu_core::{
    ContainsBlockError,
    GetBlockError,
    InsertBlockError,
    InsertBlockResult,
    InsertOpts,
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
    async fn contains_block(&self, hash: &Multihash) -> Result<bool, ContainsBlockError> {
        match self.get_block(hash).await {
            Ok(_) => Ok(true),
            Err(e) => match e {
                GetBlockError::NotFound(_) => Ok(false),
                GetBlockError::Access(e) => Err(ContainsBlockError::Access(e)),
                GetBlockError::Internal(e) => Err(ContainsBlockError::Internal(e)),
                GetBlockError::BlockVersion(e) => Err(ContainsBlockError::Internal(e.int_err())),
                GetBlockError::BlockMalformed(e) => Err(ContainsBlockError::Internal(e.int_err())),
            },
        }
    }

    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError> {
        if let Some(cached_block) = self.cache.get(hash) {
            return Ok(cached_block.clone());
        };

        let get_block_result = self.wrapped.get_block(hash).await;

        if let Ok(block) = &get_block_result {
            self.cache.insert(hash.clone(), block.clone());
        }

        get_block_result
    }

    async fn insert_block<'a>(
        &'a self,
        block: &MetadataBlock,
        options: InsertOpts<'a>,
    ) -> Result<InsertBlockResult, InsertBlockError> {
        let insert_result = self.wrapped.insert_block(block, options).await;

        if let Ok(result) = &insert_result {
            self.cache.insert(result.hash.clone(), block.clone());
        }

        insert_result
    }

    fn as_object_repo(&self) -> &dyn ObjectRepository {
        self.wrapped.as_object_repo()
    }
}
