// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use kamu_core::{
    ContainsError,
    GetBlockError,
    GetError,
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
    cache: DashMap<Multihash, Bytes>,
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
        match self.get_block_data(hash).await {
            Ok(_) => Ok(true),
            Err(e) => match e {
                GetError::NotFound(_) => Ok(false),
                GetError::Access(e) => Err(ContainsError::Access(e)),
                GetError::Internal(e) => Err(ContainsError::Internal(e)),
            },
        }
    }

    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError> {
        let block_data = self.get_block_data(hash).await?;
        let block =
            MetadataBlockRepositoryImpl::<ObjRepo>::deserialize_metadata_block(hash, &block_data)?;

        Ok(block)
    }

    async fn get_block_data(&self, hash: &Multihash) -> Result<Bytes, GetError> {
        if let Some(cached_result) = self.cache.get(hash) {
            return Ok(cached_result.clone());
        };

        let get_block_data_result = self.wrapped.get_block_data(hash).await;

        if let Ok(block_data) = &get_block_data_result {
            self.cache.insert(hash.clone(), block_data.clone());
        }

        get_block_data_result
    }

    async fn get_size(&self, hash: &Multihash) -> Result<u64, GetError> {
        let block_data = self.get_block_data(hash).await?;
        let size = u64::try_from(block_data.len()).unwrap();

        Ok(size)
    }

    async fn insert_block_data<'a>(
        &'a self,
        block_data: &'a [u8],
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let insert_result = self.wrapped.insert_block_data(block_data, options).await;

        if let Ok(result) = &insert_result {
            let copied_block_data = Bytes::copy_from_slice(block_data);

            self.cache.insert(result.hash.clone(), copied_block_data);
        }

        insert_result
    }

    fn as_object_repo(&self) -> &dyn ObjectRepository {
        self.wrapped.as_object_repo()
    }
}
