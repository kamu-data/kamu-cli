// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use internal_error::ResultIntoInternal;
use kamu_core::{
    ContainsBlockError,
    GetBlockDataError,
    GetBlockError,
    InsertBlockError,
    InsertBlockResult,
    InsertOpts,
    MetadataBlockRepository,
    ObjectRepository,
};
use opendatafabric::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use opendatafabric::serde::MetadataBlockSerializer;
use opendatafabric::{MetadataBlock, Multihash};

use crate::{MetadataBlockRepositoryExt, MetadataBlockRepositoryImpl};

/////////////////////////////////////////////////////////////////////////////////////////
// Type shortcuts
/////////////////////////////////////////////////////////////////////////////////////////

pub type MetadataBlockRepositoryImplWithCache<ObjRepo> =
    MetadataBlockRepositoryCachingInMem<MetadataBlockRepositoryImpl<ObjRepo>, ObjRepo>;

/////////////////////////////////////////////////////////////////////////////////////////

struct CachedValue {
    block_data: Bytes,
    deserialized_block: Option<MetadataBlock>,
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataBlockRepositoryCachingInMem<WrappedRepo, ObjRepo> {
    wrapped: WrappedRepo,
    cache: DashMap<Multihash, CachedValue>,
    _phantom: PhantomData<ObjRepo>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<WrappedRepo, ObjRepo> MetadataBlockRepositoryCachingInMem<WrappedRepo, ObjRepo>
where
    WrappedRepo: MetadataBlockRepositoryExt<ObjRepo> + Sync + Send,
    ObjRepo: ObjectRepository + Sync + Send,
{
    pub fn new(obj_repo: ObjRepo) -> Self {
        let wrapped = WrappedRepo::new(obj_repo);

        MetadataBlockRepositoryCachingInMem::new_wrapped(wrapped)
    }

    // This is for tests only
    pub fn new_wrapped(wrapped: WrappedRepo) -> Self {
        Self {
            wrapped,
            cache: DashMap::new(),
            _phantom: PhantomData,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<WrappedRepo, ObjRepo> MetadataBlockRepository
    for MetadataBlockRepositoryCachingInMem<WrappedRepo, ObjRepo>
where
    WrappedRepo: MetadataBlockRepository + Sync + Send,
    ObjRepo: ObjectRepository + Sync + Send,
{
    async fn contains_block(&self, hash: &Multihash) -> Result<bool, ContainsBlockError> {
        match self.get_block_data(hash).await {
            Ok(_) => Ok(true),
            Err(e) => match e {
                GetBlockDataError::NotFound(_) => Ok(false),
                GetBlockDataError::Access(e) => Err(ContainsBlockError::Access(e)),
                GetBlockDataError::Internal(e) => Err(ContainsBlockError::Internal(e)),
            },
        }
    }

    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError> {
        match self.cache.get_mut(hash) {
            Some(mut cached_value) => match &cached_value.deserialized_block {
                Some(cached_block) => Ok(cached_block.clone()),
                None => {
                    let block = MetadataBlockRepositoryImpl::<ObjRepo>::deserialize_metadata_block(
                        hash,
                        &cached_value.block_data,
                    )?;

                    cached_value.deserialized_block = Some(block.clone());

                    Ok(block)
                }
            },
            // No cached value
            None => {
                let get_block_data_result = self.wrapped.get_block_data(hash).await;

                match get_block_data_result {
                    Ok(block_data) => {
                        let block =
                            MetadataBlockRepositoryImpl::<ObjRepo>::deserialize_metadata_block(
                                hash,
                                &block_data,
                            )?;
                        let cache_value = CachedValue {
                            block_data: block_data.clone(),
                            deserialized_block: Some(block.clone()),
                        };

                        self.cache.insert(hash.clone(), cache_value);

                        Ok(block)
                    }
                    Err(e) => Err(e.into()),
                }
            }
        }
    }

    async fn get_block_data(&self, hash: &Multihash) -> Result<Bytes, GetBlockDataError> {
        if let Some(cached_value) = self.cache.get(hash) {
            return Ok(cached_value.block_data.clone());
        };

        let get_block_data_result = self.wrapped.get_block_data(hash).await;

        if let Ok(block_data) = &get_block_data_result {
            let cache_value = CachedValue {
                block_data: block_data.clone(),
                deserialized_block: None,
            };

            self.cache.insert(hash.clone(), cache_value);
        }

        get_block_data_result
    }

    async fn get_block_size(&self, hash: &Multihash) -> Result<u64, GetBlockDataError> {
        let block_data = self.get_block_data(hash).await?;
        let size = u64::try_from(block_data.len()).unwrap();

        Ok(size)
    }

    async fn insert_block<'a>(
        &'a self,
        block: &MetadataBlock,
        options: InsertOpts<'a>,
    ) -> Result<InsertBlockResult, InsertBlockError> {
        let block_data = FlatbuffersMetadataBlockSerializer
            .write_manifest(block)
            .int_err()?;
        let insert_block_data_result = self.insert_block_data(&block_data, options).await;

        if let Ok(result) = &insert_block_data_result {
            // We've already cached in insert_block_data() call
            let mut cached_value = self.cache.get_mut(&result.hash).unwrap();

            cached_value.deserialized_block = Some(block.clone());
        }

        insert_block_data_result
    }

    async fn insert_block_data<'a>(
        &'a self,
        block_data: &'a [u8],
        options: InsertOpts<'a>,
    ) -> Result<InsertBlockResult, InsertBlockError> {
        let insert_result = self.wrapped.insert_block_data(block_data, options).await;

        if let Ok(result) = &insert_result {
            let cache_value = CachedValue {
                block_data: Bytes::copy_from_slice(block_data),
                deserialized_block: None,
            };

            self.cache.insert(result.hash.clone(), cache_value);
        }

        insert_result
    }
}
