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
use kamu_core::{
    AppendError,
    AppendOpts,
    BlockRef,
    DynMetadataStream,
    GetBlockError,
    GetRefError,
    MetadataChain,
    ObjectRepository,
    ReferenceRepository,
    SetRefError,
    SetRefOpts,
};
use opendatafabric::{MetadataBlock, Multihash};

use crate::HashMetadataBlockCacheMap;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainCacheFacadeInMemory<WrappedChain> {
    chain: WrappedChain,
    cache: Arc<HashMetadataBlockCacheMap>,
    ref_cache: DashMap<BlockRef, Multihash>,
}

impl<WrappedChain> MetadataChainCacheFacadeInMemory<WrappedChain> {
    pub fn new(chain: WrappedChain, cache: Arc<HashMetadataBlockCacheMap>) -> Self {
        Self {
            chain,
            cache,
            ref_cache: DashMap::new(),
        }
    }
}
#[async_trait]
impl<WrappedChain> MetadataChain for MetadataChainCacheFacadeInMemory<WrappedChain>
where
    WrappedChain: MetadataChain,
{
    async fn get_ref(&self, block_ref: &BlockRef) -> Result<Multihash, GetRefError> {
        if let Some(cached_result) = self.ref_cache.get(block_ref) {
            return Ok(cached_result.clone());
        };

        let get_ref_result = self.chain.get_ref(block_ref).await;

        if let Ok(hash) = &get_ref_result {
            self.ref_cache.insert(block_ref.clone(), hash.clone());
        }

        get_ref_result
    }

    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError> {
        match self.cache.get(hash) {
            Some(cached_result) => Ok(cached_result.clone()),
            None => self.chain.get_block(hash).await,
        }
    }

    fn iter_blocks_interval<'a>(
        &'a self,
        head: &'a Multihash,
        tail: Option<&'a Multihash>,
        ignore_missing_tail: bool,
    ) -> DynMetadataStream<'a> {
        self.chain
            .iter_blocks_interval(head, tail, ignore_missing_tail)
    }

    fn iter_blocks_interval_inclusive<'a>(
        &'a self,
        head: &'a Multihash,
        tail: &'a Multihash,
        ignore_missing_tail: bool,
    ) -> DynMetadataStream<'a> {
        self.chain
            .iter_blocks_interval_inclusive(head, tail, ignore_missing_tail)
    }

    fn iter_blocks_interval_ref<'a>(
        &'a self,
        head: &'a BlockRef,
        tail: Option<&'a BlockRef>,
    ) -> DynMetadataStream<'a> {
        self.chain.iter_blocks_interval_ref(head, tail)
    }

    async fn set_ref<'a>(
        &'a self,
        r: &BlockRef,
        hash: &Multihash,
        opts: SetRefOpts<'a>,
    ) -> Result<(), SetRefError> {
        let set_ref_result = self.chain.set_ref(r, hash, opts).await;

        if set_ref_result.is_ok() {
            self.ref_cache.insert(r.clone(), hash.clone());
        }

        set_ref_result
    }

    async fn append<'a>(
        &'a self,
        block: MetadataBlock,
        opts: AppendOpts<'a>,
    ) -> Result<Multihash, AppendError> {
        let maybe_new_ref = opts.update_ref.map(Clone::clone);
        let append_result = self.chain.append(block.clone(), opts).await;

        if let Ok(hash) = &append_result {
            self.cache.insert(hash.clone(), block);

            if let Some(new_ref) = maybe_new_ref {
                self.ref_cache.insert(new_ref, hash.clone());
            }
        }

        append_result
    }

    fn as_object_repo(&self) -> &dyn ObjectRepository {
        self.chain.as_object_repo()
    }

    fn as_reference_repo(&self) -> &dyn ReferenceRepository {
        self.chain.as_reference_repo()
    }
}
