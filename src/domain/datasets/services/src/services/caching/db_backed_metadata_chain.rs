// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatabaseBackedMetadataChain {
    storage_chain: Arc<dyn odf::MetadataChain>,
}

impl DatabaseBackedMetadataChain {
    pub fn new(storage_chain: Arc<dyn odf::MetadataChain>) -> Self {
        Self { storage_chain }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl odf::MetadataChain for DatabaseBackedMetadataChain {
    /// Resolves reference to the block hash it's pointing to
    async fn resolve_ref(
        &self,
        r: &odf::BlockRef,
    ) -> Result<odf::Multihash, odf::storage::GetRefError> {
        self.storage_chain.resolve_ref(r).await
    }

    /// Returns true if chain contains block
    async fn contains_block(
        &self,
        hash: &odf::Multihash,
    ) -> Result<bool, odf::storage::ContainsBlockError> {
        self.storage_chain.contains_block(hash).await
    }

    /// Returns size of the specified block in bytes
    async fn get_block_size(
        &self,
        hash: &odf::Multihash,
    ) -> Result<u64, odf::storage::GetBlockDataError> {
        self.storage_chain.get_block_size(hash).await
    }

    /// Returns the specified block as raw bytes
    async fn get_block_bytes(
        &self,
        hash: &odf::Multihash,
    ) -> Result<bytes::Bytes, odf::storage::GetBlockDataError> {
        self.storage_chain.get_block_bytes(hash).await
    }

    /// Returns the specified block
    async fn get_block(
        &self,
        hash: &odf::Multihash,
    ) -> Result<odf::MetadataBlock, odf::storage::GetBlockError> {
        self.storage_chain.get_block(hash).await
    }

    /// Update reference to point at the specified block
    async fn set_ref<'a>(
        &'a self,
        r: &odf::BlockRef,
        hash: &odf::Multihash,
        opts: odf::dataset::SetRefOpts<'a>,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        self.storage_chain.set_ref(r, hash, opts).await
    }

    /// Appends the block to the chain
    async fn append<'a>(
        &'a self,
        block: odf::MetadataBlock,
        opts: odf::dataset::AppendOpts<'a>,
    ) -> Result<odf::Multihash, odf::dataset::AppendError> {
        self.storage_chain.append(block, opts).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
