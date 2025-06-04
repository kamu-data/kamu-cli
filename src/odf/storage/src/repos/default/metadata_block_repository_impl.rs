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
use internal_error::ResultIntoInternal;
use odf_metadata::serde::MetadataBlockSerializer;
use odf_metadata::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use odf_metadata::*;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataBlockRepositoryImpl<ObjRepo> {
    obj_repo: ObjRepo,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<ObjRepo> MetadataBlockRepositoryImpl<ObjRepo>
where
    ObjRepo: ObjectRepository + Sync + Send,
{
    pub fn new(obj_repo: ObjRepo) -> Self {
        Self { obj_repo }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<ObjRepo> MetadataBlockRepository for MetadataBlockRepositoryImpl<ObjRepo>
where
    ObjRepo: ObjectRepository + Sync + Send,
{
    async fn contains_block(&self, hash: &Multihash) -> Result<bool, ContainsBlockError> {
        self.obj_repo.contains(hash).await.map_err(Into::into)
    }

    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError> {
        let block_data = self.get_block_bytes(hash).await?;

        deserialize_metadata_block(hash, &block_data)
    }

    async fn get_block_bytes(&self, hash: &Multihash) -> Result<Bytes, GetBlockDataError> {
        self.obj_repo.get_bytes(hash).await.map_err(Into::into)
    }

    async fn get_block_size(&self, hash: &Multihash) -> Result<u64, GetBlockDataError> {
        self.obj_repo.get_size(hash).await.map_err(Into::into)
    }

    async fn insert_block<'a>(
        &'a self,
        block: &MetadataBlock,
        options: InsertOpts<'a>,
    ) -> Result<InsertBlockResult, InsertBlockError> {
        let block_data = FlatbuffersMetadataBlockSerializer
            .write_manifest(block)
            .int_err()?;

        self.insert_block_data(&block_data, options).await
    }

    async fn insert_block_data<'a>(
        &'a self,
        block_data: &'a [u8],
        options: InsertOpts<'a>,
    ) -> Result<InsertBlockResult, InsertBlockError> {
        self.obj_repo
            .insert_bytes(block_data, options)
            .await
            .map(Into::into)
            .map_err(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
