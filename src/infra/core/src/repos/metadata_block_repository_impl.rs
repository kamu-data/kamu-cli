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
use kamu_core::{
    BlockMalformedError,
    BlockVersionError,
    ContainsBlockError,
    ExternalTransferOpts,
    GetBlockDataError,
    GetBlockError,
    GetBlockExternalUrlError,
    GetBlockExternalUrlResult,
    InsertBlockError,
    InsertBlockResult,
    InsertOpts,
    MetadataBlockRepository,
    ObjectRepository,
};
use opendatafabric::serde::flatbuffers::FlatbuffersMetadataBlockDeserializer;
use opendatafabric::serde::{Error, MetadataBlockDeserializer};
use opendatafabric::{MetadataBlock, Multihash};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataBlockRepositoryImpl<ObjRepo> {
    obj_repo: ObjRepo,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<ObjRepo> MetadataBlockRepositoryImpl<ObjRepo>
where
    ObjRepo: ObjectRepository + Sync + Send,
{
    pub fn new(obj_repo: ObjRepo) -> Self {
        Self { obj_repo }
    }

    pub fn deserialize_metadata_block(
        hash: &Multihash,
        block_bytes: &[u8],
    ) -> Result<MetadataBlock, GetBlockError> {
        FlatbuffersMetadataBlockDeserializer
            .read_manifest(block_bytes)
            .map_err(|e| match e {
                Error::UnsupportedVersion { .. } => {
                    GetBlockError::BlockVersion(BlockVersionError {
                        hash: hash.clone(),
                        source: e.into(),
                    })
                }
                _ => GetBlockError::BlockMalformed(BlockMalformedError {
                    hash: hash.clone(),
                    source: e.into(),
                }),
            })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<ObjRepo> MetadataBlockRepository for MetadataBlockRepositoryImpl<ObjRepo>
where
    ObjRepo: ObjectRepository + Sync + Send,
{
    async fn contains_block(&self, hash: &Multihash) -> Result<bool, ContainsBlockError> {
        self.obj_repo.contains(hash).await.map_err(Into::into)
    }

    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError> {
        let block_data = self.get_block_data(hash).await?;

        Self::deserialize_metadata_block(hash, &block_data)
    }

    async fn get_block_data(&self, hash: &Multihash) -> Result<Bytes, GetBlockDataError> {
        self.obj_repo.get_bytes(hash).await.map_err(Into::into)
    }

    async fn get_block_size(&self, hash: &Multihash) -> Result<u64, GetBlockDataError> {
        self.obj_repo.get_size(hash).await.map_err(Into::into)
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

    async fn get_block_external_download_url(
        &self,
        hash: &Multihash,
        opts: ExternalTransferOpts,
    ) -> Result<GetBlockExternalUrlResult, GetBlockExternalUrlError> {
        self.obj_repo
            .get_external_download_url(hash, opts)
            .await
            .map(Into::into)
            .map_err(Into::into)
    }

    async fn get_block_external_upload_url(
        &self,
        hash: &Multihash,
        opts: ExternalTransferOpts,
    ) -> Result<GetBlockExternalUrlResult, GetBlockExternalUrlError> {
        self.obj_repo
            .get_external_upload_url(hash, opts)
            .await
            .map(Into::into)
            .map_err(Into::into)
    }
}
