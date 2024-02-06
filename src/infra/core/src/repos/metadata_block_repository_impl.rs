// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use internal_error::ResultIntoInternal;
use kamu_core::{
    deserialize_metadata_block,
    BlockNotFoundError,
    ContainsError,
    GetBlockError,
    GetError,
    InsertError,
    InsertOpts,
    InsertResult,
    MetadataBlockRepository,
    ObjectRepository,
};
use opendatafabric::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use opendatafabric::serde::MetadataBlockSerializer;
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
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<ObjRepo> MetadataBlockRepository for MetadataBlockRepositoryImpl<ObjRepo>
where
    ObjRepo: ObjectRepository + Sync + Send,
{
    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError> {
        self.obj_repo.contains(hash).await
    }

    async fn get(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError> {
        let data = match self.obj_repo.get_bytes(hash).await {
            Ok(data) => Ok(data),
            Err(GetError::NotFound(e)) => {
                Err(GetBlockError::NotFound(BlockNotFoundError { hash: e.hash }))
            }
            Err(GetError::Access(e)) => Err(GetBlockError::Access(e)),
            Err(GetError::Internal(e)) => Err(GetBlockError::Internal(e)),
        }?;

        deserialize_metadata_block(hash, &data)
    }

    async fn insert<'a>(
        &'a self,
        block: &MetadataBlock,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let data = FlatbuffersMetadataBlockSerializer
            .write_manifest(block)
            .int_err()?;

        self.obj_repo.insert_bytes(&data, options).await
    }

    fn as_object_repo(&self) -> &dyn ObjectRepository {
        &self.obj_repo
    }
}
