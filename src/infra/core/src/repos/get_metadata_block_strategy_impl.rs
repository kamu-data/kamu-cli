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
use kamu_core::{
    deserialize_metadata_block,
    BlockNotFoundError,
    GetBlockError,
    GetError,
    ObjectRepository,
};
use opendatafabric::{MetadataBlock, Multihash};

use crate::GetMetadataBlockStrategy;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct GetMetadataBlockStrategyImpl<ObjRepo> {
    obj_repo: Arc<ObjRepo>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<ObjRepo> GetMetadataBlockStrategyImpl<ObjRepo>
where
    ObjRepo: ObjectRepository + Sync + Send,
{
    pub fn new(obj_repo: Arc<ObjRepo>) -> Self {
        Self { obj_repo }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<ObjRepo> GetMetadataBlockStrategy for GetMetadataBlockStrategyImpl<ObjRepo>
where
    ObjRepo: ObjectRepository + Sync + Send,
{
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
}
