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
use opendatafabric::{MetadataBlock, Multihash};

use crate::{
    ContainsError,
    GetBlockError,
    GetError,
    InsertError,
    InsertOpts,
    InsertResult,
    ObjectRepository,
};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait MetadataBlockRepository: Send + Sync {
    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError>;

    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError>;

    async fn get_block_data(&self, hash: &Multihash) -> Result<Bytes, GetError>;

    async fn get_size(&self, hash: &Multihash) -> Result<u64, GetError>;

    async fn insert_block_data<'a>(
        &'a self,
        block_data: &'a [u8],
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError>;

    fn as_object_repo(&self) -> &dyn ObjectRepository;
}
