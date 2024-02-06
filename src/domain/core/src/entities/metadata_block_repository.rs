// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use opendatafabric::{MetadataBlock, Multihash};

use crate::{
    ContainsError,
    GetBlockError,
    InsertError,
    InsertOpts,
    InsertResult,
    ObjectRepository,
};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait MetadataBlockRepository: Send + Sync {
    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError>;

    async fn get(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError>;

    async fn insert<'a>(
        &'a self,
        block: &MetadataBlock,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError>;

    fn as_object_repo(&self) -> &dyn ObjectRepository;
}
