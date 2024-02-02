// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use kamu_core::GetBlockError;
use opendatafabric::{MetadataBlock, Multihash};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait GetMetadataBlockStrategy: Send + Sync {
    async fn get(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError>;
}
