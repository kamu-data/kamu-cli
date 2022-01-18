// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::queries::*;
use crate::scalars::*;
use crate::utils::*;

use async_graphql::*;
use kamu::domain;
use opendatafabric as odf;

////////////////////////////////////////////////////////////////////////////////////////
// MetadataRef
////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub(crate) struct BlockRef {
    name: String,
    block_hash: Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////
// MetadataChain
////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct MetadataChain {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl MetadataChain {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    #[graphql(skip)]
    fn get_chain(&self, ctx: &Context<'_>) -> Result<Box<dyn domain::MetadataChain>> {
        let dataset_reg = from_catalog::<dyn domain::DatasetRegistry>(ctx).unwrap();
        Ok(dataset_reg.get_metadata_chain(&self.dataset_handle.as_local_ref())?)
    }

    /// Returns all named metadata block references
    async fn refs(&self, ctx: &Context<'_>) -> Result<Vec<BlockRef>> {
        let chain = self.get_chain(ctx)?;
        Ok(vec![BlockRef {
            name: "head".to_owned(),
            block_hash: chain.read_ref(&domain::BlockRef::Head).unwrap().into(),
        }])
    }

    /// Returns a metadata block corresponding to the specified hash
    async fn block_by_hash(
        &self,
        ctx: &Context<'_>,
        hash: Multihash,
    ) -> Result<Option<MetadataBlock>> {
        let chain = self.get_chain(ctx)?;
        Ok(chain
            .get_block(&hash)
            .map(|b| MetadataBlock::new(hash.into(), b)))
    }

    // TODO: Add ref parameter (defaulting to "head")
    // TODO: Support before/after style iteration
    /// Iterates all metadata blocks in the reverse chronological order
    async fn blocks(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        #[graphql(default = 20)] per_page: usize,
    ) -> Result<MetadataBlockConnection> {
        let chain = self.get_chain(ctx)?;

        let page = page.unwrap_or(0);

        let nodes: Vec<_> = chain
            .iter_blocks()
            .skip(page * per_page)
            .take(per_page)
            .map(|(hash, block)| MetadataBlock::new(hash, block))
            .collect();

        // TODO: Slow but temporary
        let total_count = chain.iter_blocks().count();

        Ok(MetadataBlockConnection::new(
            nodes,
            page,
            per_page,
            Some(total_count),
        ))
    }
}

page_based_connection!(MetadataBlock, MetadataBlockConnection, MetadataBlockEdge);
