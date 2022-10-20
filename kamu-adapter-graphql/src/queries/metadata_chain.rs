// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::queries::Account;
use crate::scalars::*;
use crate::utils::*;

use async_graphql::*;
use futures::TryStreamExt;
use kamu::domain;
use kamu::domain::MetadataChainExt;
use opendatafabric as odf;

////////////////////////////////////////////////////////////////////////////////////////
// MetadataRef
////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct BlockRef {
    name: String,
    block_hash: Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////
// MetadataChain
////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChain {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl MetadataChain {
    const DEFAULT_BLOCKS_PER_PAGE: usize = 20;

    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    #[graphql(skip)]
    async fn get_dataset(&self, ctx: &Context<'_>) -> Result<std::sync::Arc<dyn domain::Dataset>> {
        let local_repo = from_catalog::<dyn domain::LocalDatasetRepository>(ctx).unwrap();
        let dataset = local_repo
            .get_dataset(&self.dataset_handle.as_local_ref())
            .await?;
        Ok(dataset)
    }

    /// Returns all named metadata block references
    async fn refs(&self, ctx: &Context<'_>) -> Result<Vec<BlockRef>> {
        let dataset = self.get_dataset(ctx).await?;
        Ok(vec![BlockRef {
            name: "head".to_owned(),
            block_hash: dataset
                .as_metadata_chain()
                .get_ref(&domain::BlockRef::Head)
                .await?
                .into(),
        }])
    }

    /// Returns a metadata block corresponding to the specified hash
    async fn block_by_hash(
        &self,
        ctx: &Context<'_>,
        hash: Multihash,
    ) -> Result<Option<MetadataBlockExtended>> {
        let dataset = self.get_dataset(ctx).await?;
        let block = dataset.as_metadata_chain().try_get_block(&hash).await?;
        Ok(block.map(|b| MetadataBlockExtended::new(hash, b, Account::mock())))
    }

    // TODO: Add ref parameter (defaulting to "head")
    // TODO: Support before/after style iteration
    // TODO: PERF: Avoid traversing entire chain
    /// Iterates all metadata blocks in the reverse chronological order
    async fn blocks(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MetadataBlockConnection> {
        let dataset = self.get_dataset(ctx).await?;

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_BLOCKS_PER_PAGE);

        let blocks: Vec<_> = dataset
            .as_metadata_chain()
            .iter_blocks()
            .try_collect()
            .await?;

        let total_count = blocks.len();

        let nodes: Vec<_> = blocks
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
            .map(|(hash, block)| MetadataBlockExtended::new(hash, block, Account::mock()))
            .collect();

        Ok(MetadataBlockConnection::new(
            nodes,
            page,
            per_page,
            total_count,
        ))
    }
}

page_based_connection!(
    MetadataBlockExtended,
    MetadataBlockConnection,
    MetadataBlockEdge
);
