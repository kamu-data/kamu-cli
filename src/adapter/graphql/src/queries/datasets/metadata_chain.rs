// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::{StreamExt, TryStreamExt};
use kamu_core::{self as domain, MetadataChainExt};
use opendatafabric as odf;

use crate::prelude::*;
use crate::queries::Account;

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
        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let dataset = dataset_repo
            .get_dataset(&self.dataset_handle.as_local_ref())
            .await
            .int_err()?;
        Ok(dataset)
    }

    /// Returns all named metadata block references
    #[tracing::instrument(level = "info", skip_all)]
    async fn refs(&self, ctx: &Context<'_>) -> Result<Vec<BlockRef>> {
        let dataset = self.get_dataset(ctx).await?;
        Ok(vec![BlockRef {
            name: "head".to_owned(),
            block_hash: dataset
                .as_metadata_chain()
                .resolve_ref(&domain::BlockRef::Head)
                .await
                .int_err()?
                .into(),
        }])
    }

    /// Returns a metadata block corresponding to the specified hash
    #[tracing::instrument(level = "info", skip_all)]
    async fn block_by_hash(
        &self,
        ctx: &Context<'_>,
        hash: Multihash,
    ) -> Result<Option<MetadataBlockExtended>> {
        let dataset = self.get_dataset(ctx).await?;
        let block = dataset.as_metadata_chain().try_get_block(&hash).await?;
        let account = Account::from_dataset_alias(ctx, &self.dataset_handle.alias)
            .await?
            .expect("Account must exist");
        Ok(block.map(|b| MetadataBlockExtended::new(hash, b, account)))
    }

    /// Returns a metadata block corresponding to the specified hash and encoded
    /// in desired format
    #[tracing::instrument(level = "info", skip_all)]
    async fn block_by_hash_encoded(
        &self,
        ctx: &Context<'_>,
        hash: Multihash,
        format: MetadataManifestFormat,
    ) -> Result<Option<String>> {
        use odf::serde::MetadataBlockSerializer;

        let dataset = self.get_dataset(ctx).await?;
        match dataset.as_metadata_chain().try_get_block(&hash).await? {
            None => Ok(None),
            Some(block) => match format {
                MetadataManifestFormat::Yaml => {
                    let ser = odf::serde::yaml::YamlMetadataBlockSerializer;
                    let buffer = ser.write_manifest(&block).int_err()?;
                    let content = std::str::from_utf8(&buffer).int_err()?;
                    Ok(Some(content.to_string()))
                }
            },
        }
    }

    // TODO: Add ref parameter (defaulting to "head")
    // TODO: Support before/after style iteration
    /// Iterates all metadata blocks in the reverse chronological order
    #[tracing::instrument(level = "info", skip_all)]
    async fn blocks(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MetadataBlockConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_BLOCKS_PER_PAGE);

        let dataset = self.get_dataset(ctx).await?;
        let chain = dataset.as_metadata_chain();

        let head = chain.resolve_ref(&domain::BlockRef::Head).await.int_err()?;
        let total_count =
            usize::try_from(chain.get_block(&head).await.int_err()?.sequence_number).unwrap() + 1;

        let mut block_stream = chain
            .iter_blocks_interval(&head, None, false)
            .skip(page * per_page)
            .take(per_page);

        let mut nodes = Vec::new();
        while let Some((hash, block)) = block_stream.try_next().await.int_err()? {
            let account = Account::from_dataset_alias(ctx, &self.dataset_handle.alias)
                .await?
                .expect("Account must exist");
            let block = MetadataBlockExtended::new(hash, block, account);
            nodes.push(block);
        }

        Ok(MetadataBlockConnection::new(
            nodes,
            page,
            per_page,
            total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    MetadataBlockExtended,
    MetadataBlockConnection,
    MetadataBlockEdge
);
