// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::{StreamExt, TryStreamExt};
use odf::dataset::MetadataChainExt as _;

use crate::prelude::*;
use crate::queries::{Account, DatasetRequestState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataRef
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct BlockRef {
    name: String,
    block_hash: Multihash<'static>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataChain
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChain<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> MetadataChain<'a> {
    const DEFAULT_BLOCKS_PER_PAGE: usize = 20;

    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    /// Returns all named metadata block references
    #[tracing::instrument(level = "info", name = MetadataChain_refs, skip_all)]
    async fn refs(&self, ctx: &Context<'_>) -> Result<Vec<BlockRef>> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;
        Ok(vec![BlockRef {
            name: "head".to_owned(),
            block_hash: resolved_dataset
                .as_metadata_chain()
                .resolve_ref(&odf::BlockRef::Head)
                .await
                .int_err()?
                .into(),
        }])
    }

    /// Returns a metadata block corresponding to the specified hash
    #[tracing::instrument(level = "info", name = MetadataChain_block_by_hash, skip_all)]
    async fn block_by_hash(
        &self,
        ctx: &Context<'_>,
        hash: Multihash<'static>,
    ) -> Result<Option<MetadataBlockExtended>> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;
        let block_maybe = resolved_dataset
            .as_metadata_chain()
            .try_get_block(&hash)
            .await?;
        let account =
            Account::from_dataset_alias(ctx, &self.dataset_request_state.dataset_handle().alias)
                .await?
                .expect("Account must exist");
        Ok(if let Some(block) = block_maybe {
            Some(MetadataBlockExtended::new(ctx, hash, block, account).await?)
        } else {
            None
        })
    }

    /// Returns a metadata block corresponding to the specified hash and encoded
    /// in desired format
    #[tracing::instrument(level = "info", name = MetadataChain_block_by_hash_encoded, skip_all)]
    async fn block_by_hash_encoded(
        &self,
        ctx: &Context<'_>,
        hash: Multihash<'_>,
        format: MetadataManifestFormat,
    ) -> Result<Option<String>> {
        use odf::metadata::serde::MetadataBlockSerializer;

        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;
        match resolved_dataset
            .as_metadata_chain()
            .try_get_block(&hash)
            .await?
        {
            None => Ok(None),
            Some(block) => match format {
                MetadataManifestFormat::Yaml => {
                    let block_content = MetadataBlock::with_extended_aliases(ctx, block).await?;
                    let ser = odf::metadata::serde::yaml::YamlMetadataBlockSerializer;
                    let buffer = ser.write_manifest(&block_content).int_err()?;
                    let content = std::str::from_utf8(&buffer).int_err()?;
                    Ok(Some(content.to_string()))
                }
            },
        }
    }

    // TODO: Add ref parameter (defaulting to "head")
    // TODO: Support before/after style iteration
    /// Iterates all metadata blocks in the reverse chronological order
    #[tracing::instrument(level = "info", name = MetadataChain_blocks, skip_all, fields(?page, ?per_page))]
    async fn blocks(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MetadataBlockConnection> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_BLOCKS_PER_PAGE);

        let chain = resolved_dataset.as_metadata_chain();

        let head = chain.resolve_ref(&odf::BlockRef::Head).await.int_err()?;
        let total_count =
            usize::try_from(chain.get_block(&head).await.int_err()?.sequence_number).unwrap() + 1;

        let mut block_stream = chain
            .iter_blocks_interval(&head, None, false)
            .skip(page * per_page)
            .take(per_page);

        let mut nodes = Vec::new();
        while let Some((hash, block)) = block_stream.try_next().await.int_err()? {
            let account = Account::from_dataset_alias(
                ctx,
                &self.dataset_request_state.dataset_handle().alias,
            )
            .await?
            .expect("Account must exist");
            let block = MetadataBlockExtended::new(ctx, hash, block, account).await?;
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    MetadataBlockExtended,
    MetadataBlockConnection,
    MetadataBlockEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
