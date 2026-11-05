// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use futures::{StreamExt, TryStreamExt};
use kamu_datasets::{DatasetBlocksFilter, DatasetBlocksQueryService, ResolvedDataset};
use odf::dataset::MetadataChainExt as _;

use crate::prelude::*;
use crate::queries::{Account, DatasetRequestState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataRef
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Named reference to a metadata block
#[derive(SimpleObject)]
pub struct BlockRef {
    /// Name of the reference, e.g. `head`
    name: String,
    /// Hash of the block the reference points to
    block_hash: Multihash<'static>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataChain
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChain<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

/// Chain of metadata blocks of a dataset
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
        let account = Account::from_dataset_alias(ctx, self.dataset_request_state.dataset_alias())
            .await?
            .expect("Account must exist");
        Ok(block_maybe.map(|block| MetadataBlockExtended::new(hash, block, account)))
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
                    let ser = odf::metadata::serde::yaml::YamlMetadataBlockSerializer;
                    let buffer = ser.write_manifest(&block).int_err()?;
                    let content = std::str::from_utf8(&buffer).int_err()?;
                    Ok(Some(content.to_string()))
                }
            },
        }
    }

    // TODO: Add ref parameter (defaulting to "head")
    // TODO: Support before/after style iteration
    /// Iterates metadata blocks, optionally filtered, in the reverse
    /// chronological order
    #[tracing::instrument(level = "info", name = MetadataChain_blocks, skip_all, fields(?page, ?per_page, ?filter))]
    async fn blocks(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        #[graphql(desc = "Counts and pages only the matching blocks")] filter: Option<
            MetadataBlocksFilter,
        >,
    ) -> Result<MetadataBlockConnection> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_BLOCKS_PER_PAGE);

        if let Some(filter) = filter
            .map(MetadataBlocksFilter::into_domain)
            .transpose()?
            .flatten()
        {
            return self
                .filtered_blocks(ctx, resolved_dataset, &filter, page, per_page)
                .await;
        }

        let chain = resolved_dataset.as_metadata_chain();

        let head = chain.resolve_ref(&odf::BlockRef::Head).await.int_err()?;
        let head_block = chain.get_block(&head).await.int_err()?;
        let total_count = usize::try_from(head_block.sequence_number).unwrap() + 1;

        let account = Account::from_dataset_alias(ctx, self.dataset_request_state.dataset_alias())
            .await?
            .expect("Account must exist");

        // Special case: head block request
        if page == 0 && per_page == 1 {
            let block = MetadataBlockExtended::new(head, head_block, account);
            Ok(MetadataBlockConnection::new(
                vec![block],
                page,
                per_page,
                total_count,
            ))
        } else {
            // General case: stream blocks
            let mut block_stream = chain
                .iter_blocks_interval((&head).into(), None, false)
                .skip(page * per_page)
                .take(per_page);

            let mut nodes = Vec::new();
            while let Some((hash, block)) = block_stream.try_next().await.int_err()? {
                let block = MetadataBlockExtended::new(hash, block, account.clone());
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

    #[graphql(skip)]
    async fn filtered_blocks(
        &self,
        ctx: &Context<'_>,
        resolved_dataset: &ResolvedDataset,
        filter: &DatasetBlocksFilter,
        page: usize,
        per_page: usize,
    ) -> Result<MetadataBlockConnection> {
        let blocks_query_service = from_catalog_n!(ctx, dyn DatasetBlocksQueryService);

        let listing = blocks_query_service
            .list_blocks(
                resolved_dataset,
                filter,
                PaginationOpts::from_page(page, per_page),
            )
            .await?;

        let account = Account::from_dataset_alias(ctx, self.dataset_request_state.dataset_alias())
            .await?
            .expect("Account must exist");

        let mut nodes = Vec::with_capacity(listing.blocks.len());
        for (hash, block) in listing.blocks {
            nodes.push(MetadataBlockExtended::new(hash, block, account.clone()));
        }

        Ok(MetadataBlockConnection::new(
            nodes,
            page,
            per_page,
            listing.total_count,
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
