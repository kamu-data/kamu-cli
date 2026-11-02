// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use dill::{component, interface};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{
    DatasetBlockQueryRepository,
    DatasetBlocksFilter,
    DatasetBlocksListing,
    DatasetBlocksQueryService,
    DatasetKeyBlockRepository,
    ResolvedDataset,
};
use odf::dataset::MetadataChainExt as _;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetBlocksQueryServiceImpl {
    key_block_repo: Arc<dyn DatasetKeyBlockRepository>,
    block_query_repo: Arc<dyn DatasetBlockQueryRepository>,
}

#[component(pub)]
#[interface(dyn DatasetBlocksQueryService)]
impl DatasetBlocksQueryServiceImpl {
    pub fn new(
        key_block_repo: Arc<dyn DatasetKeyBlockRepository>,
        block_query_repo: Arc<dyn DatasetBlockQueryRepository>,
    ) -> Self {
        Self {
            key_block_repo,
            block_query_repo,
        }
    }

    async fn list_indexed_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        filter: &DatasetBlocksFilter,
        pagination: PaginationOpts,
    ) -> Result<DatasetBlocksListing, InternalError> {
        let total_count = self
            .block_query_repo
            .count_blocks(dataset_id, &odf::BlockRef::Head, filter)
            .await?;

        let blocks = self
            .block_query_repo
            .list_blocks(dataset_id, &odf::BlockRef::Head, filter, pagination)
            .await?
            .into_iter()
            .map(|block| {
                let decoded_block = odf::storage::deserialize_metadata_block(
                    &block.block_hash,
                    &block.block_payload,
                )
                .int_err()?;
                Ok((block.block_hash, decoded_block))
            })
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(DatasetBlocksListing {
            blocks,
            total_count,
        })
    }

    /// Walks the whole chain, for datasets whose blocks are not indexed yet
    async fn list_blocks_by_chain_walk(
        dataset: &ResolvedDataset,
        filter: &DatasetBlocksFilter,
        pagination: PaginationOpts,
    ) -> Result<DatasetBlocksListing, InternalError> {
        // Type hints are answered from the index, which this dataset lacks
        let decision = odf::dataset::MetadataVisitorDecision::Next;

        let mut visitor = odf::dataset::GenericCallbackVisitor::new(
            DatasetBlocksListing {
                blocks: Vec::new(),
                total_count: 0,
            },
            decision,
            |listing, hash, block| {
                let event_type = odf::MetadataEventType::from_metadata_event(&block.event);
                if filter.matches(hash, event_type) {
                    if listing.total_count >= pagination.offset
                        && listing.blocks.len() < pagination.limit
                    {
                        listing.blocks.push((hash.clone(), block.clone()));
                    }
                    listing.total_count += 1;
                }
                decision
            },
        );

        dataset
            .as_metadata_chain()
            .accept(&mut [&mut visitor])
            .await
            .int_err()?;

        Ok(visitor.into_state())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetBlocksQueryService for DatasetBlocksQueryServiceImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(dataset_id = %dataset.get_id(), ?filter))]
    async fn list_blocks(
        &self,
        dataset: &ResolvedDataset,
        filter: &DatasetBlocksFilter,
        pagination: PaginationOpts,
    ) -> Result<DatasetBlocksListing, InternalError> {
        let is_indexed = self
            .key_block_repo
            .has_key_blocks_for_ref(dataset.get_id(), &odf::BlockRef::Head)
            .await?;

        if is_indexed {
            self.list_indexed_blocks(dataset.get_id(), filter, pagination)
                .await
        } else {
            tracing::warn!(
                dataset_id = %dataset.get_id(),
                "Blocks of the dataset are not indexed, walking the metadata chain"
            );
            Self::list_blocks_by_chain_walk(dataset, filter, pagination).await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
