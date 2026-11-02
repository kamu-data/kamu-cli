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
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetBlockQueryRepository {
    key_block_repo: Arc<dyn DatasetKeyBlockRepository>,
    data_block_repo: Arc<dyn DatasetDataBlockRepository>,
}

#[component(pub)]
#[interface(dyn DatasetBlockQueryRepository)]
impl InMemoryDatasetBlockQueryRepository {
    pub fn new(
        key_block_repo: Arc<dyn DatasetKeyBlockRepository>,
        data_block_repo: Arc<dyn DatasetDataBlockRepository>,
    ) -> Self {
        Self {
            key_block_repo,
            data_block_repo,
        }
    }

    async fn matching_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        filter: &DatasetBlocksFilter,
    ) -> Result<Vec<DatasetBlock>, InternalError> {
        let key_blocks = self
            .key_block_repo
            .get_all_key_blocks(dataset_id, block_ref)
            .await
            .int_err()?;
        let data_blocks = self
            .data_block_repo
            .get_all_data_blocks(dataset_id, block_ref)
            .await
            .int_err()?;

        let mut blocks: Vec<_> = key_blocks
            .into_iter()
            .chain(data_blocks)
            .filter(|block| filter.matches(&block.block_hash, block.event_kind))
            .collect();
        blocks.sort_by_key(|block| std::cmp::Reverse(block.sequence_number));

        Ok(blocks)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetBlockQueryRepository for InMemoryDatasetBlockQueryRepository {
    async fn list_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        filter: &DatasetBlocksFilter,
        pagination: PaginationOpts,
    ) -> Result<Vec<DatasetBlock>, InternalError> {
        Ok(self
            .matching_blocks(dataset_id, block_ref, filter)
            .await?
            .into_iter()
            .skip(pagination.offset)
            .take(pagination.limit)
            .collect())
    }

    async fn count_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        filter: &DatasetBlocksFilter,
    ) -> Result<usize, InternalError> {
        Ok(self
            .matching_blocks(dataset_id, block_ref, filter)
            .await?
            .len())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
