// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::{DatasetBlock, DatasetUnmatchedEntryError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetDataBlockRepository: Send + Sync {
    async fn has_data_blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<bool, InternalError>;

    async fn contains_data_block(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
    ) -> Result<bool, InternalError>;

    async fn get_data_block(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
    ) -> Result<Option<DatasetBlock>, InternalError>;

    async fn get_data_block_size(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
    ) -> Result<Option<usize>, InternalError>;

    async fn get_page_of_data_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        page_size: usize,
        upper_sequence_number_inclusive: u64,
    ) -> Result<Vec<DatasetBlock>, DatasetDataBlockQueryError>;

    async fn get_all_data_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<Vec<DatasetBlock>, DatasetDataBlockQueryError>;

    async fn save_data_blocks_batch(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        blocks: &[DatasetBlock],
    ) -> Result<(), DatasetDataBlockSaveError>;

    async fn delete_all_data_blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DatasetDataBlockQueryError {
    #[error(transparent)]
    UnmatchedDatasetEntry(DatasetUnmatchedEntryError),

    #[error("Internal error: {0}")]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DatasetDataBlockSaveError {
    #[error("A block already exists at one of the sequence numbers {0:?}")]
    DuplicateSequenceNumber(Vec<u64>),

    #[error(transparent)]
    UnmatchedDatasetEntry(DatasetUnmatchedEntryError),

    #[error("Internal error: {0}")]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
