// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use odf::{BlockRef, DatasetID};
use thiserror::Error;

use crate::{DatasetKeyBlock, MetadataEventType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetKeyBlockRepository: Send + Sync {
    async fn has_blocks(
        &self,
        dataset_id: &DatasetID,
        block_ref: &BlockRef,
    ) -> Result<bool, InternalError>;

    // TODO: looks nice, but not used yet
    async fn find_latest_block_of_kind(
        &self,
        dataset_id: &DatasetID,
        block_ref: &BlockRef,
        kind: MetadataEventType,
    ) -> Result<Option<DatasetKeyBlock>, DatasetKeyBlockQueryError>;

    async fn find_blocks_of_kinds_in_range(
        &self,
        dataset_id: &DatasetID,
        block_ref: &BlockRef,
        kinds: &[MetadataEventType],
        min_sequence: Option<u64>,
        max_sequence: u64,
    ) -> Result<Vec<DatasetKeyBlock>, DatasetKeyBlockQueryError>;

    // TODO: looks nice, but not used yet
    async fn find_max_sequence_number(
        &self,
        dataset_id: &DatasetID,
        block_ref: &BlockRef,
    ) -> Result<Option<u64>, DatasetKeyBlockQueryError>;

    // TODO: consider removal, batch version solves it
    async fn save_block(
        &self,
        dataset_id: &DatasetID,
        block_ref: &BlockRef,
        block: &DatasetKeyBlock,
    ) -> Result<(), DatasetKeyBlockSaveError>;

    async fn save_blocks_batch(
        &self,
        dataset_id: &DatasetID,
        block_ref: &BlockRef,
        blocks: &[DatasetKeyBlock],
    ) -> Result<(), DatasetKeyBlockSaveError>;

    // TODO: looks nice, but not used yet
    async fn delete_blocks_after(
        &self,
        dataset_id: &DatasetID,
        block_ref: &BlockRef,
        sequence_number: u64,
    ) -> Result<(), InternalError>;

    async fn delete_all_for_ref(
        &self,
        dataset_id: &DatasetID,
        block_ref: &BlockRef,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DatasetKeyBlockQueryError {
    #[error(transparent)]
    UnmatchedDatasetEntry(DatasetUnmatchedEntryError),

    #[error("Internal error: {0}")]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DatasetKeyBlockSaveError {
    #[error("A block already exists at sequence number {0}")]
    DuplicateSequenceNumber(u64),

    #[error(transparent)]
    UnmatchedDatasetEntry(DatasetUnmatchedEntryError),

    #[error("Internal error: {0}")]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Invalid dataset ID '{dataset_id}' (no matching dataset entry available)")]
pub struct DatasetUnmatchedEntryError {
    pub dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
