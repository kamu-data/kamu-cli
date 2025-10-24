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

use crate::{DatasetBlock, MetadataEventType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetKeyBlockRepository: Send + Sync {
    async fn has_key_blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<bool, InternalError>;

    async fn get_all_key_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<Vec<DatasetBlock>, DatasetKeyBlockQueryError>;

    async fn match_datasets_having_key_blocks(
        &self,
        dataset_ids: &[odf::DatasetID],
        block_ref: &odf::BlockRef,
        event_type: MetadataEventType,
    ) -> Result<Vec<(odf::DatasetID, DatasetBlock)>, InternalError>;

    async fn save_key_blocks_batch(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        blocks: &[DatasetBlock],
    ) -> Result<(), DatasetKeyBlockSaveError>;

    async fn delete_all_key_blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
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
    #[error("A block already exists at one of the sequence numbers {0:?}")]
    DuplicateSequenceNumber(Vec<u64>),

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
