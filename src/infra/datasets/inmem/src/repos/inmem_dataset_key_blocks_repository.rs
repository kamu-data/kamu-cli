// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use cheap_clone::CheapClone;
use database_common::{TransactionId, TransactionRef};
use dill::*;
use internal_error::InternalError;
use kamu_datasets::*;

use super::InMemoryDatasetKeyBlockState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetKeyBlockRepository {
    state: Arc<InMemoryDatasetKeyBlockState>,
    transaction_id: Option<TransactionId>,
}

#[component(pub)]
#[interface(dyn DatasetKeyBlockRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
impl InMemoryDatasetKeyBlockRepository {
    pub fn new(
        state: Arc<InMemoryDatasetKeyBlockState>,
        transaction_ref: Option<TransactionRef>,
    ) -> Self {
        Self {
            state,
            transaction_id: transaction_ref.map(|transaction_ref| transaction_ref.id()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetKeyBlockRepository for InMemoryDatasetKeyBlockRepository {
    async fn list_unindexed_dataset_branches(
        &self,
    ) -> Result<Vec<(odf::DatasetID, odf::BlockRef)>, InternalError> {
        // Not implemented, however, not really required for usage in tests
        Ok(vec![])
    }

    async fn has_key_blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<bool, InternalError> {
        Ok(self
            .state
            .key_blocks
            .read(self.transaction_id, |key_blocks| {
                key_blocks
                    .get(&(dataset_id.clone(), block_ref.cheap_clone()))
                    .is_some_and(|blocks| !blocks.is_empty())
            }))
    }

    async fn save_key_blocks_batch(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        blocks: &[DatasetBlock],
    ) -> Result<(), DatasetKeyBlockSaveError> {
        if blocks.is_empty() {
            return Ok(());
        }

        let key = (dataset_id.clone(), block_ref.cheap_clone());
        self.state
            .key_blocks
            .write(self.transaction_id, |key_blocks| {
                let mut saved = key_blocks.view().get(&key).cloned().unwrap_or_default();

                let existing: HashSet<_> = saved.iter().map(|b| b.sequence_number).collect();
                if let Some(dup) = blocks
                    .iter()
                    .find(|b| existing.contains(&b.sequence_number))
                {
                    return Err(DatasetKeyBlockSaveError::DuplicateSequenceNumber(vec![
                        dup.sequence_number,
                    ]));
                }

                saved.extend_from_slice(blocks);
                saved.sort_by_key(|b| b.sequence_number);
                key_blocks.put(key, saved);
                Ok(())
            })
    }

    async fn get_all_key_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<Vec<DatasetBlock>, DatasetKeyBlockQueryError> {
        Ok(self
            .state
            .key_blocks
            .read(self.transaction_id, |key_blocks| {
                key_blocks
                    .get(&(dataset_id.clone(), block_ref.cheap_clone()))
                    .cloned()
                    .unwrap_or_default()
            }))
    }

    async fn match_datasets_having_key_blocks(
        &self,
        dataset_ids: &[odf::DatasetID],
        block_ref: &odf::BlockRef,
        event_type: odf::MetadataEventType,
    ) -> Result<Vec<(odf::DatasetID, DatasetBlock)>, InternalError> {
        Ok(self
            .state
            .key_blocks
            .read(self.transaction_id, |key_blocks| {
                dataset_ids
                    .iter()
                    .filter_map(|dataset_id| {
                        key_blocks
                            .get(&(dataset_id.clone(), block_ref.cheap_clone()))?
                            .iter()
                            .rev()
                            .find(|block| block.event_kind == event_type)
                            .map(|block| (dataset_id.clone(), block.clone()))
                    })
                    .collect()
            }))
    }

    async fn delete_all_key_blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<(), InternalError> {
        self.state
            .key_blocks
            .write(self.transaction_id, |key_blocks| {
                key_blocks.delete((dataset_id.clone(), block_ref.cheap_clone()));
            });
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRemovalListener for InMemoryDatasetKeyBlockRepository {
    async fn on_dataset_entry_removed(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        self.state
            .key_blocks
            .write(self.transaction_id, |key_blocks| {
                key_blocks.delete_where(|(id, _)| id == dataset_id);
            });
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
