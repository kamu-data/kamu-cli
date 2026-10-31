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
use database_common::{InMemoryMapView, TransactionId, TransactionRef};
use dill::*;
use internal_error::InternalError;
use kamu_datasets::*;

use super::InMemoryDatasetDataBlockState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetDataBlockRepository {
    state: Arc<InMemoryDatasetDataBlockState>,
    transaction_id: Option<TransactionId>,
}

#[component(pub)]
#[interface(dyn DatasetDataBlockRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
impl InMemoryDatasetDataBlockRepository {
    pub fn new(
        state: Arc<InMemoryDatasetDataBlockState>,
        transaction_ref: Option<TransactionRef>,
    ) -> Self {
        Self {
            state,
            transaction_id: transaction_ref.map(|transaction_ref| transaction_ref.id()),
        }
    }

    fn find_data_block<R>(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
        map: impl FnOnce(&DatasetBlock) -> R,
    ) -> Option<R> {
        self.state
            .data_blocks
            .read(self.transaction_id, |data_blocks| {
                Self::dataset_blocks(data_blocks, dataset_id)
                    .find(|block| &block.block_hash == block_hash)
                    .map(map)
            })
    }

    fn dataset_blocks<'a>(
        data_blocks: &InMemoryMapView<'a, (odf::DatasetID, odf::BlockRef), Vec<DatasetBlock>>,
        dataset_id: &'a odf::DatasetID,
    ) -> impl Iterator<Item = &'a DatasetBlock> {
        data_blocks
            .iter()
            .filter(move |((id, _), _)| id == dataset_id)
            .flat_map(|(_, blocks)| blocks.iter())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetDataBlockRepository for InMemoryDatasetDataBlockRepository {
    async fn has_data_blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<bool, InternalError> {
        Ok(self
            .state
            .data_blocks
            .read(self.transaction_id, |data_blocks| {
                data_blocks
                    .get(&(dataset_id.clone(), block_ref.cheap_clone()))
                    .is_some_and(|blocks| !blocks.is_empty())
            }))
    }

    async fn contains_data_block(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
    ) -> Result<bool, InternalError> {
        Ok(self
            .find_data_block(dataset_id, block_hash, |_| ())
            .is_some())
    }

    async fn get_data_block(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
    ) -> Result<Option<DatasetBlock>, InternalError> {
        Ok(self.find_data_block(dataset_id, block_hash, DatasetBlock::clone))
    }

    async fn get_data_block_size(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
    ) -> Result<Option<usize>, InternalError> {
        Ok(self.find_data_block(dataset_id, block_hash, |block| block.block_payload.len()))
    }

    async fn get_page_of_data_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        page_size: usize,
        upper_sequence_number_inclusive: u64,
    ) -> Result<Vec<DatasetBlock>, DatasetDataBlockQueryError> {
        let mut page: Vec<DatasetBlock> =
            self.state
                .data_blocks
                .read(self.transaction_id, |data_blocks| {
                    data_blocks
                        .get(&(dataset_id.clone(), block_ref.cheap_clone()))
                        .map(|blocks| {
                            blocks
                                .iter()
                                .rev()
                                .filter(|b| b.sequence_number <= upper_sequence_number_inclusive)
                                .take(page_size)
                                .cloned()
                                .collect()
                        })
                        .unwrap_or_default()
                });

        // The page is scanned backwards until we take N values,
        // so we need to reverse the final order
        page.reverse();

        Ok(page)
    }

    async fn get_all_data_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<Vec<DatasetBlock>, DatasetDataBlockQueryError> {
        Ok(self
            .state
            .data_blocks
            .read(self.transaction_id, |data_blocks| {
                data_blocks
                    .get(&(dataset_id.clone(), block_ref.cheap_clone()))
                    .cloned()
                    .unwrap_or_default()
            }))
    }

    async fn save_data_blocks_batch(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        blocks: &[DatasetBlock],
    ) -> Result<(), DatasetDataBlockSaveError> {
        if blocks.is_empty() {
            return Ok(());
        }

        let key = (dataset_id.clone(), block_ref.cheap_clone());
        self.state
            .data_blocks
            .write(self.transaction_id, |data_blocks| {
                let mut saved = data_blocks.view().get(&key).cloned().unwrap_or_default();

                let existing: HashSet<_> = saved.iter().map(|b| b.sequence_number).collect();
                if let Some(dup) = blocks
                    .iter()
                    .find(|b| existing.contains(&b.sequence_number))
                {
                    return Err(DatasetDataBlockSaveError::DuplicateSequenceNumber(vec![
                        dup.sequence_number,
                    ]));
                }

                saved.extend_from_slice(blocks);
                saved.sort_by_key(|b| b.sequence_number);
                data_blocks.put(key, saved);
                Ok(())
            })
    }

    async fn delete_all_data_blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<(), InternalError> {
        self.state
            .data_blocks
            .write(self.transaction_id, |data_blocks| {
                data_blocks.delete((dataset_id.clone(), block_ref.cheap_clone()));
            });
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRemovalListener for InMemoryDatasetDataBlockRepository {
    async fn on_dataset_entry_removed(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        self.state
            .data_blocks
            .write(self.transaction_id, |data_blocks| {
                data_blocks.delete_where(|(id, _)| id == dataset_id);
            });
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
