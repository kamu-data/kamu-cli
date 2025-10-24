// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cheap_clone::CheapClone;
use dill::*;
use internal_error::InternalError;
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    data_blocks: HashMap<(odf::DatasetID, odf::BlockRef), Vec<DatasetBlock>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetDataBlockRepository {
    state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn DatasetDataBlockRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
#[scope(Singleton)]
impl InMemoryDatasetDataBlockRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
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
        let guard = self.state.lock().unwrap();
        Ok(guard
            .data_blocks
            .get(&(dataset_id.clone(), block_ref.cheap_clone()))
            .is_some_and(|v| !v.is_empty()))
    }

    async fn contains_data_block(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
    ) -> Result<bool, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(guard.data_blocks.iter().any(|((id, _), blocks)| {
            id == dataset_id && blocks.iter().any(|block| &block.block_hash == block_hash)
        }))
    }

    async fn get_data_block(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
    ) -> Result<Option<DatasetBlock>, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(guard.data_blocks.iter().find_map(|((id, _), blocks)| {
            if id == dataset_id {
                blocks
                    .iter()
                    .find(|block| &block.block_hash == block_hash)
                    .cloned()
            } else {
                None
            }
        }))
    }

    async fn get_data_block_size(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
    ) -> Result<Option<usize>, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(guard.data_blocks.iter().find_map(|((id, _), blocks)| {
            if id == dataset_id {
                blocks
                    .iter()
                    .find(|block| &block.block_hash == block_hash)
                    .map(|block| block.block_payload.len())
            } else {
                None
            }
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

        let mut guard = self.state.lock().unwrap();
        let entry = guard
            .data_blocks
            .entry((dataset_id.clone(), block_ref.cheap_clone()))
            .or_default();

        let existing: std::collections::HashSet<_> =
            entry.iter().map(|b| b.sequence_number).collect();
        if let Some(dup) = blocks
            .iter()
            .find(|b| existing.contains(&b.sequence_number))
        {
            return Err(DatasetDataBlockSaveError::DuplicateSequenceNumber(vec![
                dup.sequence_number,
            ]));
        }

        entry.extend_from_slice(blocks);
        entry.sort_by_key(|b| b.sequence_number);
        Ok(())
    }

    async fn get_all_data_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<Vec<DatasetBlock>, DatasetDataBlockQueryError> {
        let guard = self.state.lock().unwrap();
        Ok(guard
            .data_blocks
            .get(&(dataset_id.clone(), block_ref.cheap_clone()))
            .cloned()
            .unwrap_or_default())
    }

    async fn delete_all_data_blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard
            .data_blocks
            .remove(&(dataset_id.clone(), block_ref.cheap_clone()));
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
        let mut guard = self.state.lock().unwrap();
        guard.data_blocks.retain(|(id, _), _| id != dataset_id);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
