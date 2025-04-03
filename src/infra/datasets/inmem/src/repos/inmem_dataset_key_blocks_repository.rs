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

use dill::*;
use internal_error::InternalError;
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    key_blocks: HashMap<(odf::DatasetID, odf::BlockRef), Vec<DatasetKeyBlock>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetKeyBlockRepository {
    state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn DatasetKeyBlockRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
#[scope(Singleton)]
impl InMemoryDatasetKeyBlockRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetKeyBlockRepository for InMemoryDatasetKeyBlockRepository {
    async fn has_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<bool, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(guard
            .key_blocks
            .get(&(dataset_id.clone(), block_ref.clone()))
            .is_some_and(|v| !v.is_empty()))
    }

    async fn save_blocks_batch(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        blocks: &[DatasetKeyBlock],
    ) -> Result<(), DatasetKeyBlockSaveError> {
        if blocks.is_empty() {
            return Ok(());
        }

        let mut guard = self.state.lock().unwrap();
        let entry = guard
            .key_blocks
            .entry((dataset_id.clone(), block_ref.clone()))
            .or_default();

        let existing: std::collections::HashSet<_> =
            entry.iter().map(|b| b.sequence_number).collect();
        if let Some(dup) = blocks
            .iter()
            .find(|b| existing.contains(&b.sequence_number))
        {
            return Err(DatasetKeyBlockSaveError::DuplicateSequenceNumber(vec![
                dup.sequence_number,
            ]));
        }

        entry.extend_from_slice(blocks);
        entry.sort_by_key(|b| b.sequence_number);
        Ok(())
    }

    async fn get_all_key_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<Vec<DatasetKeyBlock>, DatasetKeyBlockQueryError> {
        let guard = self.state.lock().unwrap();
        Ok(guard
            .key_blocks
            .get(&(dataset_id.clone(), block_ref.clone()))
            .cloned()
            .unwrap_or_default())
    }

    async fn find_latest_block_of_kind(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        kind: MetadataEventType,
    ) -> Result<Option<DatasetKeyBlock>, DatasetKeyBlockQueryError> {
        let guard = self.state.lock().unwrap();
        Ok(guard
            .key_blocks
            .get(&(dataset_id.clone(), block_ref.clone()))
            .and_then(|blocks| blocks.iter().rev().find(|b| b.event_kind == kind).cloned()))
    }

    async fn find_blocks_of_kinds_in_range(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        kinds: &[MetadataEventType],
        min_sequence: Option<u64>,
        max_sequence: u64,
    ) -> Result<Vec<DatasetKeyBlock>, DatasetKeyBlockQueryError> {
        let guard = self.state.lock().unwrap();
        let min = min_sequence.unwrap_or(0);

        Ok(guard
            .key_blocks
            .get(&(dataset_id.clone(), block_ref.clone()))
            .map(|blocks| {
                blocks
                    .iter()
                    .filter(|b| {
                        kinds.contains(&b.event_kind)
                            && b.sequence_number >= min
                            && b.sequence_number <= max_sequence
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn find_max_sequence_number(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<Option<u64>, DatasetKeyBlockQueryError> {
        let guard = self.state.lock().unwrap();
        Ok(guard
            .key_blocks
            .get(&(dataset_id.clone(), block_ref.clone()))
            .and_then(|blocks| blocks.last().map(|b| b.sequence_number)))
    }

    async fn delete_blocks_after(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        sequence_number: u64,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        if let Some(blocks) = guard
            .key_blocks
            .get_mut(&(dataset_id.clone(), block_ref.clone()))
        {
            blocks.retain(|b| b.sequence_number <= sequence_number);
        }
        Ok(())
    }

    async fn delete_all_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard
            .key_blocks
            .remove(&(dataset_id.clone(), block_ref.clone()));
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
        let mut guard = self.state.lock().unwrap();
        guard.key_blocks.retain(|(id, _), _| id != dataset_id);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
