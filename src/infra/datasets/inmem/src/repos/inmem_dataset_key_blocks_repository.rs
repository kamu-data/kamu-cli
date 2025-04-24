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

    async fn filter_datasets_having_blocks(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
        block_ref: &odf::BlockRef,
        event_type: MetadataEventType,
    ) -> Result<Vec<odf::DatasetID>, InternalError> {
        let guard = self.state.lock().unwrap();
        let mut result = Vec::new();

        for dataset_id in dataset_ids {
            if let Some(blocks) = guard
                .key_blocks
                .get(&(dataset_id.clone(), block_ref.clone()))
            {
                if blocks.iter().any(|block| block.event_kind == event_type) {
                    result.push(dataset_id);
                }
            }
        }

        Ok(result)
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
