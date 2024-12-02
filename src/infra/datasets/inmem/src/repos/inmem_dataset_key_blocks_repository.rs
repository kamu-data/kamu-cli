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
use kamu_datasets::{DatasetKeyBlockRow, DatasetKeyBlockType, DatasetKeyBlocksRepository};
use opendatafabric::DatasetID;

use super::InMemoryDatasetEntryRemovalListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    key_blocks_by_dataset: HashMap<DatasetID, DatasetKeyBlocks>,
}

#[derive(Default)]
struct DatasetKeyBlocks {
    blocks_by_type: HashMap<(DatasetKeyBlockType, Option<String>), DatasetKeyBlockRow>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetKeyBlocksRepository {
    state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn DatasetKeyBlocksRepository)]
#[interface(dyn InMemoryDatasetEntryRemovalListener)]
#[scope(Singleton)]
impl InMemoryDatasetKeyBlocksRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetKeyBlocksRepository for InMemoryDatasetKeyBlocksRepository {
    async fn save_key_dataset_block(
        &self,
        dataset_id: &DatasetID,
        key_block_row: DatasetKeyBlockRow,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();

        if let Some(dataset_key_blocks) = guard.key_blocks_by_dataset.get_mut(dataset_id) {
            dataset_key_blocks.blocks_by_type.insert(
                (
                    key_block_row.key_block_type(),
                    key_block_row.key_block_extra_key(),
                ),
                key_block_row,
            );
        } else {
            guard.key_blocks_by_dataset.insert(
                dataset_id.clone(),
                DatasetKeyBlocks {
                    blocks_by_type: HashMap::from_iter([(
                        (
                            key_block_row.key_block_type(),
                            key_block_row.key_block_extra_key(),
                        ),
                        key_block_row,
                    )]),
                },
            );
        }

        Ok(())
    }

    async fn drop_key_dataset_blocks_after(
        &self,
        dataset_id: &DatasetID,
        sequence_number: u64,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();

        if let Some(dataset_key_blocks) = guard.key_blocks_by_dataset.get_mut(dataset_id) {
            dataset_key_blocks
                .blocks_by_type
                .retain(|_, row| row.sequence_number <= sequence_number);
        }

        Ok(())
    }

    async fn try_loading_key_dataset_blocks(
        &self,
        dataset_id: &DatasetID,
        block_types: &[DatasetKeyBlockType],
    ) -> Result<Vec<DatasetKeyBlockRow>, InternalError> {
        let guard = self.state.lock().unwrap();

        if let Some(dataset_key_blocks) = guard.key_blocks_by_dataset.get(dataset_id) {
            let mut res = Vec::new();
            for ((block_type, _), row) in &dataset_key_blocks.blocks_by_type {
                if block_types.iter().any(|bt| bt == block_type) {
                    res.push((*row).clone());
                }
            }
            Ok(res)
        } else {
            Ok(vec![])
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InMemoryDatasetEntryRemovalListener for InMemoryDatasetKeyBlocksRepository {
    async fn on_dataset_entry_removed(&self, dataset_id: &DatasetID) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();

        guard.key_blocks_by_dataset.remove(dataset_id);

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
