// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use dill::{component, interface, scope, Singleton};
use kamu_datasets::{
    DatasetEntry,
    DatasetEntryNotFoundError,
    DatasetEntryRepository,
    DeleteEntryDatasetError,
    GetDatasetEntryError,
    SaveDatasetEntryError,
    SaveDatasetEntryErrorDuplicate,
    UpdateDatasetEntryNameError,
};
use opendatafabric::{AccountID, DatasetID, DatasetName};
use tokio::sync::RwLock;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    rows: HashMap<DatasetID, DatasetEntry>,
}

impl State {
    fn new() -> Self {
        Self {
            rows: HashMap::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEntryRepositoryInMemory {
    state: Arc<RwLock<State>>,
}

#[component(pub)]
#[interface(dyn DatasetEntryRepository)]
#[scope(Singleton)]
impl DatasetEntryRepositoryInMemory {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State::new())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRepository for DatasetEntryRepositoryInMemory {
    async fn get_dataset_entry(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetEntry, GetDatasetEntryError> {
        let readable_state = self.state.read().await;

        let maybe_dataset_entry = readable_state.rows.get(dataset_id);

        let Some(dataset_entry) = maybe_dataset_entry else {
            return Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into());
        };

        Ok(dataset_entry.clone())
    }

    async fn get_dataset_entries_by_owner_id(
        &self,
        owner_id: &AccountID,
    ) -> Result<Vec<DatasetEntry>, GetDatasetEntryError> {
        // TODO: PERF: Slow implementation -- to reconsider if it starts to cause us
        //             trouble
        let readable_state = self.state.read().await;

        let dataset_entries = readable_state
            .rows
            .values()
            .fold(vec![], |mut acc, dataset| {
                if dataset.owner_id == *owner_id {
                    acc.push(dataset.clone());
                }

                acc
            });

        Ok(dataset_entries)
    }

    async fn save_dataset_entry(
        &self,
        dataset: &DatasetEntry,
    ) -> Result<(), SaveDatasetEntryError> {
        let mut writable_state = self.state.write().await;

        let is_duplicate = writable_state.rows.contains_key(&dataset.id);

        if is_duplicate {
            return Err(SaveDatasetEntryErrorDuplicate::new(dataset.id.clone()).into());
        }

        writable_state
            .rows
            .insert(dataset.id.clone(), dataset.clone());

        Ok(())
    }

    async fn update_dataset_entry_name(
        &self,
        dataset_id: &DatasetID,
        new_name: &DatasetName,
    ) -> Result<(), UpdateDatasetEntryNameError> {
        let mut writable_state = self.state.write().await;

        let maybe_dataset_entry = writable_state.rows.get_mut(dataset_id);

        let Some(dataset_entry) = maybe_dataset_entry else {
            return Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into());
        };

        dataset_entry.name = new_name.clone();

        Ok(())
    }

    async fn delete_dataset_entry(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<(), DeleteEntryDatasetError> {
        let mut writable_state = self.state.write().await;

        let not_found = writable_state.rows.remove(dataset_id).is_none();

        if not_found {
            return Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into());
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
