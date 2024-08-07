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
    DatasetAliasSameError,
    DatasetEntry,
    DatasetEntryRepository,
    DatasetNotFoundError,
    DeleteDatasetError,
    GetDatasetError,
    SaveDatasetError,
    SaveDatasetErrorDuplicate,
    UpdateDatasetAliasError,
};
use opendatafabric::{AccountID, DatasetAlias, DatasetID};
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
    async fn get_dataset(&self, dataset_id: &DatasetID) -> Result<DatasetEntry, GetDatasetError> {
        let readable_state = self.state.read().await;

        let maybe_dataset = readable_state.rows.get(dataset_id);

        let Some(dataset) = maybe_dataset else {
            return Err(DatasetNotFoundError::ByDatasetId(dataset_id.clone()).into());
        };

        Ok(dataset.clone())
    }

    async fn get_datasets_by_owner_id(
        &self,
        owner_id: &AccountID,
    ) -> Result<Vec<DatasetEntry>, GetDatasetError> {
        // TODO: PERF: Slow implementation -- to reconsider if it starts to cause us
        //             trouble
        let readable_state = self.state.read().await;

        let datasets = readable_state
            .rows
            .values()
            .fold(vec![], |mut acc, dataset| {
                if dataset.owner_id == *owner_id {
                    acc.push(dataset.clone());
                }

                acc
            });

        if datasets.is_empty() {
            return Err(DatasetNotFoundError::ByOwnerId(owner_id.clone()).into());
        }

        Ok(datasets)
    }

    async fn save_dataset(&self, dataset: &DatasetEntry) -> Result<(), SaveDatasetError> {
        let mut writable_state = self.state.write().await;

        let is_duplicate = writable_state.rows.contains_key(&dataset.id);

        if is_duplicate {
            return Err(SaveDatasetErrorDuplicate::new(dataset.id.clone()).into());
        }

        writable_state
            .rows
            .insert(dataset.id.clone(), dataset.clone());

        Ok(())
    }

    async fn update_dataset_alias(
        &self,
        dataset_id: &DatasetID,
        new_alias: &DatasetAlias,
    ) -> Result<(), UpdateDatasetAliasError> {
        let mut writable_state = self.state.write().await;

        let maybe_dataset = writable_state.rows.get_mut(dataset_id);

        let Some(dataset) = maybe_dataset else {
            return Err(DatasetNotFoundError::ByDatasetId(dataset_id.clone()).into());
        };

        if dataset.alias == *new_alias {
            return Err(DatasetAliasSameError::new(dataset_id.clone(), new_alias.clone()).into());
        }

        dataset.alias = new_alias.clone();

        Ok(())
    }

    async fn delete_dataset(&self, dataset_id: &DatasetID) -> Result<(), DeleteDatasetError> {
        let mut writable_state = self.state.write().await;

        let not_found = writable_state.rows.remove(dataset_id).is_none();

        if not_found {
            return Err(DatasetNotFoundError::ByDatasetId(dataset_id.clone()).into());
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
