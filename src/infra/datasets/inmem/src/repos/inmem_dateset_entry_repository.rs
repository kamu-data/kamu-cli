// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use database_common::PaginationOpts;
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::*;
use opendatafabric::{AccountID, DatasetID, DatasetName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    rows: HashMap<DatasetID, DatasetEntry>,
    rows_by_name: BTreeMap<DatasetName, DatasetEntry>,
    rows_by_owner: HashMap<AccountID, BTreeSet<DatasetID>>,
}

impl State {
    fn new() -> Self {
        Self {
            rows: HashMap::new(),
            rows_by_name: BTreeMap::new(),
            rows_by_owner: HashMap::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetEntryRepository {
    listeners: Vec<Arc<dyn InMemoryDatasetEntryRemovalListener>>,
    state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn DatasetEntryRepository)]
#[scope(Singleton)]
impl InMemoryDatasetEntryRepository {
    pub fn new(listeners: Vec<Arc<dyn InMemoryDatasetEntryRemovalListener>>) -> Self {
        Self {
            listeners,
            state: Arc::new(Mutex::new(State::new())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRepository for InMemoryDatasetEntryRepository {
    async fn dataset_entries_count(&self) -> Result<usize, InternalError> {
        let readable_state = self.state.lock().unwrap();
        Ok(readable_state.rows.len())
    }

    async fn dataset_entries_count_by_owner_id(
        &self,
        owner_id: &AccountID,
    ) -> Result<usize, InternalError> {
        let readable_state = self.state.lock().unwrap();
        let owner_entires = readable_state.rows_by_owner.get(owner_id);
        Ok(owner_entires.map_or(0, BTreeSet::len))
    }

    fn get_dataset_entries(&self, pagination: PaginationOpts) -> DatasetEntryStream {
        let dataset_entries_page: Vec<_> = {
            let readable_state = self.state.lock().unwrap();
            readable_state
                .rows_by_name
                .values()
                .skip(pagination.offset)
                .take(pagination.limit)
                .cloned()
                .map(Ok)
                .collect()
        };

        Box::pin(futures::stream::iter(dataset_entries_page))
    }

    async fn get_dataset_entry(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetEntry, GetDatasetEntryError> {
        let readable_state = self.state.lock().unwrap();

        let maybe_dataset_entry = readable_state.rows.get(dataset_id);

        let Some(dataset_entry) = maybe_dataset_entry else {
            return Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into());
        };

        Ok(dataset_entry.clone())
    }

    async fn get_multiple_dataset_entries(
        &self,
        dataset_ids: &[DatasetID],
    ) -> Result<DatasetEntriesResolution, GetMultipleDatasetEntriesError> {
        let readable_state = self.state.lock().unwrap();

        let mut resolution = DatasetEntriesResolution::default();

        for dataset_id in dataset_ids {
            let maybe_dataset_entry = readable_state.rows.get(dataset_id);
            if let Some(dataset_entry) = maybe_dataset_entry {
                resolution.resolved_entries.push(dataset_entry.clone());
            } else {
                resolution.unresolved_entries.push(dataset_id.clone());
            }
        }

        Ok(resolution)
    }

    async fn get_dataset_entry_by_owner_and_name(
        &self,
        owner_id: &AccountID,
        name: &DatasetName,
    ) -> Result<DatasetEntry, GetDatasetEntryByNameError> {
        let readable_state = self.state.lock().unwrap();

        let maybe_dataset_entry = readable_state
            .rows
            .values()
            .find(|dataset| dataset.owner_id == *owner_id && dataset.name == *name);

        let Some(dataset_entry) = maybe_dataset_entry else {
            return Err(
                DatasetEntryByNameNotFoundError::new(owner_id.clone(), name.clone()).into(),
            );
        };

        Ok(dataset_entry.clone())
    }

    fn get_dataset_entries_by_owner_id(
        &self,
        owner_id: &AccountID,
        pagination: PaginationOpts,
    ) -> DatasetEntryStream<'_> {
        let dataset_entries_page: Vec<_> = {
            let readable_state = self.state.lock().unwrap();
            if let Some(dataset_ids) = readable_state.rows_by_owner.get(owner_id) {
                dataset_ids
                    .iter()
                    .skip(pagination.offset)
                    .take(pagination.limit)
                    .map(|dataset_id| readable_state.rows.get(dataset_id).unwrap())
                    .cloned()
                    .map(Ok)
                    .collect()
            } else {
                vec![]
            }
        };

        Box::pin(futures::stream::iter(dataset_entries_page))
    }

    async fn save_dataset_entry(
        &self,
        dataset_entry: &DatasetEntry,
    ) -> Result<(), SaveDatasetEntryError> {
        let mut writable_state = self.state.lock().unwrap();

        for row in writable_state.rows.values() {
            if row.id == dataset_entry.id {
                return Err(SaveDatasetEntryErrorDuplicate::new(dataset_entry.id.clone()).into());
            }

            if row.owner_id == dataset_entry.owner_id && row.name == dataset_entry.name {
                return Err(DatasetEntryNameCollisionError::new(dataset_entry.name.clone()).into());
            }
        }

        writable_state
            .rows
            .insert(dataset_entry.id.clone(), dataset_entry.clone());

        writable_state
            .rows_by_name
            .insert(dataset_entry.name.clone(), dataset_entry.clone());

        writable_state
            .rows_by_owner
            .entry(dataset_entry.owner_id.clone())
            .and_modify(|owner_dataset_ids| {
                owner_dataset_ids.insert(dataset_entry.id.clone());
            })
            .or_insert_with(|| BTreeSet::from_iter([dataset_entry.id.clone()]));

        Ok(())
    }

    async fn update_dataset_entry_name(
        &self,
        dataset_id: &DatasetID,
        new_name: &DatasetName,
    ) -> Result<(), UpdateDatasetEntryNameError> {
        let mut writable_state = self.state.lock().unwrap();

        let maybe_dataset_entry = writable_state.rows.get(dataset_id);

        let Some(found_dataset_entry) = maybe_dataset_entry else {
            return Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into());
        };

        let has_name_collision_detected = writable_state.rows.values().any(|dataset_entry| {
            dataset_entry.id != *dataset_id
                && dataset_entry.owner_id == found_dataset_entry.owner_id
                && dataset_entry.name == *new_name
        });

        if has_name_collision_detected {
            return Err(DatasetEntryNameCollisionError::new(new_name.clone()).into());
        }

        // To avoid frustrating the borrow checker, we have to do a second look-up.
        // Safety: We're already guaranteed that the entry will be present.
        let old_name = {
            let found_dataset_entry = writable_state.rows.get_mut(dataset_id).unwrap();
            let old_name = found_dataset_entry.name.clone();
            found_dataset_entry.name = new_name.clone();
            old_name
        };

        // Mirror the change in named collection
        let mut entry = writable_state
            .rows_by_name
            .remove(&old_name)
            .expect("named record must be present");
        entry.name = new_name.clone();
        writable_state.rows_by_name.insert(new_name.clone(), entry);

        Ok(())
    }

    async fn delete_dataset_entry(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<(), DeleteEntryDatasetError> {
        {
            let mut writable_state = self.state.lock().unwrap();

            let maybe_removed_entry = writable_state.rows.remove(dataset_id);
            if let Some(removed_entry) = maybe_removed_entry {
                writable_state.rows_by_name.remove(&removed_entry.name);
                writable_state
                    .rows_by_owner
                    .get_mut(&removed_entry.owner_id)
                    .unwrap()
                    .remove(&removed_entry.id);
            } else {
                return Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into());
            }
        }

        for listener in &self.listeners {
            listener
                .on_dataset_entry_removed(dataset_id)
                .await
                .int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait InMemoryDatasetEntryRemovalListener: Send + Sync {
    async fn on_dataset_entry_removed(&self, dataset_id: &DatasetID) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
