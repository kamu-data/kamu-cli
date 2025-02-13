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

use dill::{component, interface, scope, Singleton};
use kamu_datasets::{
    DatasetEntriesResolution,
    DatasetEntry,
    DatasetEntryNotFoundError,
    DatasetEntryService,
    DatasetEntryStream,
    GetDatasetEntryError,
    GetMultipleDatasetEntriesError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FakeDatasetEntryService {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    all_entries_by_id: HashMap<odf::DatasetID, DatasetEntry>,
    entries_by_owner: HashMap<odf::AccountID, Vec<DatasetEntry>>,
}

#[component(pub)]
#[interface(dyn DatasetEntryService)]
#[scope(Singleton)]
impl FakeDatasetEntryService {
    pub fn new() -> Self {
        Self {
            state: Default::default(),
        }
    }

    pub fn add_entry(&self, entry: DatasetEntry) {
        let mut guard = self.state.lock().unwrap();
        guard
            .all_entries_by_id
            .insert(entry.id.clone(), entry.clone());
        guard
            .entries_by_owner
            .entry(entry.owner_id.clone())
            .and_modify(|entries| entries.push(entry.clone()))
            .or_insert_with(|| vec![entry]);
    }
}

#[async_trait::async_trait]
impl DatasetEntryService for FakeDatasetEntryService {
    fn all_entries(&self) -> DatasetEntryStream {
        let ok_entries = self
            .state
            .lock()
            .unwrap()
            .all_entries_by_id
            .values()
            .cloned()
            .map(Ok)
            .collect::<Vec<_>>();

        Box::pin(futures::stream::iter(ok_entries))
    }

    fn entries_owned_by(&self, owner_id: &odf::AccountID) -> DatasetEntryStream {
        let guard = self.state.lock().unwrap();
        let entries = guard
            .entries_by_owner
            .get(owner_id)
            .cloned()
            .unwrap_or_default();
        let ok_entries = entries.into_iter().map(Ok).collect::<Vec<_>>();

        Box::pin(futures::stream::iter(ok_entries))
    }

    async fn get_entry(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetEntry, GetDatasetEntryError> {
        let guard = self.state.lock().unwrap();
        guard
            .all_entries_by_id
            .get(dataset_id)
            .ok_or_else(|| {
                GetDatasetEntryError::NotFound(DatasetEntryNotFoundError {
                    dataset_id: dataset_id.clone(),
                })
            })
            .cloned()
    }

    async fn get_multiple_entries(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<DatasetEntriesResolution, GetMultipleDatasetEntriesError> {
        let guard = self.state.lock().unwrap();

        let res = dataset_ids.iter().fold(
            DatasetEntriesResolution::default(),
            |mut acc, dataset_id| {
                if let Some(dataset_entry) = guard.all_entries_by_id.get(dataset_id) {
                    acc.resolved_entries.push(dataset_entry.clone());
                } else {
                    acc.unresolved_entries.push(dataset_id.clone());
                }

                acc
            },
        );

        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
