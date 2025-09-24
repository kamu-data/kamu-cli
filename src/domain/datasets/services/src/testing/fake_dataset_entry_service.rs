// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use database_common::{BatchLookup, BatchLookupCreateOptions};
use dill::{Singleton, component, interface, scope};
use internal_error::InternalError;
use kamu_datasets::{
    DatasetEntriesResolution,
    DatasetEntry,
    DatasetEntryNotFoundError,
    DatasetEntryService,
    DatasetEntryStream,
    GetDatasetEntryByNameError,
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
        dataset_ids: &[Cow<odf::DatasetID>],
    ) -> Result<DatasetEntriesResolution, GetMultipleDatasetEntriesError> {
        let guard = self.state.lock().unwrap();

        let res = dataset_ids.iter().fold(
            DatasetEntriesResolution::default(),
            |mut acc, dataset_id| {
                if let Some(dataset_entry) = guard.all_entries_by_id.get(dataset_id) {
                    acc.resolved_entries.push(dataset_entry.clone());
                } else {
                    acc.unresolved_entries.push(dataset_id.as_ref().clone());
                }

                acc
            },
        );

        Ok(res)
    }

    async fn get_dataset_entries_by_owner_and_name(
        &self,
        owner_id_dataset_name_pairs: &[&(odf::AccountID, odf::DatasetName)],
    ) -> Result<
        BatchLookup<DatasetEntry, (odf::AccountID, odf::DatasetName), GetDatasetEntryByNameError>,
        InternalError,
    > {
        let guard = self.state.lock().unwrap();

        let found_entries = owner_id_dataset_name_pairs
            .iter()
            .filter_map(|(owner_id, dataset_name)| {
                guard
                    .entries_by_owner
                    .get(owner_id)
                    .and_then(|entries| entries.iter().find(|e| &e.name == dataset_name))
                    .cloned()
            })
            .collect::<Vec<_>>();

        Ok(BatchLookup::from_found_items(
            found_entries,
            owner_id_dataset_name_pairs,
            BatchLookupCreateOptions {
                found_ids_fn: |entries| {
                    entries
                        .iter()
                        .map(|entry| (entry.owner_id.clone(), entry.name.clone()))
                        .collect()
                },
                not_found_err_fn: |(owner_id, dataset_name)| {
                    kamu_datasets::DatasetEntryByNameNotFoundError::new(
                        (*owner_id).clone(),
                        (*dataset_name).clone(),
                    )
                    .into()
                },
                maybe_found_items_comparator: Some(|a: &DatasetEntry, b: &DatasetEntry| {
                    use std::cmp::Ordering;

                    match a.owner_name.cmp(&b.owner_name) {
                        Ordering::Equal => a.name.cmp(&b.name),
                        owner_id_cmp_res @ (Ordering::Greater | Ordering::Less) => owner_id_cmp_res,
                    }
                }),
                _phantom: Default::default(),
            },
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
