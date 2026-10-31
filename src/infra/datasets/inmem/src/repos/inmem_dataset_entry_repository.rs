// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::sync::Arc;

use database_common::{InMemoryMapView, PaginationOpts, TransactionId, TransactionRef};
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::*;

use super::InMemoryDatasetEntryState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Rows<'a> = InMemoryMapView<'a, odf::DatasetID, DatasetEntry>;

pub struct InMemoryDatasetEntryRepository {
    state: Arc<InMemoryDatasetEntryState>,
    transaction_id: Option<TransactionId>,
    removal_listeners: Vec<Arc<dyn DatasetEntryRemovalListener>>,
}

#[component(pub)]
#[interface(dyn DatasetEntryRepository)]
impl InMemoryDatasetEntryRepository {
    pub fn new(
        state: Arc<InMemoryDatasetEntryState>,
        transaction_ref: Option<TransactionRef>,
        removal_listeners: Vec<Arc<dyn DatasetEntryRemovalListener>>,
    ) -> Self {
        Self {
            state,
            transaction_id: transaction_ref.map(|transaction_ref| transaction_ref.id()),
            removal_listeners,
        }
    }

    /// The owner's entries, ordered by dataset ID
    fn owned_entries<'a>(rows: &Rows<'a>, owner_id: &odf::AccountID) -> Vec<&'a DatasetEntry> {
        let mut entries: Vec<_> = rows
            .iter()
            .map(|(_, entry)| entry)
            .filter(|entry| &entry.owner_id == owner_id)
            .collect();
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        entries
    }

    /// Dataset names compare case-insensitively
    fn entry_by_owner_and_name<'a>(
        rows: &Rows<'a>,
        owner_id: &odf::AccountID,
        name: &odf::DatasetName,
    ) -> Option<&'a DatasetEntry> {
        rows.iter()
            .map(|(_, entry)| entry)
            .find(|entry| &entry.owner_id == owner_id && &entry.name == name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRepository for InMemoryDatasetEntryRepository {
    async fn dataset_entries_count(&self) -> Result<usize, DatasetEntriesCountError> {
        Ok(self
            .state
            .rows
            .read(self.transaction_id, |rows| rows.iter().count()))
    }

    async fn dataset_entries_count_by_owner_id(
        &self,
        owner_id: &odf::AccountID,
    ) -> Result<usize, InternalError> {
        Ok(self.state.rows.read(self.transaction_id, |rows| {
            Self::owned_entries(rows, owner_id).len()
        }))
    }

    async fn get_dataset_entries<'a>(
        &'a self,
        pagination: PaginationOpts,
    ) -> DatasetEntryStream<'a> {
        let dataset_entries_page = self.state.rows.read(self.transaction_id, |rows| {
            rows.iter()
                .skip(pagination.offset)
                .take(pagination.limit)
                .map(|(_, entry)| Ok(entry.clone()))
                .collect::<Vec<_>>()
        });

        Box::pin(futures::stream::iter(dataset_entries_page))
    }

    async fn get_dataset_entry(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetEntry, GetDatasetEntryError> {
        self.state
            .rows
            .read(self.transaction_id, |rows| rows.get(dataset_id).cloned())
            .ok_or_else(|| DatasetEntryNotFoundError::new(dataset_id.clone()).into())
    }

    async fn get_multiple_dataset_entries<'a>(
        &'a self,
        dataset_ids: &[Cow<'a, odf::DatasetID>],
    ) -> Result<DatasetEntriesResolution, GetMultipleDatasetEntriesError> {
        Ok(self.state.rows.read(self.transaction_id, |rows| {
            let mut resolution = DatasetEntriesResolution::default();
            for dataset_id in dataset_ids {
                if let Some(dataset_entry) = rows.get(dataset_id) {
                    resolution.resolved_entries.push(dataset_entry.clone());
                } else {
                    resolution
                        .unresolved_entries
                        .push(dataset_id.as_ref().clone());
                }
            }
            resolution
        }))
    }

    async fn get_dataset_entry_by_owner_and_name(
        &self,
        owner_id: &odf::AccountID,
        name: &odf::DatasetName,
    ) -> Result<DatasetEntry, GetDatasetEntryByNameError> {
        self.state
            .rows
            .read(self.transaction_id, |rows| {
                Self::entry_by_owner_and_name(rows, owner_id, name).cloned()
            })
            .ok_or_else(|| {
                DatasetEntryByNameNotFoundError::new(owner_id.clone(), name.clone()).into()
            })
    }

    async fn get_dataset_entries_by_owner_id<'a>(
        &'a self,
        owner_id: &odf::AccountID,
        pagination: PaginationOpts,
    ) -> DatasetEntryStream<'a> {
        let dataset_entries_page = self.state.rows.read(self.transaction_id, |rows| {
            Self::owned_entries(rows, owner_id)
                .into_iter()
                .skip(pagination.offset)
                .take(pagination.limit)
                .map(|entry| Ok(entry.clone()))
                .collect::<Vec<_>>()
        });

        Box::pin(futures::stream::iter(dataset_entries_page))
    }

    async fn get_dataset_entries_by_owner_and_name<'a>(
        &self,
        owner_id_dataset_name_pairs: &'a [&'a (odf::AccountID, odf::DatasetName)],
    ) -> Result<Vec<DatasetEntry>, GetDatasetEntriesByNameError> {
        let mut found_entries = self.state.rows.read(self.transaction_id, |rows| {
            owner_id_dataset_name_pairs
                .iter()
                .filter_map(|(owner_id, dataset_name)| {
                    Self::entry_by_owner_and_name(rows, owner_id, dataset_name).cloned()
                })
                .collect::<Vec<_>>()
        });

        found_entries.sort_by(|a, b| {
            use std::cmp::Ordering;

            match a.owner_name.cmp(&b.owner_name) {
                Ordering::Equal => a.name.cmp(&b.name),
                owner_id_cmp_res @ (Ordering::Greater | Ordering::Less) => owner_id_cmp_res,
            }
        });

        Ok(found_entries)
    }

    async fn save_dataset_entry(
        &self,
        dataset_entry: &DatasetEntry,
    ) -> Result<(), SaveDatasetEntryError> {
        self.state.rows.write(self.transaction_id, |rows| {
            let view = rows.view();
            if view.contains_key(&dataset_entry.id) {
                return Err(SaveDatasetEntryErrorDuplicate::new(dataset_entry.id.clone()).into());
            }
            if Self::entry_by_owner_and_name(&view, &dataset_entry.owner_id, &dataset_entry.name)
                .is_some()
            {
                return Err(DatasetEntryNameCollisionError::new(dataset_entry.name.clone()).into());
            }

            rows.put(dataset_entry.id.clone(), dataset_entry.clone());
            Ok(())
        })
    }

    async fn update_dataset_entry_name(
        &self,
        dataset_id: &odf::DatasetID,
        new_name: &odf::DatasetName,
    ) -> Result<(), UpdateDatasetEntryNameError> {
        self.state.rows.write(self.transaction_id, |rows| {
            let view = rows.view();
            let Some(found_dataset_entry) = view.get(dataset_id).cloned() else {
                return Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into());
            };
            if let Some(existing_entry) =
                Self::entry_by_owner_and_name(&view, &found_dataset_entry.owner_id, new_name)
                && &existing_entry.id != dataset_id
            {
                return Err(DatasetEntryNameCollisionError::new(new_name.clone()).into());
            }

            rows.put(
                dataset_id.clone(),
                DatasetEntry {
                    name: new_name.clone(),
                    ..found_dataset_entry
                },
            );
            Ok(())
        })
    }

    async fn update_owner_entries_after_rename(
        &self,
        owner_id: &odf::AccountID,
        new_owner_name: &odf::AccountName,
    ) -> Result<(), InternalError> {
        self.state.rows.write(self.transaction_id, |rows| {
            let renamed_entries: Vec<_> = Self::owned_entries(&rows.view(), owner_id)
                .into_iter()
                .map(|entry| DatasetEntry {
                    owner_name: new_owner_name.clone(),
                    ..entry.clone()
                })
                .collect();
            for entry in renamed_entries {
                rows.put(entry.id.clone(), entry);
            }
        });
        Ok(())
    }

    async fn delete_dataset_entry(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), DeleteEntryDatasetError> {
        let deletion_result = self
            .delete_dataset_entries(&[Cow::Borrowed(dataset_id)])
            .await
            .map_err(|e| match e {
                DeleteDatasetEntriesError::Internal(e) => DeleteEntryDatasetError::Internal(e),
            })?;

        if deletion_result.deleted_dataset_ids.is_empty() {
            return Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into());
        }

        Ok(())
    }

    async fn delete_dataset_entries<'a>(
        &self,
        dataset_ids: &[Cow<'a, odf::DatasetID>],
    ) -> Result<DatasetEntriesDeletionResult, DeleteDatasetEntriesError> {
        if dataset_ids.is_empty() {
            return Ok(DatasetEntriesDeletionResult::default());
        }

        let deleted_dataset_ids = self.state.rows.write(self.transaction_id, |rows| {
            let deleted_dataset_ids: Vec<_> = dataset_ids
                .iter()
                .filter(|dataset_id| rows.view().contains_key(dataset_id.as_ref()))
                .map(|dataset_id| dataset_id.as_ref().clone())
                .collect();
            for dataset_id in &deleted_dataset_ids {
                rows.delete(dataset_id.clone());
            }
            deleted_dataset_ids
        });

        let deletion_result = DatasetEntriesDeletionResult::from_deleted_dataset_ids(
            dataset_ids,
            deleted_dataset_ids,
        );

        if !deletion_result.deleted_dataset_ids.is_empty() {
            for listener in &self.removal_listeners {
                listener
                    .on_dataset_entries_removed(&deletion_result.deleted_dataset_ids)
                    .await
                    .int_err()?;
            }
        }

        Ok(deletion_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
