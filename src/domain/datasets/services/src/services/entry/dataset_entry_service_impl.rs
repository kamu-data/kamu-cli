// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use database_common::{EntityPageListing, EntityPageStreamer, PaginationOpts};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::{
    AccountNotFoundByNameError,
    AccountService,
    CurrentAccountSubject,
    GetAccountByNameError,
};
use kamu_core::{
    DatasetHandlesResolution,
    DatasetRegistry,
    GetMultipleDatasetsError,
    ResolvedDataset,
    TenancyConfig,
};
use kamu_datasets::*;
use thiserror::Error;
use time_source::SystemTimeSource;

use super::{CreateDatasetEntryError, DatasetEntryWriter, RenameDatasetEntryError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEntryServiceImpl {
    time_source: Arc<dyn SystemTimeSource>,
    dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
    dataset_storage_unit: Arc<dyn odf::DatasetStorageUnit>,
    account_svc: Arc<dyn AccountService>,
    current_account_subject: Arc<CurrentAccountSubject>,
    tenancy_config: Arc<TenancyConfig>,
    cache: Arc<RwLock<Cache>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct Cache {
    accounts: AccountsCache,
    datasets: DatasetsCache,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct AccountsCache {
    names2ids: HashMap<odf::AccountName, odf::AccountID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct DatasetsCache {
    datasets_by_id: HashMap<odf::DatasetID, Arc<dyn odf::Dataset>>,
    entries_by_id: HashMap<odf::DatasetID, DatasetEntry>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetEntryService)]
#[interface(dyn DatasetEntryWriter)]
#[interface(dyn DatasetRegistry)]
impl DatasetEntryServiceImpl {
    pub fn new(
        time_source: Arc<dyn SystemTimeSource>,
        dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
        dataset_storage_unit: Arc<dyn odf::DatasetStorageUnit>,
        account_svc: Arc<dyn AccountService>,
        current_account_subject: Arc<CurrentAccountSubject>,
        tenancy_config: Arc<TenancyConfig>,
    ) -> Self {
        Self {
            time_source,
            dataset_entry_repo,
            dataset_storage_unit,
            account_svc,
            current_account_subject,
            tenancy_config,
            cache: Arc::new(RwLock::new(Cache::default())),
        }
    }

    fn update_entries_cache<'a, I>(&self, entries: I)
    where
        I: IntoIterator<Item = &'a DatasetEntry>,
    {
        let mut writable_cache = self.cache.write().unwrap();
        for entry in entries {
            writable_cache
                .datasets
                .entries_by_id
                .insert(entry.id.clone(), entry.clone());
        }
    }

    fn entries_as_handles(&self, entries: &[DatasetEntry]) -> Vec<odf::DatasetHandle> {
        // Convert the entries to handles
        let mut handles = Vec::new();
        for entry in entries {
            // Form DatasetHandle
            handles.push(odf::DatasetHandle::new(
                entry.id.clone(),
                self.tenancy_config
                    .make_alias(entry.owner_name.clone(), entry.name.clone()),
                entry.kind,
            ));
        }

        // Return converted list
        handles
    }

    async fn resolve_account_id_by_maybe_name(
        &self,
        maybe_account_name: Option<&odf::AccountName>,
    ) -> Result<odf::AccountID, ResolveAccountIdByNameError> {
        let account_name = maybe_account_name
            .unwrap_or_else(|| self.current_account_subject.account_name_or_default());

        let maybe_cached_id = {
            let readable_cache = self.cache.read().unwrap();
            readable_cache.accounts.names2ids.get(account_name).cloned()
        };

        if let Some(id) = maybe_cached_id {
            Ok(id)
        } else {
            let maybe_account = self.account_svc.account_by_name(account_name).await?;

            if let Some(account) = maybe_account {
                let mut writable_cache = self.cache.write().unwrap();
                writable_cache
                    .accounts
                    .names2ids
                    .insert(account_name.clone(), account.id.clone());

                Ok(account.id)
            } else {
                Err(ResolveAccountIdByNameError::NotFound(
                    AccountNotFoundByNameError {
                        account_name: account_name.clone(),
                    },
                ))
            }
        }
    }

    async fn list_all_entries(
        &self,
        pagination: PaginationOpts,
    ) -> Result<EntityPageListing<DatasetEntry>, ListDatasetEntriesError> {
        use futures::TryStreamExt;

        let total_count = self
            .dataset_entry_repo
            .dataset_entries_count()
            .await
            .int_err()?;
        let entries: Vec<DatasetEntry> = self
            .dataset_entry_repo
            .get_dataset_entries(pagination)
            .await
            .try_collect()
            .await?;

        self.update_entries_cache(&entries);

        Ok(EntityPageListing {
            list: entries,
            total_count,
        })
    }

    async fn list_all_dataset_handles(
        &self,
        pagination: PaginationOpts,
    ) -> Result<EntityPageListing<odf::DatasetHandle>, InternalError> {
        let dataset_entry_listing = self.list_all_entries(pagination).await.int_err()?;

        Ok(EntityPageListing {
            total_count: dataset_entry_listing.total_count,
            list: self.entries_as_handles(&dataset_entry_listing.list),
        })
    }

    async fn list_entries_owned_by(
        &self,
        owner_id: &odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<EntityPageListing<DatasetEntry>, ListDatasetEntriesError> {
        use futures::TryStreamExt;

        let total_count = self
            .dataset_entry_repo
            .dataset_entries_count_by_owner_id(owner_id)
            .await?;
        let entries: Vec<DatasetEntry> = self
            .dataset_entry_repo
            .get_dataset_entries_by_owner_id(owner_id, pagination)
            .await
            .try_collect()
            .await?;

        self.update_entries_cache(&entries);

        Ok(EntityPageListing {
            list: entries,
            total_count,
        })
    }

    async fn list_all_dataset_handles_by_owner_id(
        &self,
        owner_id: &odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<EntityPageListing<odf::DatasetHandle>, InternalError> {
        let dataset_entry_listing = self
            .list_entries_owned_by(owner_id, pagination)
            .await
            .int_err()?;

        Ok(EntityPageListing {
            total_count: dataset_entry_listing.total_count,
            list: self.entries_as_handles(&dataset_entry_listing.list),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryService for DatasetEntryServiceImpl {
    fn all_entries(&self) -> DatasetEntryStream {
        EntityPageStreamer::default().into_stream(
            || async { Ok(()) },
            |_, pagination| {
                let list_fut = self.list_all_entries(pagination);
                async { list_fut.await.int_err() }
            },
        )
    }

    fn entries_owned_by(&self, owner_id: &odf::AccountID) -> DatasetEntryStream {
        let owner_id = owner_id.clone();

        EntityPageStreamer::default().into_stream(
            || async { Ok(Arc::new(owner_id)) },
            move |owner_id, pagination| async move {
                self.list_entries_owned_by(&owner_id, pagination)
                    .await
                    .int_err()
            },
        )
    }

    async fn get_entry(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetEntry, GetDatasetEntryError> {
        // Try cache first
        {
            let readable_cache = &self.cache.read().unwrap();
            if let Some(dataset_entry) = readable_cache.datasets.entries_by_id.get(dataset_id) {
                return Ok(dataset_entry.clone());
            }
        }

        // No luck, resolve in db
        let dataset_entry = self
            .dataset_entry_repo
            .get_dataset_entry(dataset_id)
            .await?;

        // Update cache
        self.update_entries_cache(std::iter::once(&dataset_entry));

        Ok(dataset_entry)
    }

    async fn get_multiple_entries(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<DatasetEntriesResolution, GetMultipleDatasetEntriesError> {
        let mut cached_entries = Vec::with_capacity(dataset_ids.len());
        let mut missing_ids = Vec::with_capacity(dataset_ids.len());

        // Try if some of these IDs have cached entries
        {
            // Read lock on cache
            let readable_cache = self.cache.read().unwrap();
            for dataset_id in dataset_ids {
                if let Some(entry) = readable_cache.datasets.entries_by_id.get(dataset_id) {
                    cached_entries.push(entry.clone());
                } else {
                    missing_ids.push(dataset_id.clone());
                }
            }
        }

        Ok(if !missing_ids.is_empty() {
            // Query missing entries from DB
            let mut fetched: DatasetEntriesResolution = self
                .dataset_entry_repo
                .get_multiple_dataset_entries(&missing_ids)
                .await?;

            // Update cache with fetched entries
            self.update_entries_cache(fetched.resolved_entries.iter());

            // Extend fetched with cached
            fetched.resolved_entries.extend(cached_entries);

            fetched
        } else {
            // Purely cached resolution
            DatasetEntriesResolution {
                resolved_entries: cached_entries,
                unresolved_entries: vec![],
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl odf::dataset::DatasetHandleResolver for DatasetEntryServiceImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_ref))]
    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<odf::DatasetHandle, odf::DatasetRefUnresolvedError> {
        match dataset_ref {
            odf::DatasetRef::Handle(h) => Ok(h.clone()),
            odf::DatasetRef::Alias(alias) => {
                let owner_id = self
                    .resolve_account_id_by_maybe_name(alias.account_name.as_ref())
                    .await
                    .map_err(|e| match e {
                        ResolveAccountIdByNameError::NotFound(_) => {
                            odf::DatasetRefUnresolvedError::NotFound(odf::DatasetNotFoundError {
                                dataset_ref: dataset_ref.clone(),
                            })
                        }
                        ResolveAccountIdByNameError::Internal(e) => {
                            odf::DatasetRefUnresolvedError::Internal(e)
                        }
                    })?;

                match self
                    .dataset_entry_repo
                    .get_dataset_entry_by_owner_and_name(&owner_id, &alias.dataset_name)
                    .await
                {
                    Ok(entry) => {
                        self.update_entries_cache(std::iter::once(&entry));

                        Ok(odf::DatasetHandle::new(
                            entry.id.clone(),
                            odf::DatasetAlias::new(
                                match self.tenancy_config.as_ref() {
                                    TenancyConfig::SingleTenant => None,
                                    TenancyConfig::MultiTenant => Some(
                                        // We know the name, but since the search is
                                        // case-insensitive, we'd like to know the stored version
                                        // rather than queried
                                        entry.owner_name.clone(),
                                    ),
                                },
                                entry.name,
                            ),
                            entry.kind,
                        ))
                    }
                    Err(GetDatasetEntryByNameError::NotFound(_)) => Err(
                        odf::DatasetRefUnresolvedError::NotFound(odf::DatasetNotFoundError {
                            dataset_ref: dataset_ref.clone(),
                        }),
                    ),
                    Err(GetDatasetEntryByNameError::Internal(e)) => {
                        Err(odf::DatasetRefUnresolvedError::Internal(e))
                    }
                }
            }
            odf::DatasetRef::ID(id) => match self.get_entry(id).await {
                Ok(entry) => Ok(odf::DatasetHandle::new(
                    entry.id.clone(),
                    self.tenancy_config
                        .make_alias(entry.owner_name, entry.name.clone()),
                    entry.kind,
                )),
                Err(GetDatasetEntryError::NotFound(_)) => Err(
                    odf::DatasetRefUnresolvedError::NotFound(odf::DatasetNotFoundError {
                        dataset_ref: dataset_ref.clone(),
                    }),
                ),
                Err(GetDatasetEntryError::Internal(e)) => {
                    Err(odf::DatasetRefUnresolvedError::Internal(e))
                }
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetRegistry for DatasetEntryServiceImpl {
    #[tracing::instrument(level = "debug", skip_all)]
    fn all_dataset_handles(&self) -> odf::dataset::DatasetHandleStream {
        EntityPageStreamer::default().into_stream(
            || async { Ok(()) },
            |_, pagination| self.list_all_dataset_handles(pagination),
        )
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%owner_name))]
    fn all_dataset_handles_by_owner(
        &self,
        owner_name: &odf::AccountName,
    ) -> odf::dataset::DatasetHandleStream {
        let owner_name = owner_name.clone();

        EntityPageStreamer::default().into_stream(
            move || async move {
                let owner_id = match self
                    .resolve_account_id_by_maybe_name(Some(&owner_name))
                    .await
                {
                    Ok(owner_id) => Some(owner_id),
                    Err(ResolveAccountIdByNameError::NotFound(_)) => None,
                    Err(e) => return Err(e.int_err()),
                };
                Ok(Arc::new(owner_id))
            },
            move |owner_id_maybe, pagination| async move {
                if let Some(owner_id) = owner_id_maybe.as_ref() {
                    return self
                        .list_all_dataset_handles_by_owner_id(owner_id, pagination)
                        .await;
                }

                Ok(EntityPageListing {
                    list: Vec::new(),
                    total_count: 0,
                })
            },
        )
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?dataset_ids))]
    async fn resolve_multiple_dataset_handles_by_ids(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
    ) -> Result<DatasetHandlesResolution, GetMultipleDatasetsError> {
        let entries_resolution = self.get_multiple_entries(&dataset_ids).await.int_err()?;

        let resolved_handles = self.entries_as_handles(&entries_resolution.resolved_entries);

        let unresolved_datasets = entries_resolution
            .unresolved_entries
            .into_iter()
            .map(|id| {
                (
                    id.clone(),
                    odf::DatasetRefUnresolvedError::NotFound(odf::DatasetNotFoundError {
                        dataset_ref: id.into_local_ref(),
                    }),
                )
            })
            .collect();

        Ok(DatasetHandlesResolution {
            resolved_handles,
            unresolved_datasets,
        })
    }

    // Note: in future we will be resolving storage repository,
    // but for now we have just a single one
    async fn get_dataset_by_handle(&self, dataset_handle: &odf::DatasetHandle) -> ResolvedDataset {
        let maybe_cached_dataset = {
            let readable_cache = self.cache.read().unwrap();
            readable_cache
                .datasets
                .datasets_by_id
                .get(&dataset_handle.id)
                .cloned()
        };

        if let Some(cached_dataset) = maybe_cached_dataset {
            ResolvedDataset::new(cached_dataset, dataset_handle.clone())
        } else {
            // Note: in future we will be resolving storage repository,
            // but for now we have just a single one
            let dataset = self
                .dataset_storage_unit
                .get_stored_dataset_by_id(&dataset_handle.id)
                .await
                .expect("Dataset must exist");

            let writable_cache = &mut self.cache.write().unwrap();
            writable_cache
                .datasets
                .datasets_by_id
                .insert(dataset_handle.id.clone(), dataset.clone());

            ResolvedDataset::new(dataset, dataset_handle.clone())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryWriter for DatasetEntryServiceImpl {
    async fn create_entry(
        &self,
        dataset_id: &odf::DatasetID,
        owner_account_id: &odf::AccountID,
        owner_account_name: &odf::AccountName,
        dataset_name: &odf::DatasetName,
        dataset_kind: odf::DatasetKind,
    ) -> Result<(), CreateDatasetEntryError> {
        match self.dataset_entry_repo.get_dataset_entry(dataset_id).await {
            Ok(_) => return Ok(()), // idempotent handling of duplicates
            Err(GetDatasetEntryError::NotFound(_)) => { /* happy case, create record */ }
            Err(GetDatasetEntryError::Internal(e)) => {
                return Err(CreateDatasetEntryError::Internal(e))
            }
        }

        let entry = DatasetEntry::new(
            dataset_id.clone(),
            owner_account_id.clone(),
            owner_account_name.clone(),
            dataset_name.clone(),
            self.time_source.now(),
            dataset_kind,
        );

        self.dataset_entry_repo
            .save_dataset_entry(&entry)
            .await
            .map_err(|e| match e {
                SaveDatasetEntryError::Duplicate(e) => CreateDatasetEntryError::DuplicateId(e),
                SaveDatasetEntryError::NameCollision(e) => {
                    CreateDatasetEntryError::NameCollision(e)
                }
                SaveDatasetEntryError::Internal(e) => CreateDatasetEntryError::Internal(e),
            })?;

        self.update_entries_cache(std::iter::once(&entry));

        Ok(())
    }

    async fn rename_entry(
        &self,
        dataset_handle: &odf::DatasetHandle,
        new_dataset_name: &odf::DatasetName,
    ) -> Result<(), RenameDatasetEntryError> {
        self.dataset_entry_repo
            .update_dataset_entry_name(&dataset_handle.id, new_dataset_name)
            .await
            .map_err(|e| match e {
                UpdateDatasetEntryNameError::Internal(e) => RenameDatasetEntryError::Internal(e),
                UpdateDatasetEntryNameError::NotFound(e) => {
                    // should not happen normally, since we resolved the handle earlier
                    RenameDatasetEntryError::Internal(e.int_err())
                }
                UpdateDatasetEntryNameError::NameCollision(e) => {
                    RenameDatasetEntryError::NameCollision(e)
                }
            })?;

        // Update name in cache, if cached
        {
            let mut writable_cache = self.cache.write().unwrap();
            if let Some(entry) = writable_cache
                .datasets
                .entries_by_id
                .get_mut(&dataset_handle.id)
            {
                entry.name = new_dataset_name.clone();
            }
        }

        Ok(())
    }

    async fn remove_entry(&self, dataset_handle: &odf::DatasetHandle) -> Result<(), InternalError> {
        // Remove entry in repository
        match self
            .dataset_entry_repo
            .delete_dataset_entry(&dataset_handle.id)
            .await
        {
            Ok(_) | Err(DeleteEntryDatasetError::NotFound(_)) => Ok(()),
            Err(DeleteEntryDatasetError::Internal(e)) => Err(e),
        }?;

        // Remove entry from cache
        {
            let mut writable_cache = self.cache.write().unwrap();
            writable_cache
                .datasets
                .datasets_by_id
                .remove(&dataset_handle.id);
            writable_cache
                .datasets
                .entries_by_id
                .remove(&dataset_handle.id);
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ResolveAccountIdByNameError {
    #[error(transparent)]
    Internal(#[from] InternalError),

    #[error(transparent)]
    NotFound(AccountNotFoundByNameError),
}

impl From<GetAccountByNameError> for ResolveAccountIdByNameError {
    fn from(e: GetAccountByNameError) -> Self {
        match e {
            GetAccountByNameError::NotFound(e) => ResolveAccountIdByNameError::NotFound(e),
            GetAccountByNameError::Internal(e) => ResolveAccountIdByNameError::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
