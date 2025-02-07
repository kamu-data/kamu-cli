// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
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
    id2names: HashMap<odf::AccountID, odf::AccountName>,
    names2ids: HashMap<odf::AccountName, odf::AccountID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct DatasetsCache {
    datasets_by_id: HashMap<odf::DatasetID, Arc<dyn odf::Dataset>>,
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

    async fn entries_as_handles(
        &self,
        entries: Vec<DatasetEntry>,
    ) -> Result<Vec<odf::DatasetHandle>, ListDatasetEntriesError> {
        // Select which accounts haven't been processed yet
        let first_seen_account_ids = {
            let readable_cache = &self.cache.read().unwrap();

            let mut first_seen_account_ids = HashSet::new();
            for entry in &entries {
                if !readable_cache
                    .accounts
                    .id2names
                    .contains_key(&entry.owner_id)
                {
                    first_seen_account_ids.insert(entry.owner_id.clone());
                }
            }

            first_seen_account_ids
        };

        // Query first seen accounts and fill the table
        if !first_seen_account_ids.is_empty() {
            let account_ids = first_seen_account_ids.into_iter().collect::<Vec<_>>();
            let num_account_ids = account_ids.len();

            let accounts = self
                .account_svc
                .accounts_by_ids(account_ids)
                .await
                .int_err()?;

            assert!(
                accounts.len() == num_account_ids,
                "Number of accounts must match number of requested ids"
            );

            let mut writable_cache = self.cache.write().unwrap();
            for account in accounts {
                writable_cache
                    .accounts
                    .id2names
                    .insert(account.id.clone(), account.account_name.clone());
                writable_cache
                    .accounts
                    .names2ids
                    .insert(account.account_name, account.id);
            }
        }

        // Convert the entries to handles
        let mut handles = Vec::new();
        let readable_cache = self.cache.read().unwrap();
        for entry in &entries {
            // By now we should now the account name
            let maybe_owner_name = readable_cache.accounts.id2names.get(&entry.owner_id);
            if let Some(owner_name) = maybe_owner_name {
                // Form DatasetHandle
                handles.push(odf::DatasetHandle::new(
                    entry.id.clone(),
                    self.tenancy_config
                        .make_alias(owner_name.clone(), entry.name.clone()),
                ));
            }
        }

        // Return converted list
        Ok(handles)
    }

    async fn resolve_account_name_by_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<odf::AccountName, InternalError> {
        let maybe_cached_name = {
            let readable_cache = self.cache.read().unwrap();
            readable_cache.accounts.id2names.get(account_id).cloned()
        };

        if let Some(name) = maybe_cached_name {
            Ok(name)
        } else {
            let account = self
                .account_svc
                .account_by_id(account_id)
                .await
                .int_err()?
                .expect("Account must exist");

            let mut writable_cache = self.cache.write().unwrap();
            writable_cache
                .accounts
                .id2names
                .insert(account_id.clone(), account.account_name.clone());
            writable_cache
                .accounts
                .names2ids
                .insert(account.account_name.clone(), account_id.clone());

            Ok(account.account_name)
        }
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
                    .id2names
                    .insert(account.id.clone(), account_name.clone());
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
        let entries = self
            .dataset_entry_repo
            .get_dataset_entries(pagination)
            .await
            .try_collect()
            .await?;

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
            list: self
                .entries_as_handles(dataset_entry_listing.list)
                .await
                .int_err()?,
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
        let entries = self
            .dataset_entry_repo
            .get_dataset_entries_by_owner_id(owner_id, pagination)
            .await
            .try_collect()
            .await?;

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
            list: self
                .entries_as_handles(dataset_entry_listing.list)
                .await
                .int_err()?,
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
        self.dataset_entry_repo.get_dataset_entry(dataset_id).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl odf::dataset::DatasetHandleResolver for DatasetEntryServiceImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_ref))]
    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<odf::DatasetHandle, odf::dataset::GetDatasetError> {
        match dataset_ref {
            odf::DatasetRef::Handle(h) => Ok(h.clone()),
            odf::DatasetRef::Alias(alias) => {
                let owner_id = self
                    .resolve_account_id_by_maybe_name(alias.account_name.as_ref())
                    .await
                    .map_err(|e| match e {
                        ResolveAccountIdByNameError::NotFound(_) => {
                            odf::dataset::GetDatasetError::NotFound(
                                odf::dataset::DatasetNotFoundError {
                                    dataset_ref: dataset_ref.clone(),
                                },
                            )
                        }
                        ResolveAccountIdByNameError::Internal(e) => {
                            odf::dataset::GetDatasetError::Internal(e)
                        }
                    })?;

                match self
                    .dataset_entry_repo
                    .get_dataset_entry_by_owner_and_name(&owner_id, &alias.dataset_name)
                    .await
                {
                    Ok(entry) => Ok(odf::DatasetHandle::new(
                        entry.id.clone(),
                        odf::DatasetAlias::new(
                            match self.tenancy_config.as_ref() {
                                TenancyConfig::SingleTenant => None,
                                TenancyConfig::MultiTenant => Some(
                                    // We know the name, but since the search is case-insensitive,
                                    // we'd like to know the stored version rather than queried
                                    self.resolve_account_name_by_id(&owner_id).await.int_err()?,
                                ),
                            },
                            entry.name,
                        ),
                    )),
                    Err(GetDatasetEntryByNameError::NotFound(_)) => {
                        Err(odf::dataset::GetDatasetError::NotFound(
                            odf::dataset::DatasetNotFoundError {
                                dataset_ref: dataset_ref.clone(),
                            },
                        ))
                    }
                    Err(GetDatasetEntryByNameError::Internal(e)) => {
                        Err(odf::dataset::GetDatasetError::Internal(e))
                    }
                }
            }
            odf::DatasetRef::ID(id) => match self.dataset_entry_repo.get_dataset_entry(id).await {
                Ok(entry) => {
                    let owner_name = self.resolve_account_name_by_id(&entry.owner_id).await?;
                    Ok(odf::DatasetHandle::new(
                        entry.id.clone(),
                        self.tenancy_config
                            .make_alias(owner_name, entry.name.clone()),
                    ))
                }
                Err(GetDatasetEntryError::NotFound(_)) => Err(
                    odf::dataset::GetDatasetError::NotFound(odf::dataset::DatasetNotFoundError {
                        dataset_ref: dataset_ref.clone(),
                    }),
                ),
                Err(GetDatasetEntryError::Internal(e)) => {
                    Err(odf::dataset::GetDatasetError::Internal(e))
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
        let entries_resolution = self
            .dataset_entry_repo
            .get_multiple_dataset_entries(&dataset_ids)
            .await
            .map_err(|e| match e {
                GetMultipleDatasetEntriesError::Internal(e) => {
                    GetMultipleDatasetsError::Internal(e)
                }
            })?;

        let resolved_handles = self
            .entries_as_handles(entries_resolution.resolved_entries)
            .await
            .map_err(|e| match e {
                ListDatasetEntriesError::Internal(e) => GetMultipleDatasetsError::Internal(e),
            })?;

        let unresolved_datasets = entries_resolution
            .unresolved_entries
            .into_iter()
            .map(|id| {
                (
                    id.clone(),
                    odf::dataset::GetDatasetError::NotFound(odf::dataset::DatasetNotFoundError {
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
    fn get_dataset_by_handle(&self, dataset_handle: &odf::DatasetHandle) -> ResolvedDataset {
        let writable_cache = &mut self.cache.write().unwrap();
        let dataset = if let Some(dataset) = writable_cache
            .datasets
            .datasets_by_id
            .get(&dataset_handle.id)
        {
            dataset.clone()
        } else {
            // Note: in future we will be resolving storage repository,
            // but for now we have just a single one
            let dataset = self
                .dataset_storage_unit
                .get_stored_dataset_by_handle(dataset_handle);

            writable_cache
                .datasets
                .datasets_by_id
                .insert(dataset_handle.id.clone(), dataset.clone());

            dataset
        };

        ResolvedDataset::new(dataset, dataset_handle.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryWriter for DatasetEntryServiceImpl {
    async fn create_entry(
        &self,
        dataset_id: &odf::DatasetID,
        owner_account_id: &odf::AccountID,
        dataset_name: &odf::DatasetName,
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
            dataset_name.clone(),
            self.time_source.now(),
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
            })
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
            })
    }

    async fn remove_entry(&self, dataset_handle: &odf::DatasetHandle) -> Result<(), InternalError> {
        match self
            .dataset_entry_repo
            .delete_dataset_entry(&dataset_handle.id)
            .await
        {
            Ok(_) | Err(DeleteEntryDatasetError::NotFound(_)) => Ok(()),
            Err(DeleteEntryDatasetError::Internal(e)) => Err(e),
        }
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
