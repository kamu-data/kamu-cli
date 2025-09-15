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
use std::sync::{Arc, RwLock};

use database_common::{
    BatchLookup,
    BatchLookupCreateOptions,
    EntityPageListing,
    EntityPageStreamer,
    PaginationOpts,
};
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

    fn group_aliases_by_resolved_owner_account_name<'a>(
        &'a self,
        dataset_aliases: &'a [&'a odf::DatasetAlias],
    ) -> Result<HashMap<&'a odf::AccountName, Vec<&'a odf::DatasetAlias>>, InternalError> {
        let mut res = HashMap::new();

        let mut single_tenant_count = 0;
        let mut multi_tenant_count = 0;

        for dataset_alias in dataset_aliases {
            let account_name = match &dataset_alias.account_name {
                Some(account_name) => {
                    multi_tenant_count += 1;
                    account_name
                }
                None => {
                    single_tenant_count += 1;
                    self.current_account_subject.account_name_or_default()
                }
            };

            let owner_dataset_aliases = res.entry(account_name).or_insert_with(Vec::new);
            owner_dataset_aliases.push(*dataset_alias);
        }

        if single_tenant_count > 0 && multi_tenant_count > 0 {
            return Err(format!(
                "Simultaneous presence of single-tenant and multi-tenant account names: {}",
                itertools::join(dataset_aliases, ",")
            )
            .int_err());
        }

        Ok(res)
    }

    async fn resolve_account_ids_by_maybe_names(
        &self,
        maybe_account_names: &[Option<&odf::AccountName>],
    ) -> Result<ResolveAccountIdsByMaybeNamesResponse, InternalError> {
        let mut single_tenant_count = 0;
        let mut multi_tenant_count = 0;

        let account_names = maybe_account_names
            .iter()
            .map(|maybe_account_name| match maybe_account_name {
                Some(account_name) => {
                    multi_tenant_count += 1;
                    account_name
                }
                None => {
                    single_tenant_count += 1;
                    self.current_account_subject.account_name_or_default()
                }
            })
            .collect::<Vec<_>>();

        if single_tenant_count > 0 && multi_tenant_count > 0 {
            return Err(format!(
                "Simultaneous presence of single-tenant and multi-tenant account names: {}",
                itertools::join(
                    maybe_account_names
                        .iter()
                        .map(|maybe_account_name| { format!("{maybe_account_name:?}") }),
                    ","
                )
            )
            .int_err());
        }

        // 1. Read all available data from the cache
        let cached_name_id_pairs = {
            let readable_cache = self.cache.read().unwrap();
            account_names
                .iter()
                .fold(HashMap::new(), |mut acc, account_name| {
                    if let Some(id) = readable_cache.accounts.names2ids.get(account_name) {
                        acc.insert(*account_name, id.clone());
                    }
                    acc
                })
        };

        let mut resolved_account_ids = Vec::with_capacity(cached_name_id_pairs.len());
        for (name, resolved_id) in &cached_name_id_pairs {
            resolved_account_ids.push(((*name).clone(), resolved_id.clone()));
        }

        let mut unresolved_account_names = Vec::new();

        // 2. Request unresolved account names from the service
        if account_names.len() != resolved_account_ids.len() {
            let mut account_names_to_request =
                Vec::with_capacity(account_names.len() - resolved_account_ids.len());
            for account_name in account_names {
                if !cached_name_id_pairs.contains_key(account_name) {
                    account_names_to_request.push(account_name);
                }
            }

            let lookup = self
                .account_svc
                .get_accounts_by_names(&account_names_to_request)
                .await?;
            let resolved_account_ids_from_service = lookup
                .found
                .into_iter()
                .map(|a| (a.account_name, a.id))
                .collect::<Vec<_>>();

            // Cache the resolved results
            {
                let mut writable_cache = self.cache.write().unwrap();
                for (account_name, account_id) in &resolved_account_ids_from_service {
                    writable_cache
                        .accounts
                        .names2ids
                        .insert(account_name.clone(), account_id.clone());
                }
            }

            // Add service-{un,}resolved ones to response
            resolved_account_ids.extend(resolved_account_ids_from_service);

            unresolved_account_names = lookup
                .not_found
                .into_iter()
                .map(|(id, e)| (id, e.into()))
                .collect::<Vec<_>>();
        }

        Ok(ResolveAccountIdsByMaybeNamesResponse {
            resolved_account_ids,
            unresolved_account_names,
        })
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

    fn make_dataset_handle(&self, entry: DatasetEntry) -> odf::DatasetHandle {
        odf::DatasetHandle::new(
            entry.id,
            self.tenancy_config.make_alias(entry.owner_name, entry.name),
            entry.kind,
        )
    }

    async fn resolve_dataset_handles_by_dataset_ids<'a>(
        &'a self,
        dataset_ids: &[Cow<'a, odf::DatasetID>],
    ) -> Result<odf::dataset::ResolveDatasetHandlesByRefsResponse, InternalError> {
        let entries_resolution_by_ids = self.get_multiple_entries(dataset_ids).await.int_err()?;

        let mut resolved_handles =
            Vec::with_capacity(entries_resolution_by_ids.resolved_entries.len());
        let mut unresolved_refs =
            Vec::with_capacity(entries_resolution_by_ids.unresolved_entries.len());

        for entry in entries_resolution_by_ids.resolved_entries {
            resolved_handles.push((entry.id.as_local_ref(), self.make_dataset_handle(entry)));
        }

        for unresolved_dataset_id in entries_resolution_by_ids.unresolved_entries {
            let dataset_ref = unresolved_dataset_id.as_local_ref();
            unresolved_refs.push((
                dataset_ref.clone(),
                odf::DatasetNotFoundError::new(dataset_ref).into(),
            ));
        }

        Ok(odf::dataset::ResolveDatasetHandlesByRefsResponse {
            resolved_handles,
            unresolved_refs,
        })
    }

    async fn resolve_dataset_handles_by_dataset_aliases(
        &self,
        dataset_aliases: &[&odf::DatasetAlias],
    ) -> Result<odf::dataset::ResolveDatasetHandlesByRefsResponse, InternalError> {
        let mut resolved_handles = Vec::with_capacity(dataset_aliases.len());
        let mut unresolved_refs = Vec::with_capacity(dataset_aliases.len());

        // First, we attempt to resolve accounts
        let owner_name_dataset_aliases_mapping =
            self.group_aliases_by_resolved_owner_account_name(dataset_aliases)?;
        let maybe_owner_names = dataset_aliases
            .iter()
            .map(|alias| alias.account_name.as_ref())
            .collect::<Vec<_>>();

        let owner_ids_resolution_by_names = self
            .resolve_account_ids_by_maybe_names(&maybe_owner_names)
            .await?;

        for (unresolved_owner_name, _) in owner_ids_resolution_by_names.unresolved_account_names {
            // If a dataset owner account not found, then no datasets
            let dataset_aliases_without_owner = owner_name_dataset_aliases_mapping
                .get(&unresolved_owner_name)
                .unwrap_or_else(|| {
                    unreachable!(
                        "{unresolved_owner_name} not found in \
                         {owner_name_dataset_aliases_mapping:?}"
                    );
                });
            for dataset_alias_without_owner in dataset_aliases_without_owner {
                let dataset_ref = dataset_alias_without_owner.as_local_ref();
                let e = odf::DatasetNotFoundError::new(dataset_ref.clone());
                unresolved_refs.push((dataset_ref, e.into()));
            }
        }

        // After that, try to resolve dataset entries
        let owner_id_name_mapping = owner_ids_resolution_by_names
            .resolved_account_ids
            .into_iter()
            .map(|(name, id)| (id, name))
            .collect::<HashMap<_, _>>();
        let mut owner_id_dataset_name_pairs =
            Vec::with_capacity(dataset_aliases.len() - unresolved_refs.len());

        for (owner_id, owner_name) in &owner_id_name_mapping {
            let dataset_aliases = owner_name_dataset_aliases_mapping
                .get(&owner_name)
                .unwrap_or_else(|| {
                    unreachable!(
                        "{owner_name} not found in {owner_name_dataset_aliases_mapping:?}"
                    );
                });
            for dataset_alias in dataset_aliases {
                owner_id_dataset_name_pairs
                    .push((owner_id.clone(), dataset_alias.dataset_name.clone()));
            }
        }

        let owner_id_dataset_name_pairs_refs =
            owner_id_dataset_name_pairs.iter().collect::<Vec<_>>();

        let entries_lookup = self
            .get_dataset_entries_by_owner_and_name(&owner_id_dataset_name_pairs_refs)
            .await?;

        for ((owner_id, dataset_name), _e) in entries_lookup.not_found {
            let owner_name = owner_id_name_mapping.get(&owner_id).unwrap_or_else(|| {
                unreachable!("{owner_id} not found in {owner_id_dataset_name_pairs:?}");
            });
            let alias = self
                .tenancy_config
                .make_alias(owner_name.clone(), dataset_name);
            let dataset_ref = alias.as_local_ref();
            let e = odf::DatasetNotFoundError::new(dataset_ref.clone());
            unresolved_refs.push((dataset_ref, e.into()));
        }

        for entry in entries_lookup.found {
            let dataset_handle = self.make_dataset_handle(entry);
            resolved_handles.push((dataset_handle.alias.as_local_ref(), dataset_handle));
        }

        Ok(odf::dataset::ResolveDatasetHandlesByRefsResponse {
            resolved_handles,
            unresolved_refs,
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
        dataset_ids: &[Cow<odf::DatasetID>],
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
                Ok(entry) => Ok(self.make_dataset_handle(entry)),
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
    fn all_dataset_handles_by_owner_name(
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

    fn all_dataset_handles_by_owner_id(
        &self,
        owner_id: &odf::AccountID,
    ) -> odf::dataset::DatasetHandleStream<'_> {
        let owner_id = owner_id.clone();

        EntityPageStreamer::default().into_stream(
            move || async { Ok(owner_id) },
            move |owner_id, pagination| async move {
                self.list_all_dataset_handles_by_owner_id(&owner_id, pagination)
                    .await
            },
        )
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?dataset_ids))]
    async fn resolve_multiple_dataset_handles_by_ids(
        &self,
        dataset_ids: &[Cow<odf::DatasetID>],
    ) -> Result<DatasetHandlesResolution, GetMultipleDatasetsError> {
        let entries_resolution = self.get_multiple_entries(dataset_ids).await.int_err()?;

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
                return Err(CreateDatasetEntryError::Internal(e));
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

    async fn update_owner_entries_after_rename(
        &self,
        owner_account_id: &odf::AccountID,
        old_owner_account_name: &odf::AccountName,
        new_owner_account_name: &odf::AccountName,
    ) -> Result<(), InternalError> {
        self.dataset_entry_repo
            .update_owner_entries_after_rename(owner_account_id, new_owner_account_name)
            .await?;

        // Update owner name in cache, if cached
        {
            let mut writable_cache = self.cache.write().unwrap();

            // Accounts
            if let Some(account_id) = writable_cache
                .accounts
                .names2ids
                .remove(old_owner_account_name)
            {
                writable_cache
                    .accounts
                    .names2ids
                    .insert(new_owner_account_name.clone(), account_id);
            }

            // Entries
            for entry in writable_cache.datasets.entries_by_id.values_mut() {
                if entry.owner_id == *owner_account_id {
                    entry.owner_name = new_owner_account_name.clone();
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ResolveAccountIdsByMaybeNamesResponse {
    resolved_account_ids: Vec<(odf::AccountName, odf::AccountID)>,
    unresolved_account_names: Vec<(odf::AccountName, ResolveAccountIdByNameError)>,
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
