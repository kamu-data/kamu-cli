// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use database_common::{EntityPageListing, EntityPageStreamer, PaginationOpts};
use dill::{component, interface, meta, Catalog};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{AccountRepository, CurrentAccountSubject};
use kamu_core::{
    DatasetHandleStream,
    DatasetHandlesResolution,
    DatasetLifecycleMessage,
    DatasetLifecycleMessageCreated,
    DatasetLifecycleMessageDeleted,
    DatasetLifecycleMessageRenamed,
    DatasetNotFoundError,
    DatasetRegistry,
    DatasetRepository,
    GetDatasetError,
    GetMultipleDatasetsError,
    ResolvedDataset,
    TenancyConfig,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use kamu_datasets::*;
use messaging_outbox::{
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
};
use opendatafabric as odf;
use time_source::SystemTimeSource;

use crate::MESSAGE_CONSUMER_KAMU_DATASET_ENTRY_SERVICE;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEntryServiceImpl {
    time_source: Arc<dyn SystemTimeSource>,
    dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
    dataset_repo: Arc<dyn DatasetRepository>,
    account_repo: Arc<dyn AccountRepository>,
    current_account_subject: Arc<CurrentAccountSubject>,
    tenancy_config: Arc<TenancyConfig>,
    accounts_cache: Arc<Mutex<AccountsCache>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct AccountsCache {
    id2names: HashMap<odf::AccountID, odf::AccountName>,
    names2ids: HashMap<odf::AccountName, odf::AccountID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetEntryService)]
#[interface(dyn DatasetRegistry)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASET_ENTRY_SERVICE,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Immediate,
})]
impl DatasetEntryServiceImpl {
    pub fn new(
        time_source: Arc<dyn SystemTimeSource>,
        dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
        dataset_repo: Arc<dyn DatasetRepository>,
        account_repo: Arc<dyn AccountRepository>,
        current_account_subject: Arc<CurrentAccountSubject>,
        tenancy_config: Arc<TenancyConfig>,
    ) -> Self {
        Self {
            time_source,
            dataset_entry_repo,
            dataset_repo,
            account_repo,
            current_account_subject,
            tenancy_config,
            accounts_cache: Default::default(),
        }
    }

    async fn handle_dataset_lifecycle_created_message(
        &self,
        DatasetLifecycleMessageCreated {
            dataset_id,
            owner_account_id,
            dataset_name,
            ..
        }: &DatasetLifecycleMessageCreated,
    ) -> Result<(), InternalError> {
        match self.dataset_entry_repo.get_dataset_entry(dataset_id).await {
            Ok(_) => return Ok(()), // idempotent handling of duplicates
            Err(GetDatasetEntryError::NotFound(_)) => { /* happy case, create record */ }
            Err(GetDatasetEntryError::Internal(e)) => return Err(e),
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
            .int_err()
    }

    async fn handle_dataset_lifecycle_deleted_message(
        &self,
        DatasetLifecycleMessageDeleted { dataset_id, .. }: &DatasetLifecycleMessageDeleted,
    ) -> Result<(), InternalError> {
        match self
            .dataset_entry_repo
            .delete_dataset_entry(dataset_id)
            .await
        {
            Ok(_) | Err(DeleteEntryDatasetError::NotFound(_)) => Ok(()),
            Err(DeleteEntryDatasetError::Internal(e)) => Err(e),
        }
    }

    async fn handle_dataset_lifecycle_renamed_message(
        &self,
        DatasetLifecycleMessageRenamed {
            dataset_id,
            new_dataset_name,
            ..
        }: &DatasetLifecycleMessageRenamed,
    ) -> Result<(), InternalError> {
        self.dataset_entry_repo
            .update_dataset_entry_name(dataset_id, new_dataset_name)
            .await
            .int_err()
    }

    async fn entries_as_handles(
        &self,
        entries: Vec<DatasetEntry>,
    ) -> Result<Vec<odf::DatasetHandle>, ListDatasetEntriesError> {
        // Select which accounts haven't been processed yet
        let first_seen_account_ids = {
            let accounts_cache = self.accounts_cache.lock().unwrap();

            let mut first_seen_account_ids = HashSet::new();
            for entry in &entries {
                if !accounts_cache.id2names.contains_key(&entry.owner_id) {
                    first_seen_account_ids.insert(entry.owner_id.clone());
                }
            }

            first_seen_account_ids
        };

        // Query first seen accounts and fill the table
        if !first_seen_account_ids.is_empty() {
            let account_ids = first_seen_account_ids.into_iter().collect::<Vec<_>>();
            let accounts = self
                .account_repo
                .get_accounts_by_ids(account_ids)
                .await
                .int_err()?;

            let mut accounts_cache = self.accounts_cache.lock().unwrap();
            for account in accounts {
                accounts_cache
                    .id2names
                    .insert(account.id.clone(), account.account_name.clone());
                accounts_cache
                    .names2ids
                    .insert(account.account_name, account.id);
            }
        }

        // Convert the entries to handles
        let mut handles = Vec::new();
        let accounts_cache = self.accounts_cache.lock().unwrap();
        for entry in &entries {
            // By now we should now the account name
            let maybe_owner_name = accounts_cache.id2names.get(&entry.owner_id);
            if let Some(owner_name) = maybe_owner_name {
                // Form DatasetHandle
                handles.push(odf::DatasetHandle::new(
                    entry.id.clone(),
                    self.make_alias(owner_name.clone(), entry.name.clone()),
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
            let accounts_cache = self.accounts_cache.lock().unwrap();
            accounts_cache.id2names.get(account_id).cloned()
        };

        if let Some(name) = maybe_cached_name {
            Ok(name)
        } else {
            let account = self
                .account_repo
                .get_account_by_id(account_id)
                .await
                .int_err()?;

            let mut accounts_cache = self.accounts_cache.lock().unwrap();
            accounts_cache
                .id2names
                .insert(account_id.clone(), account.account_name.clone());
            accounts_cache
                .names2ids
                .insert(account.account_name.clone(), account_id.clone());

            Ok(account.account_name)
        }
    }

    async fn resolve_account_id_by_maybe_name(
        &self,
        maybe_account_name: Option<&odf::AccountName>,
    ) -> Result<odf::AccountID, InternalError> {
        let account_name = maybe_account_name
            .unwrap_or_else(|| self.current_account_subject.account_name_or_default());

        let maybe_cached_id = {
            let accounts_cache = self.accounts_cache.lock().unwrap();
            accounts_cache.names2ids.get(account_name).cloned()
        };

        if let Some(id) = maybe_cached_id {
            Ok(id)
        } else {
            let account = self
                .account_repo
                .get_account_by_name(account_name)
                .await
                .int_err()?;

            let mut accounts_cache = self.accounts_cache.lock().unwrap();
            accounts_cache
                .id2names
                .insert(account.id.clone(), account_name.clone());
            accounts_cache
                .names2ids
                .insert(account_name.clone(), account.id.clone());

            Ok(account.id)
        }
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

    async fn list_all_dataset_handles_by_owner_name(
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

    fn make_alias(
        &self,
        owner_name: odf::AccountName,
        dataset_name: odf::DatasetName,
    ) -> odf::DatasetAlias {
        match *self.tenancy_config {
            TenancyConfig::MultiTenant => odf::DatasetAlias::new(Some(owner_name), dataset_name),
            TenancyConfig::SingleTenant => odf::DatasetAlias::new(None, dataset_name),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryService for DatasetEntryServiceImpl {
    async fn list_all_entries(
        &self,
        pagination: PaginationOpts,
    ) -> Result<EntityPageListing<DatasetEntry>, ListDatasetEntriesError> {
        use futures::TryStreamExt;

        let total_count = self.dataset_entry_repo.dataset_entries_count().await?;
        let entries = self
            .dataset_entry_repo
            .get_dataset_entries(pagination)
            .try_collect()
            .await?;

        Ok(EntityPageListing {
            list: entries,
            total_count,
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
            .try_collect()
            .await?;

        Ok(EntityPageListing {
            list: entries,
            total_count,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetRegistry for DatasetEntryServiceImpl {
    #[tracing::instrument(level = "debug", skip_all)]
    fn all_dataset_handles(&self) -> DatasetHandleStream {
        EntityPageStreamer::default().into_stream(
            || async { Ok(()) },
            |_, pagination| self.list_all_dataset_handles(pagination),
        )
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%owner_name))]
    fn all_dataset_handles_by_owner(&self, owner_name: &odf::AccountName) -> DatasetHandleStream {
        let owner_name = owner_name.clone();

        EntityPageStreamer::default().into_stream(
            move || async move {
                let owner_id = self
                    .resolve_account_id_by_maybe_name(Some(&owner_name))
                    .await?;
                Ok(Arc::new(owner_id))
            },
            move |owner_id, pagination| async move {
                self.list_all_dataset_handles_by_owner_name(&owner_id, pagination)
                    .await
            },
        )
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_ref))]
    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<odf::DatasetHandle, GetDatasetError> {
        match dataset_ref {
            odf::DatasetRef::Handle(h) => Ok(h.clone()),
            odf::DatasetRef::Alias(alias) => {
                let owner_id = self
                    .resolve_account_id_by_maybe_name(alias.account_name.as_ref())
                    .await?;
                match self
                    .dataset_entry_repo
                    .get_dataset_entry_by_owner_and_name(&owner_id, &alias.dataset_name)
                    .await
                {
                    Ok(entry) => Ok(odf::DatasetHandle::new(entry.id.clone(), alias.clone())),
                    Err(GetDatasetEntryByNameError::NotFound(_)) => {
                        Err(GetDatasetError::NotFound(DatasetNotFoundError {
                            dataset_ref: dataset_ref.clone(),
                        }))
                    }
                    Err(GetDatasetEntryByNameError::Internal(e)) => {
                        Err(GetDatasetError::Internal(e))
                    }
                }
            }
            odf::DatasetRef::ID(id) => match self.dataset_entry_repo.get_dataset_entry(id).await {
                Ok(entry) => {
                    let owner_name = self.resolve_account_name_by_id(&entry.owner_id).await?;
                    Ok(odf::DatasetHandle::new(
                        entry.id.clone(),
                        self.make_alias(owner_name, entry.name.clone()),
                    ))
                }
                Err(GetDatasetEntryError::NotFound(_)) => {
                    Err(GetDatasetError::NotFound(DatasetNotFoundError {
                        dataset_ref: dataset_ref.clone(),
                    }))
                }
                Err(GetDatasetEntryError::Internal(e)) => Err(GetDatasetError::Internal(e)),
            },
        }
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
                    GetDatasetError::NotFound(DatasetNotFoundError {
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
        let dataset = self.dataset_repo.get_dataset_by_handle(dataset_handle);
        ResolvedDataset::new(dataset, dataset_handle.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DatasetEntryServiceImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for DatasetEntryServiceImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetEntryService[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        match message {
            DatasetLifecycleMessage::Created(message) => {
                self.handle_dataset_lifecycle_created_message(message).await
            }

            DatasetLifecycleMessage::Deleted(message) => {
                self.handle_dataset_lifecycle_deleted_message(message).await
            }

            DatasetLifecycleMessage::Renamed(message) => {
                self.handle_dataset_lifecycle_renamed_message(message).await
            }

            DatasetLifecycleMessage::DependenciesUpdated(_) => {
                // No action required
                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
