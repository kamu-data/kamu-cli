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

use database_common::PaginationOpts;
use dill::{component, interface, meta, Catalog};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{AccountRepository, DEFAULT_ACCOUNT_NAME};
use kamu_core::{
    Dataset,
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
use opendatafabric::{
    AccountID,
    AccountName,
    DatasetAlias,
    DatasetHandle,
    DatasetID,
    DatasetName,
    DatasetRef,
};
use time_source::SystemTimeSource;

use crate::MESSAGE_CONSUMER_KAMU_DATASET_ENTRY_SERVICE;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEntryServiceImpl {
    time_source: Arc<dyn SystemTimeSource>,
    dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
    dataset_repo: Arc<dyn DatasetRepository>,
    account_repo: Arc<dyn AccountRepository>,
    tenancy_config: Arc<TenancyConfig>,
    accounts_cache: Arc<Mutex<AccountsCache>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct AccountsCache {
    id2names: HashMap<AccountID, AccountName>,
    names2ids: HashMap<AccountName, AccountID>,
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
        tenancy_config: Arc<TenancyConfig>,
    ) -> Self {
        Self {
            time_source,
            dataset_entry_repo,
            dataset_repo,
            account_repo,
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
            Ok(_) => return Ok(()), // idemponent handling of duplicates
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
    ) -> Result<Vec<DatasetHandle>, ListDatasetEntriesError> {
        // Select which accounts haven't been processed yet
        let first_seen_account_ids = {
            let accounts_cache = self.accounts_cache.lock().unwrap();

            let mut first_seen_account_ids: HashSet<AccountID> = HashSet::new();
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
                handles.push(DatasetHandle::new(
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
        account_id: &AccountID,
    ) -> Result<AccountName, InternalError> {
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
        maybe_account_name: Option<&AccountName>,
    ) -> Result<AccountID, InternalError> {
        let account_name = maybe_account_name.unwrap_or(&DEFAULT_ACCOUNT_NAME);

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

    fn stream_datasets<'a, Args, HInitArgs, HInitArgsFut, HListing, HListingFut>(
        &'a self,
        get_args_callback: HInitArgs,
        next_entries_callback: HListing,
    ) -> DatasetHandleStream<'a>
    where
        Args: Clone + Send + 'a,
        HInitArgs: FnOnce() -> HInitArgsFut + Send + 'a,
        HInitArgsFut: std::future::Future<Output = Result<Args, InternalError>> + Send + 'a,
        HListing: Fn(Args, PaginationOpts) -> HListingFut + Send + 'a,
        HListingFut: std::future::Future<Output = Result<DatasetEntryListing, ListDatasetEntriesError>>
            + Send
            + 'a,
    {
        Box::pin(async_stream::try_stream! {
            // Init arguments
            let args = get_args_callback().await?;

            // Tracking pagination progress
            let mut offset = 0;
            let limit = 100;

            loop {
                // Load a page of dataset entries
                let entries_page = next_entries_callback(args.clone(), PaginationOpts { limit, offset })
                    .await
                    .int_err()?;

                // Actually read entires
                let loaded_entries_count = entries_page.list.len();

                // Convert entries to handles
                let handles = self.entries_as_handles(entries_page.list).await.int_err()?;

                // Stream the entries
                for hdl in handles {
                    yield hdl;
                }

                // Next page
                offset += loaded_entries_count;
                if offset >= entries_page.total_count {
                    break;
                }
            }
        })
    }

    fn make_alias(&self, owner_name: AccountName, dataset_name: DatasetName) -> DatasetAlias {
        match *self.tenancy_config {
            TenancyConfig::MultiTenant => DatasetAlias::new(Some(owner_name), dataset_name),
            TenancyConfig::SingleTenant => DatasetAlias::new(None, dataset_name),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryService for DatasetEntryServiceImpl {
    async fn list_all_entries(
        &self,
        pagination: PaginationOpts,
    ) -> Result<DatasetEntryListing, ListDatasetEntriesError> {
        use futures::TryStreamExt;

        let total_count = self.dataset_entry_repo.dataset_entries_count().await?;
        let entries = self
            .dataset_entry_repo
            .get_dataset_entries(pagination)
            .try_collect()
            .await?;

        Ok(DatasetEntryListing {
            list: entries,
            total_count,
        })
    }

    async fn list_entries_owned_by(
        &self,
        owner_id: AccountID,
        pagination: PaginationOpts,
    ) -> Result<DatasetEntryListing, ListDatasetEntriesError> {
        use futures::TryStreamExt;

        let total_count = self
            .dataset_entry_repo
            .dataset_entries_count_by_owner_id(&owner_id)
            .await?;
        let entries = self
            .dataset_entry_repo
            .get_dataset_entries_by_owner_id(&owner_id, pagination)
            .try_collect()
            .await?;

        Ok(DatasetEntryListing {
            list: entries,
            total_count,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetRegistry for DatasetEntryServiceImpl {
    #[tracing::instrument(level = "debug", skip_all)]
    fn all_dataset_handles<'a>(&'a self) -> DatasetHandleStream<'a> {
        #[derive(Clone)]
        struct NoArgs {}

        self.stream_datasets(
            || async { Ok(NoArgs {}) },
            |_, pagination| self.list_all_entries(pagination),
        )
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%owner_name))]
    fn all_dataset_handles_by_owner(&self, owner_name: &AccountName) -> DatasetHandleStream<'_> {
        #[derive(Clone)]
        struct OwnerArgs {
            owner_id: AccountID,
        }

        let owner_name = owner_name.clone();

        self.stream_datasets(
            move || async move {
                let owner_id = self
                    .resolve_account_id_by_maybe_name(Some(&owner_name))
                    .await?;
                Ok(OwnerArgs { owner_id })
            },
            |args, pagination| self.list_entries_owned_by(args.owner_id, pagination),
        )
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_ref))]
    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetHandle, GetDatasetError> {
        match dataset_ref {
            DatasetRef::Handle(h) => Ok(h.clone()),
            DatasetRef::Alias(alias) => {
                let owner_id = self
                    .resolve_account_id_by_maybe_name(alias.account_name.as_ref())
                    .await?;
                match self
                    .dataset_entry_repo
                    .get_dataset_entry_by_owner_and_name(&owner_id, &alias.dataset_name)
                    .await
                {
                    Ok(entry) => Ok(DatasetHandle::new(entry.id.clone(), alias.clone())),
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
            DatasetRef::ID(id) => match self.dataset_entry_repo.get_dataset_entry(id).await {
                Ok(entry) => {
                    let owner_name = self.resolve_account_name_by_id(&entry.owner_id).await?;
                    Ok(DatasetHandle::new(
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
        dataset_ids: Vec<DatasetID>,
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
    fn get_dataset_by_handle(&self, dataset_handle: &DatasetHandle) -> Arc<dyn Dataset> {
        self.dataset_repo.get_dataset_by_handle(dataset_handle)
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
