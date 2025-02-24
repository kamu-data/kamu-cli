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

use dill::{component, interface, meta};
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{
    AccountRepository,
    GetAccountByNameError,
    DEFAULT_ACCOUNT_ID,
    JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
};
use kamu_datasets::{DatasetEntry, DatasetEntryRepository};
use messaging_outbox::JOB_MESSAGING_OUTBOX_STARTUP;
use time_source::SystemTimeSource;

use crate::JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEntryIndexer {
    dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
    time_source: Arc<dyn SystemTimeSource>,
    dataset_storage_unit: Arc<dyn odf::DatasetStorageUnit>, /* Note: potentially we will have
                                                             * multiple */
    account_repository: Arc<dyn AccountRepository>,
}

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER,
    depends_on: &[
        JOB_MESSAGING_OUTBOX_STARTUP,
        JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
    ],
    requires_transaction: true,
})]
impl DatasetEntryIndexer {
    pub fn new(
        dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
        time_source: Arc<dyn SystemTimeSource>,
        dataset_storage_unit: Arc<dyn odf::DatasetStorageUnit>,
        account_repository: Arc<dyn AccountRepository>,
    ) -> Self {
        Self {
            dataset_entry_repo,
            time_source,
            dataset_storage_unit,
            account_repository,
        }
    }

    async fn has_datasets_indexed(&self) -> Result<bool, InternalError> {
        let stored_dataset_entries_count = self
            .dataset_entry_repo
            .dataset_entries_count()
            .await
            .int_err()?;

        Ok(stored_dataset_entries_count > 0)
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetEntryIndexer::index_datasets"
    )]
    async fn index_datasets(&self) -> Result<(), InternalError> {
        use futures::TryStreamExt;

        let dataset_ids: Vec<_> = self
            .dataset_storage_unit
            .stored_dataset_ids()
            .try_collect()
            .await?;

        let mut dataset_handles = Vec::with_capacity(dataset_ids.len());
        for dataset_id in dataset_ids {
            let dataset = self
                .dataset_storage_unit
                .get_stored_dataset_by_id(&dataset_id)
                .await
                .int_err()?;
            let dataset_alias = odf::dataset::read_dataset_alias(dataset.as_ref())
                .await
                .int_err()?;
            dataset_handles.push(odf::DatasetHandle::new(dataset_id, dataset_alias));
        }

        let account_name_id_mapping = self.build_account_name_id_mapping(&dataset_handles).await?;

        for dataset_handle in dataset_handles {
            let Some(owner_account_id) = account_name_id_mapping
                .get(&dataset_handle.alias.account_name)
                .cloned()
            else {
                tracing::warn!(dataset_handle=%dataset_handle, "Skipped indexing dataset due to unresolved owner");
                continue;
            };

            let dataset_entry = DatasetEntry::new(
                dataset_handle.id,
                owner_account_id,
                dataset_handle.alias.dataset_name,
                self.time_source.now(),
            );

            use tracing::Instrument;
            self.dataset_entry_repo
                .save_dataset_entry(&dataset_entry)
                .instrument(tracing::debug_span!(
                    "Saving indexed dataset entry",
                    dataset_entry = ?dataset_entry,
                    owner_account_name = ?dataset_handle.alias.account_name,
                ))
                .await
                .int_err()?;
        }

        Ok(())
    }

    async fn build_account_name_id_mapping(
        &self,
        dataset_handles: &[odf::DatasetHandle],
    ) -> Result<HashMap<Option<odf::AccountName>, odf::AccountID>, InternalError> {
        let mut map = HashMap::new();

        for dataset_handle in dataset_handles {
            let maybe_owner_name = &dataset_handle.alias.account_name;

            if map.contains_key(maybe_owner_name) {
                continue;
            }

            match self.get_dataset_owner_id(maybe_owner_name.as_ref()).await {
                Ok(owner_account_id) => {
                    map.insert(maybe_owner_name.clone(), owner_account_id);
                }
                Err(e) => {
                    // Log error, but don't crash
                    tracing::error!(error=?e, account_name = ?maybe_owner_name, "Account unresolved by dataset indexing job");
                }
            }
        }

        Ok(map)
    }

    async fn get_dataset_owner_id(
        &self,
        maybe_owner_name: Option<&odf::AccountName>,
    ) -> Result<odf::AccountID, GetAccountByNameError> {
        match &maybe_owner_name {
            Some(account_name) => {
                let account = self
                    .account_repository
                    .get_account_by_name(account_name)
                    .await?;

                tracing::debug!(account_id=%account.id, account_name=%account_name, "Account resolved by dataset indexing job");
                Ok(account.id)
            }
            None => Ok(DEFAULT_ACCOUNT_ID.clone()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for DatasetEntryIndexer {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetEntryIndexer::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        if self.has_datasets_indexed().await? {
            tracing::debug!("Skip initialization: datasets already have indexed");

            return Ok(());
        }

        self.index_datasets().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
