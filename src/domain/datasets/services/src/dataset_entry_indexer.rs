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
    DEFAULT_ACCOUNT_ID,
    JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
};
use kamu_core::DatasetRepository;
use kamu_datasets::{DatasetEntry, DatasetEntryRepository};
use messaging_outbox::JOB_MESSAGING_OUTBOX_STARTUP;
use opendatafabric as odf;
use opendatafabric::DatasetHandle;
use time_source::SystemTimeSource;

use crate::JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEntryIndexer {
    dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
    time_source: Arc<dyn SystemTimeSource>,
    dataset_repo: Arc<dyn DatasetRepository>,
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
        dataset_repo: Arc<dyn DatasetRepository>,
        account_repository: Arc<dyn AccountRepository>,
    ) -> Self {
        Self {
            dataset_entry_repo,
            time_source,
            dataset_repo,
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

    async fn index_datasets(&self) -> Result<(), InternalError> {
        use futures::TryStreamExt;

        let dataset_handles: Vec<_> = self.dataset_repo.get_all_datasets().try_collect().await?;

        self.concurrent_dataset_handles_processing(dataset_handles)
            .await?;

        Ok(())
    }

    async fn concurrent_dataset_handles_processing(
        &self,
        dataset_handles: Vec<DatasetHandle>,
    ) -> Result<(), InternalError> {
        let mut join_set = tokio::task::JoinSet::new();
        let now = self.time_source.now();
        let account_name_id_mapping = self.build_account_name_id_mapping(&dataset_handles).await?;

        for dataset_handle in dataset_handles {
            let task_owner_account_id =
                account_name_id_mapping[&dataset_handle.alias.account_name].clone();
            let task_now = now;
            let task_dataset_entry_repo = self.dataset_entry_repo.clone();

            join_set.spawn(async move {
                let dataset_entry = DatasetEntry::new(
                    dataset_handle.id,
                    task_owner_account_id,
                    dataset_handle.alias.dataset_name,
                    task_now,
                );

                task_dataset_entry_repo
                    .save_dataset_entry(&dataset_entry)
                    .await
                    .int_err()
            });
        }

        while let Some(join_result) = join_set.join_next().await {
            let task_res = join_result.int_err()?;

            task_res?;
        }

        Ok(())
    }

    async fn build_account_name_id_mapping(
        &self,
        dataset_handles: &[DatasetHandle],
    ) -> Result<HashMap<Option<odf::AccountName>, odf::AccountID>, InternalError> {
        let mut map = HashMap::new();

        for dataset_handle in dataset_handles {
            let maybe_owner_name = &dataset_handle.alias.account_name;

            if map.contains_key(maybe_owner_name) {
                continue;
            }

            let owner_account_id = self.get_dataset_owner_id(maybe_owner_name).await?;

            map.insert(maybe_owner_name.clone(), owner_account_id);
        }

        Ok(map)
    }

    async fn get_dataset_owner_id(
        &self,
        maybe_owner_name: &Option<odf::AccountName>,
    ) -> Result<odf::AccountID, InternalError> {
        match &maybe_owner_name {
            Some(account_name) => {
                let account = self
                    .account_repository
                    .get_account_by_name(account_name)
                    .await
                    .int_err()?;

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
