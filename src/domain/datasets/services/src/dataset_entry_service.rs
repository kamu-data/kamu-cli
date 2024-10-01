// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface, meta, Catalog};
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{
    AccountRepository,
    DEFAULT_ACCOUNT_ID,
    JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
};
use kamu_core::{
    DatasetLifecycleMessage,
    DatasetLifecycleMessageCreated,
    DatasetLifecycleMessageDeleted,
    DatasetLifecycleMessageRenamed,
    DatasetRepository,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use kamu_datasets::{DatasetEntry, DatasetEntryRepository};
use messaging_outbox::{
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageConsumptionDurability,
};
use opendatafabric as odf;
use opendatafabric::DatasetHandle;
use time_source::SystemTimeSource;

use crate::{JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER, MESSAGE_CONSUMER_KAMU_DATASET_ENTRY_SERVICE};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct DatasetEntryServiceInitializationSkipper {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASET_ENTRY_SERVICE,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
    ],
    durability: MessageConsumptionDurability::Durable,
})]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER,
    depends_on: &[JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR],
    requires_transaction: true,
})]
pub struct DatasetEntryService {
    dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
    time_source: Arc<dyn SystemTimeSource>,
    dataset_repo: Arc<dyn DatasetRepository>,
    account_repository: Arc<dyn AccountRepository>,
    maybe_initialization_skipper: Option<Arc<DatasetEntryServiceInitializationSkipper>>,
}

impl DatasetEntryService {
    pub fn new(
        dataset_entry_repo: Arc<dyn DatasetEntryRepository>,
        time_source: Arc<dyn SystemTimeSource>,
        dataset_repo: Arc<dyn DatasetRepository>,
        account_repository: Arc<dyn AccountRepository>,
        maybe_initialization_skipper: Option<Arc<DatasetEntryServiceInitializationSkipper>>,
    ) -> Self {
        Self {
            dataset_entry_repo,
            time_source,
            dataset_repo,
            account_repository,
            maybe_initialization_skipper,
        }
    }

    async fn handle_dataset_lifecycle_created_message(
        &self,
        message @ DatasetLifecycleMessageCreated {
            dataset_id,
            owner_account_id,
            ..
        }: &DatasetLifecycleMessageCreated,
    ) -> Result<(), InternalError> {
        let entry = DatasetEntry::new(
            dataset_id.clone(),
            owner_account_id.clone(),
            message.dataset_name().into_owned(),
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
        self.dataset_entry_repo
            .delete_dataset_entry(dataset_id)
            .await
            .int_err()
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

        for dataset_handle in dataset_handles {
            let task_account_repository = self.account_repository.clone();
            let task_dataset_entry_repo = self.dataset_entry_repo.clone();
            let task_now = now;

            join_set.spawn(async move {
                let owner_account_id = get_dataset_owner_id(
                    &task_account_repository,
                    &dataset_handle.alias.account_name,
                )
                .await?;
                let dataset_entry = DatasetEntry::new(
                    dataset_handle.id,
                    owner_account_id.clone(),
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DatasetEntryService {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for DatasetEntryService {
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

#[async_trait::async_trait]
impl InitOnStartup for DatasetEntryService {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetEntryService::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        if self.maybe_initialization_skipper.is_some() {
            tracing::debug!("Skip initialization: since a skipper is present");

            return Ok(());
        }

        if self.has_datasets_indexed().await? {
            tracing::debug!("Skip initialization: datasets already have indexed");

            return Ok(());
        }

        self.index_datasets().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn get_dataset_owner_id(
    account_repository: &Arc<dyn AccountRepository>,
    maybe_owner_name: &Option<odf::AccountName>,
) -> Result<odf::AccountID, InternalError> {
    match &maybe_owner_name {
        Some(account_name) => {
            let account = account_repository
                .get_account_by_name(account_name)
                .await
                .int_err()?;

            Ok(account.id)
        }
        None => Ok(DEFAULT_ACCOUNT_ID.clone()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
