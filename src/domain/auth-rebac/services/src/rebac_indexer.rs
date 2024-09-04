// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface, meta};
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{AccountRepository, JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR};
use kamu_auth_rebac::{AccountPropertyName, DatasetPropertyName, RebacRepository, RebacService};
use kamu_datasets::DatasetEntryRepository;
use kamu_datasets_services::JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER;

use crate::JOB_KAMU_REBAC_INDEXER;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RebacIndexer {
    rebac_repo: Arc<dyn RebacRepository>,
    rebac_service: Arc<dyn RebacService>,
    dataset_entry_repository: Arc<dyn DatasetEntryRepository>,
    account_repository: Arc<dyn AccountRepository>,
}

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_REBAC_INDEXER,
    depends_on: &[
        JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
        JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER
    ],
    requires_transaction: true,
})]
impl RebacIndexer {
    pub fn new(
        rebac_repo: Arc<dyn RebacRepository>,
        rebac_service: Arc<dyn RebacService>,
        dataset_entry_repository: Arc<dyn DatasetEntryRepository>,
        account_repository: Arc<dyn AccountRepository>,
    ) -> Self {
        Self {
            rebac_repo,
            rebac_service,
            dataset_entry_repository,
            account_repository,
        }
    }

    async fn has_entities_indexed(&self) -> Result<bool, InternalError> {
        let properties_count = self.rebac_repo.properties_count().await.int_err()?;

        Ok(properties_count > 0)
    }

    async fn index_entities(&self) -> Result<(), InternalError> {
        self.index_dataset_entries().await?;
        self.index_accounts().await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn index_dataset_entries(&self) -> Result<(), InternalError> {
        let dataset_entries = self
            .dataset_entry_repository
            .get_dataset_entries()
            .await
            .int_err()?;

        for dataset_entry in dataset_entries {
            for (name, value) in [
                DatasetPropertyName::allows_public_read(false),
                DatasetPropertyName::allows_anonymous_read(false),
            ] {
                self.rebac_service
                    .set_dataset_property(&dataset_entry.id, name, &value)
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn index_accounts(&self) -> Result<(), InternalError> {
        let accounts = self.account_repository.get_accounts().await.int_err()?;

        for account in accounts {
            for (name, value) in [AccountPropertyName::is_admin(account.is_admin)] {
                self.rebac_service
                    .set_account_property(&account.id, name, &value)
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for RebacIndexer {
    #[tracing::instrument(level = "debug", skip_all, name = "RebacIndexer::run_initialization")]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        if self.has_entities_indexed().await? {
            tracing::debug!("Skip initialization: entities already have indexed");
            return Ok(());
        }

        self.index_entities().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
