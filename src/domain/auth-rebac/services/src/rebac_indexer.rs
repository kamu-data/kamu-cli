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
    ExpensiveAccountRepository,
    ExpensiveAccountRepositoryExt,
    PredefinedAccountsConfig,
    JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
};
use kamu_auth_rebac::{AccountPropertyName, DatasetPropertyName, RebacService};
use kamu_datasets::DatasetEntryService;
use kamu_datasets_services::JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER;

use crate::JOB_KAMU_REBAC_INDEXER;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type PredefinedAccountIdDatasetVisibilityMapping = HashMap<odf::AccountID, odf::DatasetVisibility>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RebacIndexer {
    rebac_service: Arc<dyn RebacService>,
    dataset_entry_service: Arc<dyn DatasetEntryService>,
    expensive_account_repo: Arc<dyn ExpensiveAccountRepository>,
    predefined_accounts_config: Arc<PredefinedAccountsConfig>,
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
        rebac_service: Arc<dyn RebacService>,
        dataset_entry_service: Arc<dyn DatasetEntryService>,
        expensive_account_repo: Arc<dyn ExpensiveAccountRepository>,
        predefined_accounts_config: Arc<PredefinedAccountsConfig>,
    ) -> Self {
        Self {
            rebac_service,
            dataset_entry_service,
            expensive_account_repo,
            predefined_accounts_config,
        }
    }

    async fn has_entities_indexed(&self) -> Result<bool, InternalError> {
        let properties_count = self.rebac_service.properties_count().await.int_err()?;

        Ok(properties_count > 0)
    }

    async fn index_entities(&self) -> Result<(), InternalError> {
        let visibility_map = self.index_accounts().await?;
        self.index_dataset_entries(visibility_map).await?;

        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn index_dataset_entries(
        &self,
        visibility_map: PredefinedAccountIdDatasetVisibilityMapping,
    ) -> Result<(), InternalError> {
        use futures::TryStreamExt;

        let mut dataset_entry_stream = self.dataset_entry_service.all_entries();

        while let Some(dataset_entry) = dataset_entry_stream.try_next().await? {
            let is_public_dataset =
                if let Some(visibility) = visibility_map.get(&dataset_entry.owner_id) {
                    visibility.is_public()
                } else {
                    false
                };

            for (name, value) in [
                DatasetPropertyName::allows_public_read(is_public_dataset),
                DatasetPropertyName::allows_anonymous_read(is_public_dataset),
            ] {
                self.rebac_service
                    .set_dataset_property(&dataset_entry.id, name, &value)
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn index_accounts(
        &self,
    ) -> Result<PredefinedAccountIdDatasetVisibilityMapping, InternalError> {
        use futures::TryStreamExt;

        let mut accounts_stream = self.expensive_account_repo.all_accounts();

        let predefined_accounts_map = self.predefined_accounts_config.predefined.iter().fold(
            HashMap::new(),
            |mut acc, account| {
                acc.insert(
                    account.account_name.clone(),
                    account.treat_datasets_as_public,
                );
                acc
            },
        );
        let mut visibility_map = HashMap::new();

        while let Some(account) = accounts_stream.try_next().await? {
            for (name, value) in [AccountPropertyName::is_admin(account.is_admin)] {
                self.rebac_service
                    .set_account_property(&account.id, name, &value)
                    .await
                    .int_err()?;
            }

            if let Some(treat_datasets_as_public) =
                predefined_accounts_map.get(&account.account_name)
            {
                let visibility = if *treat_datasets_as_public {
                    odf::DatasetVisibility::Public
                } else {
                    odf::DatasetVisibility::Private
                };

                visibility_map.insert(account.id, visibility);
            }
        }

        Ok(visibility_map)
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
