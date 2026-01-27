// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use email_utils::Email;
use internal_error::InternalError;
use kamu_accounts::{
    Account,
    AccountProvider,
    AccountType,
    CurrentAccountSubject,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME,
};
use kamu_core::{CompactionExecutor, CompactionPlanner, TenancyConfig};
use kamu_datasets::{
    AlwaysHappyDatasetActionAuthorizer,
    CommitDatasetEventUseCase,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetUseCase,
    DatasetActionAuthorizer,
    DatasetReferenceService,
    DatasetRegistry,
};
use kamu_datasets_services::DatasetEntryWriter;
use kamu_datasets_services::testing::MockDatasetActionAuthorizer;
use odf::dataset::DatasetLayout;
use reqwest::Url;
use time_source::SystemTimeSourceStub;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const SERVER_ACCOUNT_NAME: &str = "kamu-server";
pub(crate) const SERVER_ACCOUNT_EMAIL_ADDRESS: &str = "kamu-server@example.com";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub(crate) trait ServerSideHarness {
    fn server_account_id(&self) -> odf::AccountID;

    fn server_account_name(&self) -> odf::AccountName;

    fn operating_account_name(&self) -> Option<odf::AccountName>;

    fn cli_dataset_registry(&self) -> Arc<dyn DatasetRegistry>;

    fn cli_create_dataset_use_case(&self) -> Arc<dyn CreateDatasetUseCase>;

    fn cli_create_dataset_from_snapshot_use_case(
        &self,
    ) -> Arc<dyn CreateDatasetFromSnapshotUseCase>;

    fn cli_commit_dataset_event_use_case(&self) -> Arc<dyn CommitDatasetEventUseCase>;

    fn cli_compaction_planner(&self) -> Arc<dyn CompactionPlanner>;

    fn cli_compaction_executor(&self) -> Arc<dyn CompactionExecutor>;

    fn cli_dataset_entry_writer(&self) -> Arc<dyn DatasetEntryWriter>;

    fn cli_dataset_reference_service(&self) -> Arc<dyn DatasetReferenceService>;

    fn dataset_layout(&self, dataset_handle: &odf::DatasetHandle) -> DatasetLayout;

    fn dataset_url_with_scheme(&self, dataset_alias: &odf::DatasetAlias, scheme: &str) -> Url;

    fn dataset_url(&self, dataset_alias: &odf::DatasetAlias) -> Url {
        self.dataset_url_with_scheme(dataset_alias, "odf+http")
    }

    fn api_server_addr(&self) -> String;

    fn api_server_account(&self) -> Account;

    fn system_time_source(&self) -> &SystemTimeSourceStub;

    async fn api_server_run(self) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ServerSideHarnessOptions {
    pub tenancy_config: TenancyConfig,
    pub authorized_writes: bool,
    pub base_catalog: Option<dill::Catalog>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_server_account(tenancy_config: TenancyConfig) -> Account {
    match tenancy_config {
        TenancyConfig::MultiTenant => Account {
            id: odf::AccountID::new_seeded_ed25519(SERVER_ACCOUNT_NAME.as_bytes()),
            account_name: odf::AccountName::new_unchecked(SERVER_ACCOUNT_NAME),
            account_type: AccountType::User,
            display_name: SERVER_ACCOUNT_NAME.to_string(),
            email: Email::parse(SERVER_ACCOUNT_EMAIL_ADDRESS).unwrap(),
            avatar_url: None,
            registered_at: Utc::now(),
            provider: AccountProvider::Password.to_string(),
            provider_identity_key: String::from(SERVER_ACCOUNT_NAME),
        },
        TenancyConfig::SingleTenant => Account::dummy(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn create_cli_user_catalog(
    base_catalog: &dill::Catalog,
    tenancy_config: TenancyConfig,
) -> dill::Catalog {
    let current_account_subject = match tenancy_config {
        TenancyConfig::SingleTenant => {
            CurrentAccountSubject::logged(DEFAULT_ACCOUNT_ID.clone(), DEFAULT_ACCOUNT_NAME.clone())
        }
        TenancyConfig::MultiTenant => CurrentAccountSubject::logged(
            odf::AccountID::new_seeded_ed25519(SERVER_ACCOUNT_NAME.as_bytes()),
            odf::AccountName::new_unchecked(SERVER_ACCOUNT_NAME),
        ),
    };

    dill::CatalogBuilder::new_chained(base_catalog)
        .add_value(current_account_subject)
        .add::<AlwaysHappyDatasetActionAuthorizer>()
        .build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn create_web_user_catalog(
    base_catalog: &dill::Catalog,
    options: &ServerSideHarnessOptions,
) -> dill::Catalog {
    let mut web_catalog_builder = dill::CatalogBuilder::new_chained(base_catalog);
    if options.authorized_writes {
        web_catalog_builder.add::<AlwaysHappyDatasetActionAuthorizer>();
    } else {
        let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();
        mock_dataset_action_authorizer
            .expect_get_allowed_actions()
            .returning(|_dataset_id| Ok(HashSet::new()));
        web_catalog_builder
            .add_value(mock_dataset_action_authorizer)
            .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>();
    }

    web_catalog_builder.build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
