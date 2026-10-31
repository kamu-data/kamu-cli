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
use std::time::Duration;

use chrono::Utc;
use email_utils::Email;
use internal_error::InternalError;
use kamu_accounts::{Account, AccountProvider, AccountType, CurrentAccountSubject};
use kamu_adapter_http::smart_protocol::SmartTransferProtocolServerConfig;
use kamu_core::{CompactionExecutor, CompactionPlanner, TenancyConfig};
use kamu_datasets::{
    AlwaysHappyDatasetPermissionChecker,
    CommitDatasetEventUseCase,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetUseCase,
    DatasetPermissionChecker,
    DatasetReferenceService,
    DatasetRegistry,
    MockDatasetPermissionChecker,
};
use kamu_datasets_services::DatasetEntryWriter;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use odf::metadata::testing::MetadataFactory;
use reqwest::Url;
use time_source::SystemTimeSourceStub;

use super::{ServerSideDatasetFixture, create_headless_dataset, find_stored_head};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const SERVER_ACCOUNT_NAME: &str = "kamu-server";
pub(crate) const SERVER_ACCOUNT_EMAIL_ADDRESS: &str = "kamu-server@example.com";

/// The node's default: no test upload lasts long enough for a progress request
const DEFAULT_UPLOAD_PROGRESS_INTERVAL: Duration = Duration::from_secs(10);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
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

    fn cli_create_dataset_use_case_helper(&self) -> Arc<CreateDatasetUseCaseHelper>;

    /// A dataset of the operating account with its own `Seed` but no `HEAD`
    async fn create_headless_dataset(&self, dataset_name: &str) -> odf::DatasetHandle {
        create_headless_dataset(
            self.cli_create_dataset_use_case_helper().as_ref(),
            &self.server_account_id(),
            &odf::DatasetAlias::new(
                self.operating_account_name(),
                odf::DatasetName::new_unchecked(dataset_name),
            ),
        )
        .await
    }

    /// Taken before `api_server_run`, which consumes the harness
    fn stored_heads(&self) -> StoredHeads {
        StoredHeads {
            dataset_reference_service: self.cli_dataset_reference_service(),
        }
    }

    /// Taken before `api_server_run`, which consumes the harness
    fn server_commits(&self) -> ServerCommits {
        ServerCommits {
            commit_dataset_event: self.cli_commit_dataset_event_use_case(),
        }
    }

    fn dataset_url_with_scheme(&self, dataset_alias: &odf::DatasetAlias, scheme: &str) -> Url;

    fn dataset_url(&self, dataset_alias: &odf::DatasetAlias) -> Url {
        self.dataset_url_with_scheme(dataset_alias, "odf+http")
    }

    fn api_server_addr(&self) -> String;

    fn api_server_account(&self) -> Account;

    fn system_time_source(&self) -> &SystemTimeSourceStub;

    fn dataset_fixture(&self) -> Arc<dyn ServerSideDatasetFixture>;

    async fn api_server_run(self) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ServerSideHarnessOptions {
    pub tenancy_config: TenancyConfig,
    pub authorized_writes: bool,
    pub base_catalog: Option<dill::Catalog>,
    pub smart_transfer_protocol_config: SmartTransferProtocolServerConfig,
}

impl Default for ServerSideHarnessOptions {
    fn default() -> Self {
        Self {
            tenancy_config: TenancyConfig::SingleTenant,
            authorized_writes: false,
            base_catalog: None,
            smart_transfer_protocol_config: SmartTransferProtocolServerConfig {
                upload_progress_interval: DEFAULT_UPLOAD_PROGRESS_INTERVAL,
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_server_account(tenancy_config: TenancyConfig) -> Account {
    match tenancy_config {
        TenancyConfig::MultiTenant => Account {
            id: odf::metadata::testing::account_id(&SERVER_ACCOUNT_NAME),
            resource_id: Account::seed_resource_id_from_name(SERVER_ACCOUNT_NAME),
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
        TenancyConfig::SingleTenant => CurrentAccountSubject::new_test(),
        TenancyConfig::MultiTenant => CurrentAccountSubject::logged(
            kamu_accounts::Account::seed_resource_id_from_name(SERVER_ACCOUNT_NAME),
            odf::metadata::testing::account_id(&SERVER_ACCOUNT_NAME),
            odf::AccountName::new_unchecked(SERVER_ACCOUNT_NAME),
        ),
    };

    dill::CatalogBuilder::new_chained(base_catalog)
        .add_value(current_account_subject)
        .add::<AlwaysHappyDatasetPermissionChecker>()
        .build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn create_web_user_catalog(
    base_catalog: &dill::Catalog,
    options: &ServerSideHarnessOptions,
) -> dill::Catalog {
    let mut web_catalog_builder = dill::CatalogBuilder::new_chained(base_catalog);
    if options.authorized_writes {
        web_catalog_builder.add::<AlwaysHappyDatasetPermissionChecker>();
    } else {
        let mut mock_dataset_permission_checker = MockDatasetPermissionChecker::new();
        mock_dataset_permission_checker
            .expect_get_granted_permissions()
            .returning(|_dataset_id| Ok(HashSet::new()));
        web_catalog_builder
            .add_value(mock_dataset_permission_checker)
            .bind::<dyn DatasetPermissionChecker, MockDatasetPermissionChecker>();
    }

    web_catalog_builder.build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reads the `HEAD` stored on the server
pub(crate) struct StoredHeads {
    dataset_reference_service: Arc<dyn DatasetReferenceService>,
}

impl StoredHeads {
    pub async fn get(&self, dataset_id: &odf::DatasetID) -> odf::Multihash {
        self.dataset_reference_service
            .get_reference(dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    }

    /// `None` while the dataset has no `HEAD`
    pub async fn find(&self, dataset_id: &odf::DatasetID) -> Option<odf::Multihash> {
        find_stored_head(self.dataset_reference_service.as_ref(), dataset_id).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Commits to server datasets while the server runs, moving `HEAD` as any
/// writer would
pub(crate) struct ServerCommits {
    commit_dataset_event: Arc<dyn CommitDatasetEventUseCase>,
}

impl ServerCommits {
    pub async fn commit_set_info(&self, dataset_handle: &odf::DatasetHandle) -> odf::Multihash {
        self.commit_dataset_event
            .execute(
                dataset_handle,
                MetadataFactory::set_info()
                    .description("committed while the server runs")
                    .build()
                    .into(),
            )
            .await
            .unwrap()
            .new_head
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
