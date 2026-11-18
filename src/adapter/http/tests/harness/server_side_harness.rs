// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use email_utils::Email;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::{RunInfoDir, ServerUrlConfig};
use kamu::*;
use kamu_accounts::{
    Account,
    AccountConfig,
    AccountProvider,
    AccountType,
    AuthConfig,
    CurrentAccountSubject,
    DEFAULT_ACCOUNT_NAME,
    DidSecretEncryptionConfig,
    JwtAuthenticationConfig,
    PredefinedAccountsConfig,
    TEST_ACCOUNT_ID,
};
use kamu_accounts_inmem::{
    InMemoryAccessTokenRepository,
    InMemoryAccountRepository,
    InMemoryDidSecretKeyRepository,
    InMemoryOAuthDeviceCodeRepository,
};
use kamu_accounts_services::*;
use kamu_adapter_http::smart_protocol::SmartTransferProtocolServerConfig;
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacDatasetRegistryFacadeImpl,
    RebacServiceImpl,
};
use kamu_core::{CompactionExecutor, CompactionPlanner, DidGeneratorDefault, TenancyConfig};
use kamu_datasets::{
    AlwaysHappyDatasetPermissionChecker,
    CommitDatasetEventUseCase,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetUseCase,
    DatasetLifecycleMessage,
    DatasetPermissionChecker,
    DatasetReferenceMessage,
    DatasetReferenceService,
    DatasetRegistry,
    MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    MockDatasetPermissionChecker,
};
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use messaging_outbox::{ConsumerFilter, Outbox, OutboxImmediateImpl, register_message_dispatcher};
use reqwest::Url;
use time_source::{SystemTimeSource, SystemTimeSourceStub};

use super::{
    DatasetCommits,
    HeadlessDatasets,
    ServerSideDatasetFixture,
    StoredHeads,
    TestAPIServer,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const SERVER_ACCOUNT_NAME: &str = "kamu-server";
pub(crate) const SERVER_ACCOUNT_EMAIL_ADDRESS: &str = "kamu-server@example.com";

/// Longer than any test upload, so no progress request is sent unless a test
/// sets a shorter interval
const DEFAULT_UPLOAD_PROGRESS_INTERVAL: Duration = Duration::from_secs(10);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
pub(crate) trait ServerSideHarness {
    fn base(&self) -> &BaseServerSideHarness;

    fn dataset_fixture(&self) -> Arc<dyn ServerSideDatasetFixture>;

    async fn api_server_run(self) -> Result<(), InternalError>;

    fn base_catalog(&self) -> &dill::Catalog {
        &self.base().base_catalog
    }

    fn tenancy_config(&self) -> TenancyConfig {
        self.base().options.tenancy_config
    }

    fn api_server(&self) -> &TestAPIServer {
        &self.base().api_server
    }

    fn server_account(&self) -> &Account {
        &self.base().account
    }

    fn system_time_source(&self) -> &SystemTimeSourceStub {
        &self.base().time_source
    }

    fn server_account_id(&self) -> odf::AccountID {
        match self.tenancy_config() {
            TenancyConfig::MultiTenant => odf::metadata::testing::account_id(&SERVER_ACCOUNT_NAME),
            TenancyConfig::SingleTenant => TEST_ACCOUNT_ID.clone(),
        }
    }

    fn server_account_name(&self) -> odf::AccountName {
        match self.tenancy_config() {
            TenancyConfig::MultiTenant => odf::AccountName::new_unchecked(SERVER_ACCOUNT_NAME),
            TenancyConfig::SingleTenant => DEFAULT_ACCOUNT_NAME.clone(),
        }
    }

    fn operating_account_name(&self) -> Option<odf::AccountName> {
        match self.tenancy_config() {
            TenancyConfig::MultiTenant => {
                Some(odf::AccountName::new_unchecked(SERVER_ACCOUNT_NAME))
            }
            TenancyConfig::SingleTenant => None,
        }
    }

    fn cli_catalog(&self) -> dill::Catalog {
        let current_account_subject = match self.tenancy_config() {
            TenancyConfig::SingleTenant => CurrentAccountSubject::new_test(),
            TenancyConfig::MultiTenant => CurrentAccountSubject::logged(
                Account::seed_resource_id_from_name(SERVER_ACCOUNT_NAME),
                odf::metadata::testing::account_id(&SERVER_ACCOUNT_NAME),
                odf::AccountName::new_unchecked(SERVER_ACCOUNT_NAME),
            ),
        };

        let mut b = dill::CatalogBuilder::new_chained(self.base_catalog());
        current_account_subject.register(&mut b);
        b.add::<AlwaysHappyDatasetPermissionChecker>();
        b.build()
    }

    fn cli_dataset_registry(&self) -> Arc<dyn DatasetRegistry> {
        self.cli_catalog().get_one().unwrap()
    }

    fn cli_create_dataset_use_case(&self) -> Arc<dyn CreateDatasetUseCase> {
        self.cli_catalog().get_one().unwrap()
    }

    fn cli_create_dataset_from_snapshot_use_case(
        &self,
    ) -> Arc<dyn CreateDatasetFromSnapshotUseCase> {
        self.cli_catalog().get_one().unwrap()
    }

    fn cli_commit_dataset_event_use_case(&self) -> Arc<dyn CommitDatasetEventUseCase> {
        self.cli_catalog().get_one().unwrap()
    }

    fn cli_compaction_planner(&self) -> Arc<dyn CompactionPlanner> {
        self.cli_catalog().get_one().unwrap()
    }

    fn cli_compaction_executor(&self) -> Arc<dyn CompactionExecutor> {
        self.cli_catalog().get_one().unwrap()
    }

    fn cli_dataset_entry_writer(&self) -> Arc<dyn DatasetEntryWriter> {
        self.cli_catalog().get_one().unwrap()
    }

    fn cli_dataset_reference_service(&self) -> Arc<dyn DatasetReferenceService> {
        self.cli_catalog().get_one().unwrap()
    }

    fn cli_create_dataset_use_case_helper(&self) -> Arc<CreateDatasetUseCaseHelper> {
        self.cli_catalog().get_one().unwrap()
    }

    /// A dataset of the operating account with its own `Seed` but no `HEAD`
    async fn create_headless_dataset(&self, dataset_name: &str) -> odf::DatasetHandle {
        HeadlessDatasets::new(self.cli_create_dataset_use_case_helper())
            .create(
                &self.server_account_id(),
                &odf::DatasetAlias::new(
                    self.operating_account_name(),
                    odf::DatasetName::new_unchecked(dataset_name),
                ),
            )
            .await
    }

    /// The server's handle of a dataset pushed from the client, which keeps its
    /// ID on the server
    async fn server_dataset_handle(
        &self,
        client_handle: &odf::DatasetHandle,
    ) -> odf::DatasetHandle {
        self.cli_dataset_registry()
            .resolve_dataset_handle_by_ref(&client_handle.as_local_ref())
            .await
            .unwrap()
    }

    /// Taken before `api_server_run`, which consumes the harness
    fn stored_heads(&self) -> StoredHeads {
        StoredHeads::new(self.cli_dataset_reference_service())
    }

    /// Taken before `api_server_run`, which consumes the harness
    fn server_commits(&self) -> DatasetCommits {
        DatasetCommits::new(self.cli_commit_dataset_event_use_case())
    }

    fn dataset_url_with_scheme(&self, dataset_alias: &odf::DatasetAlias, scheme: &str) -> Url {
        let api_server_address = self.api_server_addr();
        let url = match self.tenancy_config() {
            TenancyConfig::MultiTenant => {
                let account_name = dataset_alias
                    .account_name
                    .as_ref()
                    .expect("Account name not specified in alias");
                format!(
                    "{scheme}://{api_server_address}/{account_name}/{}",
                    dataset_alias.dataset_name
                )
            }
            TenancyConfig::SingleTenant => format!(
                "{scheme}://{api_server_address}/{}",
                dataset_alias.dataset_name
            ),
        };
        Url::parse(&url).unwrap()
    }

    fn dataset_url(&self, dataset_alias: &odf::DatasetAlias) -> Url {
        self.dataset_url_with_scheme(dataset_alias, "odf+http")
    }

    fn api_server_addr(&self) -> String {
        self.api_server().local_addr().to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ServerSideHarnessOptions {
    pub tenancy_config: TenancyConfig,
    pub authorized_writes: bool,
    /// Builds the permission checker of web requests in place of the one
    /// `authorized_writes` selects
    pub web_permission_checker: Option<fn() -> MockDatasetPermissionChecker>,
    pub base_catalog: Option<dill::Catalog>,
    pub smart_transfer_protocol_config: SmartTransferProtocolServerConfig,
}

impl Default for ServerSideHarnessOptions {
    fn default() -> Self {
        Self {
            tenancy_config: TenancyConfig::SingleTenant,
            authorized_writes: false,
            web_permission_checker: None,
            base_catalog: None,
            smart_transfer_protocol_config: SmartTransferProtocolServerConfig {
                upload_progress_interval: DEFAULT_UPLOAD_PROGRESS_INTERVAL,
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The server state shared by the storage-specific harnesses: the account, the
/// clock, the base catalog and the API server over it
pub(crate) struct BaseServerSideHarness {
    base_catalog: dill::Catalog,
    api_server: TestAPIServer,
    options: ServerSideHarnessOptions,
    time_source: SystemTimeSourceStub,
    account: Account,
}

impl BaseServerSideHarness {
    /// `register_storage` adds what depends on the storage: the dataset
    /// storage, the metadata append and reference update paths, and the
    /// database components
    pub async fn new(
        options: ServerSideHarnessOptions,
        run_info_dir: PathBuf,
        register_storage: impl FnOnce(&mut dill::CatalogBuilder),
    ) -> Self {
        let account = Self::make_server_account(options.tenancy_config);
        let time_source = SystemTimeSourceStub::new();
        let (listener, base_url_rest) = Self::bind_api_listener().await;

        let base_catalog = {
            let mut b = match &options.base_catalog {
                None => dill::CatalogBuilder::new(),
                Some(c) => dill::CatalogBuilder::new_chained(c),
            };

            Self::register_common_server_components(
                &mut b,
                &options,
                &account,
                &time_source,
                run_info_dir,
                &base_url_rest,
            );
            register_storage(&mut b);

            b.build()
        };

        let api_server = Self::start_api_server(&base_catalog, listener, &options).await;

        Self {
            base_catalog,
            api_server,
            options,
            time_source,
            account,
        }
    }

    pub async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }

    /// Binds the API server's port up front, so the catalog can carry its URL
    async fn bind_api_listener() -> (tokio::net::TcpListener, String) {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let base_url_rest = format!("http://{}", listener.local_addr().unwrap());
        (listener, base_url_rest)
    }

    /// Registers the server components that do not depend on the storage
    fn register_common_server_components(
        b: &mut dill::CatalogBuilder,
        options: &ServerSideHarnessOptions,
        server_account: &Account,
        time_source: &SystemTimeSourceStub,
        run_info_dir: PathBuf,
        base_url_rest: &str,
    ) {
        let predefined_accounts_config = match options.tenancy_config {
            TenancyConfig::SingleTenant => PredefinedAccountsConfig::test_single_tenant_with_id(),
            TenancyConfig::MultiTenant => {
                let mut predefined_accounts_config = PredefinedAccountsConfig::new();
                predefined_accounts_config.predefined.push(
                    AccountConfig::test_config_from_name_with_id(odf::AccountName::new_unchecked(
                        SERVER_ACCOUNT_NAME,
                    )),
                );
                predefined_accounts_config
            }
        };

        let jwt_authentication_config = JwtAuthenticationConfig {
            jwt_secret: "dummy".to_string(),
            maybe_dummy_token_account: Some(server_account.clone()),
        };

        kamu_datasets_inmem::register_dependencies(b);
        b.add_value(time_source.clone())
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<DidGeneratorDefault>()
            .add_value(RunInfoDir::new(run_info_dir))
            .add_value(AuthConfig::sample())
            .add_builder(OutboxImmediateImpl::builder(ConsumerFilter::AllConsumers))
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add::<DependencyGraphServiceImpl>()
            .add_value(options.tenancy_config)
            .add_value(MetadataChainDbBackedConfig::default())
            .add_value(ServerUrlConfig::new_test(Some(base_url_rest)))
            .add_value(options.smart_transfer_protocol_config.clone())
            .add_value(EngineConfigDatafusionEmbeddedCompaction::default())
            .add::<CompactionPlannerImpl>()
            .add::<CompactionExecutorImpl>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add::<CreateDatasetUseCaseImpl>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<CommitDatasetEventUseCaseImpl>()
            .add::<CreateDatasetUseCaseHelper>()
            .add::<DatasetEntryServiceImpl>()
            .add::<DatasetReferenceServiceImpl>()
            .add::<AuthenticationServiceImpl>()
            .add::<AccountServiceImpl>()
            .add::<CreateAccountUseCaseImpl>()
            .add::<UpdateAccountUseCaseImpl>()
            .add::<InMemoryAccountRepository>()
            .add::<InMemoryDidSecretKeyRepository>()
            .add_value(DidSecretEncryptionConfig::sample())
            .add::<AccessTokenServiceImpl>()
            .add::<InMemoryAccessTokenRepository>()
            .add_value(jwt_authentication_config)
            .add::<LoginPasswordAuthProvider>()
            .add::<PredefinedAccountsRegistrator>()
            .add::<RebacServiceImpl>()
            .add::<InMemoryRebacRepository>()
            .add_value(DefaultAccountProperties::default())
            .add_value(DefaultDatasetProperties::default())
            .add::<RebacDatasetRegistryFacadeImpl>()
            .add_value(predefined_accounts_config)
            .add::<OAuthDeviceCodeServiceImpl>()
            .add::<OAuthDeviceCodeGeneratorDefault>()
            .add::<InMemoryOAuthDeviceCodeRepository>();

        register_message_dispatcher::<DatasetLifecycleMessage>(
            b,
            MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        );

        register_message_dispatcher::<DatasetReferenceMessage>(
            b,
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
        );
    }

    /// Runs the startup jobs, such as registering the predefined accounts, and
    /// builds the API server over the web user catalog
    async fn start_api_server(
        base_catalog: &dill::Catalog,
        listener: tokio::net::TcpListener,
        options: &ServerSideHarnessOptions,
    ) -> TestAPIServer {
        init_on_startup::run_startup_jobs(base_catalog)
            .await
            .unwrap();

        TestAPIServer::new(
            Self::create_web_user_catalog(base_catalog, options),
            listener,
            options.tenancy_config,
        )
    }

    fn make_server_account(tenancy_config: TenancyConfig) -> Account {
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

    fn create_web_user_catalog(
        base_catalog: &dill::Catalog,
        options: &ServerSideHarnessOptions,
    ) -> dill::Catalog {
        let mut web_catalog_builder = dill::CatalogBuilder::new_chained(base_catalog);
        if let Some(make_checker) = options.web_permission_checker {
            web_catalog_builder
                .add_value(make_checker())
                .bind::<dyn DatasetPermissionChecker, MockDatasetPermissionChecker>();
        } else if options.authorized_writes {
            web_catalog_builder.add::<AlwaysHappyDatasetPermissionChecker>();
        } else {
            let mut mock_dataset_permission_checker = MockDatasetPermissionChecker::new();
            mock_dataset_permission_checker
                .expect_get_granted_permissions()
                .returning(|_, _| Ok(HashSet::new()));
            web_catalog_builder
                .add_value(mock_dataset_permission_checker)
                .bind::<dyn DatasetPermissionChecker, MockDatasetPermissionChecker>();
        }

        web_catalog_builder.build()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
