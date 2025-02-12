// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use dill::Component;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::*;
use kamu::*;
use kamu_accounts::*;
use kamu_accounts_inmem::{InMemoryAccessTokenRepository, InMemoryAccountRepository};
use kamu_accounts_services::*;
use kamu_core::{DatasetRegistry, DidGeneratorDefault, TenancyConfig};
use kamu_datasets::*;
use kamu_datasets_inmem::{InMemoryDatasetDependencyRepository, InMemoryDatasetEntryRepository};
use kamu_datasets_services::*;
use messaging_outbox::DummyOutboxImpl;
use odf::dataset::DatasetLayout;
use s3_utils::S3Context;
use test_utils::LocalS3Server;
use time_source::{SystemTimeSource, SystemTimeSourceStub};
use url::Url;

use super::{
    create_cli_user_catalog,
    create_web_user_catalog,
    make_server_account,
    ServerSideHarness,
    ServerSideHarnessOptions,
    TestAPIServer,
    SERVER_ACCOUNT_NAME,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub(crate) struct ServerSideS3Harness {
    s3: LocalS3Server,
    base_catalog: dill::Catalog,
    api_server: TestAPIServer,
    options: ServerSideHarnessOptions,
    time_source: SystemTimeSourceStub,
    _temp_dir: tempfile::TempDir,
    account: Account,
}

impl ServerSideS3Harness {
    pub async fn new(options: ServerSideHarnessOptions) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = temp_dir.path().join("run");
        std::fs::create_dir(&run_info_dir).unwrap();

        let s3 = LocalS3Server::new().await;
        let s3_context = S3Context::from_url(&s3.url).await;

        let time_source = SystemTimeSourceStub::new();

        let account = make_server_account(options.tenancy_config);

        let predefined_accounts_config = match options.tenancy_config {
            TenancyConfig::SingleTenant => PredefinedAccountsConfig::single_tenant(),
            TenancyConfig::MultiTenant => {
                let mut predefined_accounts_config = PredefinedAccountsConfig::new();
                predefined_accounts_config
                    .predefined
                    .push(AccountConfig::test_config_from_name(
                        odf::AccountName::new_unchecked(SERVER_ACCOUNT_NAME),
                    ));
                predefined_accounts_config
            }
        };

        let jwt_authentication_config = JwtAuthenticationConfig {
            jwt_secret: "dummy".to_string(),
            maybe_dummy_token_account: Some(account.clone()),
        };

        let (base_catalog, listener) = {
            let addr = SocketAddr::from(([127, 0, 0, 1], 0));
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            let base_url_rest = format!("http://{}", listener.local_addr().unwrap());

            let mut b = dill::CatalogBuilder::new();

            b.add_value(time_source.clone())
                .add::<DidGeneratorDefault>()
                .add_value(RunInfoDir::new(run_info_dir))
                .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
                .add::<DummyOutboxImpl>()
                .add::<DependencyGraphServiceImpl>()
                .add::<InMemoryDatasetDependencyRepository>()
                .add_value(options.tenancy_config)
                .add_builder(DatasetStorageUnitS3::builder().with_s3_context(s3_context.clone()))
                .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitS3>()
                .bind::<dyn odf::DatasetStorageUnitWriter, DatasetStorageUnitS3>()
                .add_value(ServerUrlConfig::new_test(Some(&base_url_rest)))
                .add::<CompactionPlannerImpl>()
                .add::<CompactionExecutorImpl>()
                .add::<ObjectStoreRegistryImpl>()
                .add::<ObjectStoreBuilderLocalFs>()
                .add_value(ObjectStoreBuilderS3::new(s3_context, true))
                .bind::<dyn ObjectStoreBuilder, ObjectStoreBuilderS3>()
                .add::<AppendDatasetMetadataBatchUseCaseImpl>()
                .add::<CreateDatasetUseCaseImpl>()
                .add::<CreateDatasetFromSnapshotUseCaseImpl>()
                .add::<CommitDatasetEventUseCaseImpl>()
                .add::<ViewDatasetUseCaseImpl>()
                .add::<EditDatasetUseCaseImpl>()
                .add::<DatasetEntryServiceImpl>()
                .add::<InMemoryDatasetEntryRepository>()
                .add::<AuthenticationServiceImpl>()
                .add::<AccountServiceImpl>()
                .add::<InMemoryAccountRepository>()
                .add::<AccessTokenServiceImpl>()
                .add::<InMemoryAccessTokenRepository>()
                .add_value(jwt_authentication_config)
                .add::<LoginPasswordAuthProvider>()
                .add::<PredefinedAccountsRegistrator>()
                .add_value(predefined_accounts_config);

            database_common::NoOpDatabasePlugin::init_database_components(&mut b);

            (b.build(), listener)
        };

        init_on_startup::run_startup_jobs(&base_catalog)
            .await
            .unwrap();

        let api_server = TestAPIServer::new(
            create_web_user_catalog(&base_catalog, &options),
            listener,
            options.tenancy_config,
        );

        Self {
            s3,
            base_catalog,
            api_server,
            options,
            time_source,
            _temp_dir: temp_dir,
            account,
        }
    }

    pub fn internal_bucket_folder_path(&self) -> PathBuf {
        self.s3.tmp_dir.path().join(&self.s3.bucket)
    }
}

#[async_trait::async_trait]
impl ServerSideHarness for ServerSideS3Harness {
    fn server_account_id(&self) -> odf::AccountID {
        match self.options.tenancy_config {
            TenancyConfig::MultiTenant => {
                odf::AccountID::new_seeded_ed25519(SERVER_ACCOUNT_NAME.as_bytes())
            }
            TenancyConfig::SingleTenant => DEFAULT_ACCOUNT_ID.clone(),
        }
    }

    fn operating_account_name(&self) -> Option<odf::AccountName> {
        match self.options.tenancy_config {
            TenancyConfig::MultiTenant => {
                Some(odf::AccountName::new_unchecked(SERVER_ACCOUNT_NAME))
            }
            TenancyConfig::SingleTenant => None,
        }
    }

    fn cli_dataset_registry(&self) -> Arc<dyn DatasetRegistry> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn cli_create_dataset_use_case(&self) -> Arc<dyn CreateDatasetUseCase> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn cli_create_dataset_from_snapshot_use_case(
        &self,
    ) -> Arc<dyn CreateDatasetFromSnapshotUseCase> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn cli_commit_dataset_event_use_case(&self) -> Arc<dyn CommitDatasetEventUseCase> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn cli_compaction_planner(&self) -> Arc<dyn CompactionPlanner> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn cli_compaction_executor(&self) -> Arc<dyn CompactionExecutor> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn cli_dataset_entry_writer(&self) -> Arc<dyn DatasetEntryWriter> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn dataset_url_with_scheme(&self, dataset_alias: &odf::DatasetAlias, scheme: &str) -> Url {
        let api_server_address = self.api_server_addr();
        Url::from_str(
            match self.options.tenancy_config {
                TenancyConfig::MultiTenant => format!(
                    "{}://{}/{}/{}",
                    scheme,
                    api_server_address,
                    dataset_alias.account_name.as_ref().unwrap(),
                    dataset_alias.dataset_name
                ),
                TenancyConfig::SingleTenant => format!(
                    "{}://{}/{}",
                    scheme, api_server_address, dataset_alias.dataset_name
                ),
            }
            .as_str(),
        )
        .unwrap()
    }

    fn api_server_addr(&self) -> String {
        self.api_server.local_addr().to_string()
    }

    fn api_server_account(&self) -> Account {
        self.account.clone()
    }

    fn dataset_layout(&self, dataset_handle: &odf::DatasetHandle) -> DatasetLayout {
        DatasetLayout::new(
            self.internal_bucket_folder_path()
                .join(dataset_handle.id.as_multibase().to_stack_string())
                .as_path(),
        )
    }

    fn system_time_source(&self) -> &SystemTimeSourceStub {
        &self.time_source
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
