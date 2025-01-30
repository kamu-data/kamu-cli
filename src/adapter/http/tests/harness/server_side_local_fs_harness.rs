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
use kamu::domain::{CacheDir, CommitDatasetEventUseCase, RunInfoDir, ServerUrlConfig};
use kamu::{
    AppendDatasetMetadataBatchUseCaseImpl,
    CommitDatasetEventUseCaseImpl,
    CompactionExecutorImpl,
    CompactionPlannerImpl,
    DatasetRegistrySoloUnitBridge,
    DatasetStorageUnitLocalFs,
    EditDatasetUseCaseImpl,
    ObjectStoreBuilderLocalFs,
    ObjectStoreRegistryImpl,
    RemoteRepositoryRegistryImpl,
    ViewDatasetUseCaseImpl,
};
use kamu_accounts::testing::MockAuthenticationService;
use kamu_accounts::{Account, AuthenticationService};
use kamu_core::{
    CompactionExecutor,
    CompactionPlanner,
    DatasetRegistry,
    DidGeneratorDefault,
    TenancyConfig,
};
use kamu_datasets::{CreateDatasetFromSnapshotUseCase, CreateDatasetUseCase};
use kamu_datasets_inmem::InMemoryDatasetDependencyRepository;
use kamu_datasets_services::{
    CreateDatasetFromSnapshotUseCaseImpl,
    CreateDatasetUseCaseImpl,
    DependencyGraphServiceImpl,
};
use messaging_outbox::DummyOutboxImpl;
use odf::dataset::DatasetLayout;
use tempfile::TempDir;
use time_source::{SystemTimeSource, SystemTimeSourceStub};
use url::Url;

use super::{
    create_cli_user_catalog,
    create_web_user_catalog,
    make_server_account,
    server_authentication_mock,
    ServerSideHarness,
    ServerSideHarnessOptions,
    TestAPIServer,
    SERVER_ACCOUNT_NAME,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub(crate) struct ServerSideLocalFsHarness {
    tempdir: TempDir,
    base_catalog: dill::Catalog,
    api_server: TestAPIServer,
    options: ServerSideHarnessOptions,
    time_source: SystemTimeSourceStub,
    account: Account,
}

impl ServerSideLocalFsHarness {
    pub async fn new(options: ServerSideHarnessOptions) -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let run_info_dir = tempdir.path().join("run");
        std::fs::create_dir(&run_info_dir).unwrap();

        let cache_dir = tempdir.path().join("cache");
        std::fs::create_dir(&cache_dir).unwrap();

        let account = make_server_account();

        let time_source = SystemTimeSourceStub::new();
        let (base_catalog, listener) = {
            let mut b = match &options.base_catalog {
                None => dill::CatalogBuilder::new(),
                Some(c) => dill::CatalogBuilder::new_chained(c),
            };

            let addr = SocketAddr::from(([127, 0, 0, 1], 0));
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            let base_url_rest = format!("http://{}", listener.local_addr().unwrap());

            b.add_value(RunInfoDir::new(run_info_dir))
                .add::<DidGeneratorDefault>()
                .add_value(CacheDir::new(cache_dir))
                .add::<DummyOutboxImpl>()
                .add_value(time_source.clone())
                .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
                .add::<DependencyGraphServiceImpl>()
                .add::<InMemoryDatasetDependencyRepository>()
                .add_value(options.tenancy_config)
                .add_builder(DatasetStorageUnitLocalFs::builder().with_root(datasets_dir))
                .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitLocalFs>()
                .bind::<dyn odf::DatasetStorageUnitWriter, DatasetStorageUnitLocalFs>()
                .add_value(server_authentication_mock(&account))
                .add::<DatasetRegistrySoloUnitBridge>()
                .bind::<dyn AuthenticationService, MockAuthenticationService>()
                .add_value(ServerUrlConfig::new_test(Some(&base_url_rest)))
                .add::<CompactionPlannerImpl>()
                .add::<CompactionExecutorImpl>()
                .add::<ObjectStoreRegistryImpl>()
                .add::<ObjectStoreBuilderLocalFs>()
                .add::<RemoteRepositoryRegistryImpl>()
                .add::<AppendDatasetMetadataBatchUseCaseImpl>()
                .add::<CreateDatasetUseCaseImpl>()
                .add::<CreateDatasetFromSnapshotUseCaseImpl>()
                .add::<CommitDatasetEventUseCaseImpl>()
                .add::<ViewDatasetUseCaseImpl>()
                .add::<EditDatasetUseCaseImpl>();

            database_common::NoOpDatabasePlugin::init_database_components(&mut b);

            (b.build(), listener)
        };

        let api_server = TestAPIServer::new(
            create_web_user_catalog(&base_catalog, &options),
            listener,
            options.tenancy_config,
        );

        Self {
            tempdir,
            base_catalog,
            api_server,
            options,
            time_source,
            account,
        }
    }

    fn internal_datasets_folder_path(&self) -> PathBuf {
        self.tempdir.path().join("datasets")
    }

    pub fn base_catalog(&self) -> &dill::Catalog {
        &self.base_catalog
    }

    pub fn server_account(&self) -> &Account {
        &self.account
    }
}

#[async_trait::async_trait]
impl ServerSideHarness for ServerSideLocalFsHarness {
    fn operating_account_name(&self) -> Option<odf::AccountName> {
        match self.options.tenancy_config {
            TenancyConfig::MultiTenant => {
                Some(odf::AccountName::new_unchecked(SERVER_ACCOUNT_NAME))
            }
            TenancyConfig::SingleTenant => None,
        }
    }

    fn cli_dataset_registry(&self) -> Arc<dyn DatasetRegistry> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog);
        cli_catalog.get_one::<dyn DatasetRegistry>().unwrap()
    }

    fn cli_create_dataset_use_case(&self) -> Arc<dyn CreateDatasetUseCase> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog);
        cli_catalog.get_one::<dyn CreateDatasetUseCase>().unwrap()
    }

    fn cli_create_dataset_from_snapshot_use_case(
        &self,
    ) -> Arc<dyn CreateDatasetFromSnapshotUseCase> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog);
        cli_catalog
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap()
    }

    fn cli_commit_dataset_event_use_case(&self) -> Arc<dyn CommitDatasetEventUseCase> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog);
        cli_catalog
            .get_one::<dyn CommitDatasetEventUseCase>()
            .unwrap()
    }

    fn cli_compaction_planner(&self) -> Arc<dyn CompactionPlanner> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog);
        cli_catalog.get_one::<dyn CompactionPlanner>().unwrap()
    }

    fn cli_compaction_executor(&self) -> Arc<dyn CompactionExecutor> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog);
        cli_catalog.get_one::<dyn CompactionExecutor>().unwrap()
    }

    fn api_server_addr(&self) -> String {
        self.api_server.local_addr().to_string()
    }

    fn api_server_account(&self) -> Account {
        self.account.clone()
    }

    fn dataset_url_with_scheme(&self, dataset_alias: &odf::DatasetAlias, scheme: &str) -> Url {
        let api_server_address = self.api_server_addr();
        Url::from_str(
            match self.options.tenancy_config {
                TenancyConfig::MultiTenant => format!(
                    "{}://{}/{}/{}",
                    scheme,
                    api_server_address,
                    if let Some(account_name) = &dataset_alias.account_name {
                        account_name.to_string()
                    } else {
                        panic!("Account name not specified in alias");
                    },
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

    fn dataset_layout(&self, dataset_handle: &odf::DatasetHandle) -> DatasetLayout {
        let root_path = match self.options.tenancy_config {
            TenancyConfig::MultiTenant => self
                .internal_datasets_folder_path()
                .join(
                    if let Some(account_name) = &dataset_handle.alias.account_name {
                        account_name.to_string()
                    } else {
                        panic!("Account name not specified in alias");
                    },
                )
                .join(dataset_handle.id.as_multibase().to_stack_string()),
            TenancyConfig::SingleTenant => self
                .internal_datasets_folder_path()
                .join(dataset_handle.alias.dataset_name.clone()),
        };
        DatasetLayout::new(root_path.as_path())
    }

    fn system_time_source(&self) -> &SystemTimeSourceStub {
        &self.time_source
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
