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
use kamu::domain::{
    CacheDir,
    CommitDatasetEventUseCase,
    CompactionService,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetUseCase,
    DatasetRepository,
    RunInfoDir,
    ServerUrlConfig,
};
use kamu::{
    AppendDatasetMetadataBatchUseCaseImpl,
    CommitDatasetEventUseCaseImpl,
    CompactionServiceImpl,
    CreateDatasetFromSnapshotUseCaseImpl,
    CreateDatasetUseCaseImpl,
    DatasetLayout,
    DatasetRepositoryLocalFs,
    DatasetRepositoryWriter,
    DependencyGraphServiceInMemory,
    ObjectStoreBuilderLocalFs,
    ObjectStoreRegistryImpl,
};
use kamu_accounts::{AuthenticationService, MockAuthenticationService};
use messaging_outbox::DummyOutboxImpl;
use opendatafabric::{AccountName, DatasetAlias, DatasetHandle};
use tempfile::TempDir;
use time_source::{SystemTimeSource, SystemTimeSourceStub};
use url::Url;

use super::{
    create_cli_user_catalog,
    create_web_user_catalog,
    server_authentication_mock,
    ServerSideHarness,
    ServerSideHarnessOptions,
    ServerSideHarnessOverrides,
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
}

impl ServerSideLocalFsHarness {
    pub async fn new(
        options: ServerSideHarnessOptions,
        overrides: ServerSideHarnessOverrides,
    ) -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let run_info_dir = tempdir.path().join("run");
        std::fs::create_dir(&run_info_dir).unwrap();

        let cache_dir = tempdir.path().join("cache");
        std::fs::create_dir(&cache_dir).unwrap();

        let time_source = SystemTimeSourceStub::new();
        let (base_catalog, listener) = {
            let mut b = match &options.base_catalog {
                None => dill::CatalogBuilder::new(),
                Some(c) => dill::CatalogBuilder::new_chained(c),
            };

            let addr = SocketAddr::from(([127, 0, 0, 1], 0));
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            let base_url_rest = format!("http://{}", listener.local_addr().unwrap());
            let mock_auth_service = overrides
                .mock_authentication_service
                .unwrap_or(server_authentication_mock());

            b.add_value(RunInfoDir::new(run_info_dir))
                .add_value(CacheDir::new(cache_dir))
                .add::<DummyOutboxImpl>()
                .add_value(time_source.clone())
                .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
                .add::<DependencyGraphServiceInMemory>()
                .add_builder(
                    DatasetRepositoryLocalFs::builder()
                        .with_root(datasets_dir)
                        .with_multi_tenant(options.multi_tenant),
                )
                .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
                .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
                .add_value(mock_auth_service)
                .bind::<dyn AuthenticationService, MockAuthenticationService>()
                .add_value(ServerUrlConfig::new_test(Some(&base_url_rest)))
                .add::<CompactionServiceImpl>()
                .add::<ObjectStoreRegistryImpl>()
                .add::<ObjectStoreBuilderLocalFs>()
                .add::<AppendDatasetMetadataBatchUseCaseImpl>()
                .add::<CreateDatasetUseCaseImpl>()
                .add::<CreateDatasetFromSnapshotUseCaseImpl>()
                .add::<CommitDatasetEventUseCaseImpl>();

            database_common::NoOpDatabasePlugin::init_database_components(&mut b);

            (b.build(), listener)
        };

        let api_server = TestAPIServer::new(
            create_web_user_catalog(&base_catalog, &options),
            listener,
            options.multi_tenant,
        );

        Self {
            tempdir,
            base_catalog,
            api_server,
            options,
            time_source,
        }
    }

    pub fn api_server_addr(&self) -> String {
        self.api_server.local_addr().to_string()
    }

    fn internal_datasets_folder_path(&self) -> PathBuf {
        self.tempdir.path().join("datasets")
    }

    pub fn base_catalog(&self) -> &dill::Catalog {
        &self.base_catalog
    }
}

#[async_trait::async_trait]
impl ServerSideHarness for ServerSideLocalFsHarness {
    fn operating_account_name(&self) -> Option<AccountName> {
        if self.options.multi_tenant {
            Some(AccountName::new_unchecked(SERVER_ACCOUNT_NAME))
        } else {
            None
        }
    }

    fn cli_dataset_repository(&self) -> Arc<dyn DatasetRepository> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog);
        cli_catalog.get_one::<dyn DatasetRepository>().unwrap()
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

    fn cli_compaction_service(&self) -> Arc<dyn CompactionService> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog);
        cli_catalog.get_one::<dyn CompactionService>().unwrap()
    }

    fn dataset_url_with_scheme(&self, dataset_alias: &DatasetAlias, scheme: &str) -> Url {
        let api_server_address = self.api_server_addr();
        Url::from_str(
            if self.options.multi_tenant {
                format!(
                    "{}://{}/{}/{}",
                    scheme,
                    api_server_address,
                    if let Some(account_name) = &dataset_alias.account_name {
                        account_name.to_string()
                    } else {
                        panic!("Account name not specified in alias");
                    },
                    dataset_alias.dataset_name
                )
            } else {
                format!(
                    "{}://{}/{}",
                    scheme, api_server_address, dataset_alias.dataset_name
                )
            }
            .as_str(),
        )
        .unwrap()
    }

    fn dataset_layout(&self, dataset_handle: &DatasetHandle) -> DatasetLayout {
        let root_path = if self.options.multi_tenant {
            self.internal_datasets_folder_path()
                .join(
                    if let Some(account_name) = &dataset_handle.alias.account_name {
                        account_name.to_string()
                    } else {
                        panic!("Account name not specified in alias");
                    },
                )
                .join(dataset_handle.id.as_multibase().to_stack_string())
        } else {
            self.internal_datasets_folder_path()
                .join(dataset_handle.alias.dataset_name.clone())
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
