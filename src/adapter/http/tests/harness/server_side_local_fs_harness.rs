// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::InMemoryTransactionManager;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::CacheDir;
use kamu_accounts::Account;
use kamu_accounts_inmem::InMemoryAccountQuotaEventStore;
use kamu_accounts_services::AccountQuotaServiceImpl;
use kamu_core::TenancyConfig;
use kamu_datasets::*;
use kamu_datasets_services::testing::{
    FailingDatasetChangeListener,
    RacingAppendDatasetMetadataBatchUseCase,
    RacingDatasetReferenceUpdater,
};
use kamu_datasets_services::*;
use messaging_outbox::register_message_dispatcher;
use tempfile::TempDir;
use time_source::SystemTimeSourceStub;

use super::{
    LocalFsDatasetFixture,
    ServerSideDatasetFixture,
    ServerSideHarness,
    ServerSideHarnessOptions,
    TestAPIServer,
    UnreferencedBlocks,
    bind_api_listener,
    make_server_account,
    register_common_server_components,
    start_api_server,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

        let account = make_server_account(options.tenancy_config);
        let time_source = SystemTimeSourceStub::new();
        let (listener, base_url_rest) = bind_api_listener().await;

        let base_catalog = {
            let mut b = match &options.base_catalog {
                None => dill::CatalogBuilder::new(),
                Some(c) => dill::CatalogBuilder::new_chained(c),
            };

            register_common_server_components(
                &mut b,
                &options,
                &account,
                &time_source,
                run_info_dir,
                &base_url_rest,
            );

            b.add_value(CacheDir::new(cache_dir))
                .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                    datasets_dir,
                ))
                .add::<DatasetLfsBuilderDatabaseBackedImpl>()
                .add::<RacingAppendDatasetMetadataBatchUseCase>()
                .add_value(RacingDatasetReferenceUpdater::default())
                .bind::<dyn DatasetReferenceUpdater, RacingDatasetReferenceUpdater>()
                .add::<InMemoryAccountQuotaEventStore>()
                .add::<AccountQuotaServiceImpl>()
                .add::<DatasetStatisticsServiceImpl>()
                .add::<AccountQuotaCheckerStorageImpl>()
                .add_value(QuotaDefaultsConfig::default());

            // A failed transaction rolls back the dataset repositories every read goes
            // through; storage files stay, such as `refs/head` written by an immediate
            // consumer
            b.add::<InMemoryTransactionManager>();
            b.add::<FailingDatasetChangeListener>();

            register_message_dispatcher::<DatasetExternallyChangedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
            );

            b.build()
        };

        let api_server = start_api_server(&base_catalog, listener, &options).await;

        Self {
            tempdir,
            base_catalog,
            api_server,
            options,
            time_source,
            account,
        }
    }

    fn internal_datasets_folder_path(&self) -> std::path::PathBuf {
        self.tempdir.path().join("datasets")
    }

    /// Arms the next `HEAD` commit on the server to first move `HEAD` to
    /// `concurrent_head`, the way a concurrent writer would
    pub fn move_reference_before_next_update(&self, concurrent_head: odf::Multihash) {
        self.base_catalog
            .get_one::<RacingDatasetReferenceUpdater>()
            .unwrap()
            .move_reference_before_next_update(&self.cli_catalog(), concurrent_head);
    }

    /// Arms the next metadata batch append on the server to first move `HEAD`
    /// to `concurrent_head`, before the commit decides which head it expects
    pub fn move_head_before_next_append(&self, concurrent_head: odf::Multihash) {
        self.base_catalog
            .get_one::<RacingAppendDatasetMetadataBatchUseCase>()
            .unwrap()
            .move_head_before_next_append(&self.cli_catalog(), concurrent_head);
    }

    /// The server's dataset change listener, which a test arms to fail. Taken
    /// before `api_server_run`, which consumes the harness
    pub fn dataset_change_listener(&self) -> Arc<FailingDatasetChangeListener> {
        self.base_catalog.get_one().unwrap()
    }

    pub async fn append_unreferenced_block(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> odf::Multihash {
        UnreferencedBlocks::new(self.cli_dataset_registry())
            .append(dataset_handle)
            .await
    }
}

#[async_trait::async_trait(?Send)]
impl ServerSideHarness for ServerSideLocalFsHarness {
    fn base_catalog(&self) -> &dill::Catalog {
        &self.base_catalog
    }

    fn tenancy_config(&self) -> TenancyConfig {
        self.options.tenancy_config
    }

    fn api_server(&self) -> &TestAPIServer {
        &self.api_server
    }

    fn server_account(&self) -> &Account {
        &self.account
    }

    fn system_time_source(&self) -> &SystemTimeSourceStub {
        &self.time_source
    }

    fn dataset_fixture(&self) -> Arc<dyn ServerSideDatasetFixture> {
        Arc::new(LocalFsDatasetFixture::new(
            self.internal_datasets_folder_path(),
        ))
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
