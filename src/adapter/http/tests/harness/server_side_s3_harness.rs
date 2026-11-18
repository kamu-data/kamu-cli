// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu::*;
use kamu_accounts::Account;
use kamu_accounts_services::ModifyAccountPasswordUseCaseImpl;
use kamu_core::TenancyConfig;
use kamu_datasets_services::{
    AppendDatasetMetadataBatchUseCaseImpl,
    DatasetReferenceUpdaterImpl,
    DatasetS3BuilderDatabaseBackedImpl,
};
use test_utils::LocalS3Server;
use time_source::SystemTimeSourceStub;

use super::{
    S3DatasetFixture,
    ServerSideDatasetFixture,
    ServerSideHarness,
    ServerSideHarnessOptions,
    TestAPIServer,
    bind_api_listener,
    make_server_account,
    register_common_server_components,
    start_api_server,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        s3.set_credentials_env_vars();

        let account = make_server_account(options.tenancy_config);
        let time_source = SystemTimeSourceStub::new();
        let (listener, base_url_rest) = bind_api_listener().await;

        let base_catalog = {
            let mut b = dill::CatalogBuilder::new();

            register_common_server_components(
                &mut b,
                &options,
                &account,
                &time_source,
                run_info_dir,
                &base_url_rest,
            );

            b.add_builder(odf::dataset::DatasetStorageUnitS3::builder(s3.ctx.clone()))
                .add::<DatasetS3BuilderDatabaseBackedImpl>()
                .add_builder(ObjectStoreBuilderS3::builder(
                    s3.ctx.clone(),
                    true,
                    Some(s3.access_key.clone()),
                    Some(s3.secret_key.clone()),
                ))
                .add::<AppendDatasetMetadataBatchUseCaseImpl>()
                .add::<DatasetReferenceUpdaterImpl>()
                .add::<ModifyAccountPasswordUseCaseImpl>();

            database_common::NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        let api_server = start_api_server(&base_catalog, listener, &options).await;

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

    pub fn bucket(&self) -> &str {
        &self.s3.bucket
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl ServerSideHarness for ServerSideS3Harness {
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
        Arc::new(S3DatasetFixture::new(self.s3.ctx.clone()))
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
