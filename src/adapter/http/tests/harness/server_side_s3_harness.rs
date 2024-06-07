// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use dill::Component;
use event_bus::EventBus;
use kamu::domain::{
    CompactingService,
    DatasetRepository,
    InternalError,
    ObjectStoreBuilder,
    ResultIntoInternal,
    ServerUrlConfig,
    SystemTimeSource,
    SystemTimeSourceStub,
};
use kamu::testing::LocalS3Server;
use kamu::utils::s3_context::S3Context;
use kamu::{
    CompactingServiceImpl,
    DatasetLayout,
    DatasetRepositoryS3,
    DependencyGraphServiceInMemory,
    ObjectStoreBuilderLocalFs,
    ObjectStoreBuilderS3,
    ObjectStoreRegistryImpl,
};
use kamu_accounts::{AuthenticationService, MockAuthenticationService};
use opendatafabric::{AccountName, DatasetAlias, DatasetHandle};
use url::Url;

use super::{
    create_cli_user_catalog,
    create_web_user_catalog,
    server_authentication_mock,
    ServerSideHarness,
    ServerSideHarnessOptions,
    TestAPIServer,
    SERVER_ACCOUNT_NAME,
};

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub(crate) struct ServerSideS3Harness {
    s3: LocalS3Server,
    base_catalog: dill::Catalog,
    api_server: TestAPIServer,
    options: ServerSideHarnessOptions,
    time_source: SystemTimeSourceStub,
    _temp_dir: tempfile::TempDir,
}

impl ServerSideS3Harness {
    pub async fn new(options: ServerSideHarnessOptions) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = temp_dir.path().join("run");
        std::fs::create_dir(&run_info_dir).unwrap();

        let s3 = LocalS3Server::new().await;
        let s3_context = S3Context::from_url(&s3.url).await;

        let time_source = SystemTimeSourceStub::new();

        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let bind_socket = TcpListener::bind(addr).unwrap();
        let base_url_rest = format!("http://{}", bind_socket.local_addr().unwrap());

        let mut base_catalog_builder = dill::CatalogBuilder::new();
        base_catalog_builder
            .add_value(time_source.clone())
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_builder(
                DatasetRepositoryS3::builder()
                    .with_s3_context(s3_context.clone())
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryS3>()
            .add_value(server_authentication_mock())
            .bind::<dyn AuthenticationService, MockAuthenticationService>()
            .add_value(ServerUrlConfig::new_test(Some(&base_url_rest)))
            .add_builder(CompactingServiceImpl::builder().with_run_info_dir(run_info_dir.clone()))
            .bind::<dyn CompactingService, CompactingServiceImpl>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add_value(ObjectStoreBuilderS3::new(s3_context, true))
            .bind::<dyn ObjectStoreBuilder, ObjectStoreBuilderS3>();

        let base_catalog = base_catalog_builder.build();

        let api_server = TestAPIServer::new(
            create_web_user_catalog(&base_catalog, &options),
            bind_socket,
            options.multi_tenant,
        );

        Self {
            s3,
            base_catalog,
            api_server,
            options,
            time_source,
            _temp_dir: temp_dir,
        }
    }

    pub fn internal_bucket_folder_path(&self) -> PathBuf {
        self.s3.tmp_dir.path().join(&self.s3.bucket)
    }

    fn api_server_addr(&self) -> String {
        self.api_server.local_addr().to_string()
    }
}

#[async_trait::async_trait]
impl ServerSideHarness for ServerSideS3Harness {
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

    fn cli_compacting_service(&self) -> Arc<dyn CompactingService> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog);
        cli_catalog.get_one::<dyn CompactingService>().unwrap()
    }

    fn dataset_url_with_scheme(&self, dataset_alias: &DatasetAlias, scheme: &str) -> Url {
        let api_server_address = self.api_server_addr();
        Url::from_str(
            if self.options.multi_tenant {
                format!(
                    "{}://{}/{}/{}",
                    scheme,
                    api_server_address,
                    dataset_alias.account_name.as_ref().unwrap(),
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

/////////////////////////////////////////////////////////////////////////////////////////
