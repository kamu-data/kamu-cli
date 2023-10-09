// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use dill::builder_for;
use kamu::domain::{
    auth,
    DatasetRepository,
    InternalError,
    ResultIntoInternal,
    SystemTimeSource,
    SystemTimeSourceDefault,
};
use kamu::testing::MockAuthenticationService;
use kamu::{DatasetLayout, DatasetRepositoryLocalFs};
use opendatafabric::{AccountName, DatasetAlias, DatasetHandle};
use tempfile::TempDir;
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
pub(crate) struct ServerSideLocalFsHarness {
    tempdir: TempDir,
    base_catalog: dill::Catalog,
    api_server: TestAPIServer,
    options: ServerSideHarnessOptions,
}

impl ServerSideLocalFsHarness {
    pub async fn new(options: ServerSideHarnessOptions) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let mut base_catalog_builder = dill::CatalogBuilder::new();
        base_catalog_builder
            .add::<SystemTimeSourceDefault>()
            .bind::<dyn SystemTimeSource, SystemTimeSourceDefault>()
            .add_builder(
                builder_for::<DatasetRepositoryLocalFs>()
                    .with_root(datasets_dir)
                    .with_multi_tenant(options.multi_tenant),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(server_authentication_mock())
            .bind::<dyn auth::AuthenticationService, MockAuthenticationService>();

        let base_catalog = base_catalog_builder.build();

        let api_server = TestAPIServer::new(
            create_web_user_catalog(&base_catalog, &options),
            Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            None,
            options.multi_tenant,
        );

        Self {
            tempdir,
            base_catalog,
            api_server,
            options,
        }
    }

    fn api_server_addr(&self) -> String {
        self.api_server.local_addr().to_string()
    }

    fn internal_datasets_folder_path(&self) -> PathBuf {
        self.tempdir.path().join("datasets")
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

    fn dataset_url(&self, dataset_alias: &DatasetAlias) -> Url {
        let api_server_address = self.api_server_addr();
        Url::from_str(
            if self.options.multi_tenant {
                format!(
                    "odf+http://{}/{}/{}",
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
                    "odf+http://{}/{}",
                    api_server_address, dataset_alias.dataset_name
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
                .join(dataset_handle.id.cid.to_string())
        } else {
            self.internal_datasets_folder_path()
                .join(dataset_handle.alias.dataset_name.clone())
        };
        DatasetLayout::new(root_path.as_path())
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
