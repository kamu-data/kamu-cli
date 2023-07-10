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
use kamu::domain::{CurrentAccountSubject, DatasetRepository, InternalError, ResultIntoInternal};
use kamu::{DatasetLayout, DatasetRepositoryLocalFs, WorkspaceLayout};
use tempfile::TempDir;
use url::Url;

use super::{ServerSideHarness, TestAPIServer};

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub struct ServerSideLocalFsHarness {
    tempdir: TempDir,
    catalog: dill::Catalog,
    api_server: TestAPIServer,
}

impl ServerSideLocalFsHarness {
    pub async fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let workspace_layout = WorkspaceLayout::create(tempdir.path(), false).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add_builder(
                builder_for::<DatasetRepositoryLocalFs>()
                    .with_root(workspace_layout.datasets_dir.clone())
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .build();

        let api_server = TestAPIServer::new(
            catalog.clone(),
            Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            None,
        );

        Self {
            tempdir,
            catalog,
            api_server,
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
    fn dataset_repository(&self) -> Arc<dyn DatasetRepository> {
        self.catalog.get_one::<dyn DatasetRepository>().unwrap()
    }

    fn dataset_url(&self, dataset_name: &str) -> Url {
        let api_server_address = self.api_server_addr();
        Url::from_str(format!("odf+http://{}/{}", api_server_address, dataset_name).as_str())
            .unwrap()
    }

    fn dataset_layout(&self, dataset_name: &str) -> DatasetLayout {
        DatasetLayout::new(
            self.internal_datasets_folder_path()
                .join(dataset_name)
                .as_path(),
        )
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
