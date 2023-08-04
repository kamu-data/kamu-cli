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

use kamu::domain::{CurrentAccountSubject, DatasetRepository, InternalError, ResultIntoInternal};
use kamu::testing::LocalS3Server;
use kamu::utils::s3_context::S3Context;
use kamu::{DatasetLayout, DatasetRepositoryS3};
use opendatafabric::DatasetHandle;
use url::Url;

use super::{ServerSideHarness, TestAPIServer};

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub struct ServerSideS3Harness {
    s3: LocalS3Server,
    catalog: dill::Catalog,
    api_server: TestAPIServer,
}

impl ServerSideS3Harness {
    pub async fn new() -> Self {
        let s3 = LocalS3Server::new().await;
        let catalog = dill::CatalogBuilder::new()
            .add_value(s3_repo(&s3).await)
            .bind::<dyn DatasetRepository, DatasetRepositoryS3>()
            .build();

        let api_server = TestAPIServer::new(
            catalog.clone(),
            Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            None,
        );

        Self {
            s3,
            catalog,
            api_server,
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
    fn dataset_repository(&self) -> Arc<dyn DatasetRepository> {
        self.catalog.get_one::<dyn DatasetRepository>().unwrap()
    }

    fn dataset_url(&self, dataset_name: &str) -> Url {
        let api_server_address = self.api_server_addr();
        Url::from_str(format!("odf+http://{}/{}", api_server_address, dataset_name).as_str())
            .unwrap()
    }

    fn dataset_layout(&self, dataset_handle: &DatasetHandle) -> DatasetLayout {
        DatasetLayout::new(
            self.internal_bucket_folder_path()
                .join(dataset_handle.id.cid.to_string())
                .as_path(),
        )
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn s3_repo(s3: &LocalS3Server) -> DatasetRepositoryS3 {
    let s3_context = S3Context::from_url(&s3.url).await;
    DatasetRepositoryS3::new(
        s3_context,
        Arc::new(CurrentAccountSubject::new_test()),
        false,
    )
}

/////////////////////////////////////////////////////////////////////////////////////////
