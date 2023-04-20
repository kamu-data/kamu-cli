// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use kamu::{
    domain::{DatasetRepository, InternalError, ResultIntoInternal},
    infra::{utils::s3_context::S3Context, DatasetLayout, DatasetRepositoryS3},
    testing::MinioServer,
};
use url::Url;

use super::{ServerSideHarness, TestAPIServer};

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub struct S3 {
    tmp_dir: tempfile::TempDir,
    minio: MinioServer,
    url: Url,
    bucket_name: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

fn run_s3_server() -> S3 {
    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    std::env::set_var("AWS_ACCESS_KEY_ID", access_key);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", secret_key);

    let tmp_dir = tempfile::tempdir().unwrap();
    let bucket = "test-bucket";
    std::fs::create_dir(tmp_dir.path().join(bucket)).unwrap();

    let minio = MinioServer::new(tmp_dir.path(), access_key, secret_key);

    let url = Url::parse(&format!(
        "s3+http://{}:{}/{}",
        minio.address, minio.host_port, bucket
    ))
    .unwrap();

    S3 {
        tmp_dir,
        minio,
        url,
        bucket_name: String::from(bucket),
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub struct ServerSideS3Harness {
    s3: S3,
    catalog: dill::Catalog,
    api_server: TestAPIServer,
}

impl ServerSideS3Harness {
    pub async fn new() -> Self {
        let s3 = run_s3_server();
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
        self.s3.tmp_dir.path().join(self.s3.bucket_name.clone())
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

    fn dataset_layout(&self, dataset_name: &str) -> DatasetLayout {
        DatasetLayout::new(
            self.internal_bucket_folder_path()
                .join(dataset_name)
                .as_path(),
        )
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn s3_repo(s3: &S3) -> DatasetRepositoryS3 {
    let (endpoint, bucket, key_prefix) = S3Context::split_url(&s3.url);
    DatasetRepositoryS3::new(
        S3Context::from_items(endpoint.clone(), bucket, key_prefix).await,
        endpoint.unwrap(),
    )
}

/////////////////////////////////////////////////////////////////////////////////////////
