// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::{
    domain::{AppendOpts, DatasetRepository, GetDatasetError},
    infra::{utils::s3_context::S3Context, DatasetRepositoryS3},
    testing::MetadataFactory,
};
use opendatafabric::{DatasetKind, DatasetName};
use url::Url;

use crate::utils::MinioServer;
use std::assert_matches::assert_matches;

#[allow(dead_code)]
struct S3 {
    tmp_dir: tempfile::TempDir,
    minio: MinioServer,
    url: Url,
}

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
    }
}

#[tokio::test]
async fn test_create_dataset() {
    let s3 = run_s3_server();
    let (endpoint, bucket, key_prefix) = S3Context::split_url(&s3.url);
    let repo = DatasetRepositoryS3::new(
        S3Context::from_items(endpoint.clone(), bucket, key_prefix),
        endpoint.unwrap(),
    );

    let dataset_name = DatasetName::new_unchecked("foo");

    assert_matches!(
        repo.get_dataset(&dataset_name.as_local_ref())
            .await
            .err()
            .unwrap(),
        GetDatasetError::NotFound(_)
    );

    let builder = repo.create_dataset(&dataset_name).await.unwrap();
    let chain = builder.as_dataset().as_metadata_chain();

    chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    let hdl = builder.finish().await.unwrap();
    assert_eq!(hdl.name, dataset_name);

    assert!(repo.get_dataset(&dataset_name.as_local_ref()).await.is_ok());
}
