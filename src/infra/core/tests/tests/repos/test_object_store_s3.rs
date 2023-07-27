// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use datafusion::execution::object_store::ObjectStoreRegistry;
use kamu::testing::MinioServer;
use kamu::utils::s3_context::S3Context;
use kamu::*;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
struct S3 {
    tmp_dir: tempfile::TempDir,
    minio: MinioServer,
    bucket: String,
    url: Url,
}

async fn run_s3_server() -> S3 {
    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    std::env::set_var("AWS_ACCESS_KEY_ID", access_key);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", secret_key);

    let tmp_dir = tempfile::tempdir().unwrap();
    let bucket = "test-bucket".to_string();
    std::fs::create_dir(tmp_dir.path().join(&bucket)).unwrap();

    let minio = MinioServer::new(tmp_dir.path(), access_key, secret_key).await;

    let url = Url::parse(&format!(
        "s3+http://{}:{}/{}",
        minio.address, minio.host_port, bucket
    ))
    .unwrap();

    S3 {
        tmp_dir,
        minio,
        bucket,
        url,
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_auth_explicit_endpoint() {
    let s3 = run_s3_server().await;
    let s3_ctx = S3Context::from_url(&s3.url).await;

    let store_url = Url::parse(&format!("s3://{}/", s3.bucket)).unwrap();
    let reg = ObjectStoreRegistryImpl::new(vec![Arc::new(ObjectStoreBuilderS3::new(s3_ctx, true))]);
    let store = reg.get_store(&store_url).unwrap();

    let path = object_store::path::Path::parse("asdf").unwrap();
    store.put(&path, Bytes::from_static(b"test")).await.unwrap();

    let store = reg.get_store(&store_url).unwrap();
    let res = store.get(&path).await.unwrap();
    let data = res.bytes().await.unwrap();
    assert_eq!(&data[..], b"test");
}
