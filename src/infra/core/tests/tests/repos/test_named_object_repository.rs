// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::repos::named_object_repository::GetError;
use kamu::domain::*;
use kamu::infra::*;
use reqwest::Url;

use crate::utils::HttpFileServer;
use crate::utils::MinioServer;
use std::assert_matches::assert_matches;

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_named_repository_operations(repo: &dyn NamedObjectRepository) {
    assert_matches!(repo.get("head").await, Err(GetError::NotFound(_)));

    repo.set("head", b"foo").await.unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"foo");

    repo.set("head", b"bar").await.unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"bar");

    repo.delete("head").await.unwrap();
    assert_matches!(repo.get("head").await, Err(GetError::NotFound(_)));
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_basics_in_memory() {
    let repo = NamedObjectRepositoryInMemory::new();
    test_named_repository_operations(&repo).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_basics_local_fs() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let repo = NamedObjectRepositoryLocalFS::new(tmp_dir.path());

    test_named_repository_operations(&repo).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_basics_s3() {
    let s3_srv = run_s3_server();
    let repo = NamedObjectRepositoryS3::from_url(&s3_srv.url);

    test_named_repository_operations(&repo).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_basics_http() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let http_server = HttpFileServer::new(tmp_repo_dir.path());
    let base_url = url::Url::parse(&format!("http://{}/", http_server.local_addr())).unwrap();
    let _srv_handle = tokio::spawn(http_server.run());
    let repo = NamedObjectRepositoryHttp::new(reqwest::Client::new(), base_url);

    assert_matches!(repo.get("head").await, Err(GetError::NotFound(_)));

    std::fs::write(tmp_repo_dir.path().join("head"), b"foo").unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"foo");

    std::fs::write(tmp_repo_dir.path().join("head"), b"bar").unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"bar");

    std::fs::remove_file(tmp_repo_dir.path().join("head")).unwrap();
    assert_matches!(repo.get("head").await, Err(GetError::NotFound(_)));
}

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////
