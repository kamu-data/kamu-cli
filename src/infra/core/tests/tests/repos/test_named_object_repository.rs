// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu::domain::*;
use kamu::testing::{HttpFileServer, LocalS3Server};
use kamu::*;

////////////////////////////////////////////////////////////////////////////////

async fn test_named_repository_operations(repo: &dyn NamedObjectRepository) {
    assert_matches!(repo.get("head").await, Err(GetNamedError::NotFound(_)));

    repo.set("head", b"foo").await.unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"foo");

    repo.set("head", b"bar").await.unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"bar");

    repo.delete("head").await.unwrap();
    assert_matches!(repo.get("head").await, Err(GetNamedError::NotFound(_)));
}

////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_basics_in_memory() {
    let repo = NamedObjectRepositoryInMemory::new();
    test_named_repository_operations(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_basics_local_fs() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let repo = NamedObjectRepositoryLocalFS::new(tmp_dir.path());

    test_named_repository_operations(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_basics_s3() {
    let s3 = LocalS3Server::new().await;
    let s3_context = kamu::utils::s3_context::S3Context::from_url(&s3.url).await;
    let repo = NamedObjectRepositoryS3::new(s3_context);

    test_named_repository_operations(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_basics_http() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let http_server = HttpFileServer::new(tmp_repo_dir.path());
    let base_url = url::Url::parse(&format!("http://{}/", http_server.local_addr())).unwrap();
    let _srv_handle = tokio::spawn(http_server.run());
    let repo = NamedObjectRepositoryHttp::new(reqwest::Client::new(), base_url, Default::default());

    assert_matches!(repo.get("head").await, Err(GetNamedError::NotFound(_)));

    std::fs::write(tmp_repo_dir.path().join("head"), b"foo").unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"foo");

    std::fs::write(tmp_repo_dir.path().join("head"), b"bar").unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"bar");

    std::fs::remove_file(tmp_repo_dir.path().join("head")).unwrap();
    assert_matches!(repo.get("head").await, Err(GetNamedError::NotFound(_)));
}

////////////////////////////////////////////////////////////////////////////////
