// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
use kamu::infra::*;
use opendatafabric::*;

use crate::utils::HttpFileServer;
use std::assert_matches::assert_matches;

#[tokio::test]
async fn test_read_only() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let http_server = HttpFileServer::new(tmp_repo_dir.path());
    let base_url = url::Url::parse(&format!("http://{}/", http_server.local_addr())).unwrap();
    let _srv_handle = tokio::spawn(http_server.run());
    let repo = ObjectRepositoryHttp::new(reqwest::Client::new(), base_url);

    assert_matches!(
        repo.insert_bytes(b"foo", InsertOpts::default()).await,
        Err(InsertError::Access(AccessError::ReadOnly(_)))
    );
}

#[tokio::test]
async fn test_bytes() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let http_server = HttpFileServer::new(tmp_repo_dir.path());
    let base_url = url::Url::parse(&format!("http://{}/", http_server.local_addr())).unwrap();
    let _srv_handle = tokio::spawn(http_server.run());
    let repo = ObjectRepositoryHttp::new(reqwest::Client::new(), base_url);

    let hash_foo = Multihash::from_digest_sha3_256(b"foo");
    let hash_bar = Multihash::from_digest_sha3_256(b"bar");

    assert!(!repo.contains(&hash_foo).await.unwrap());
    assert_matches!(repo.get_bytes(&hash_foo).await, Err(GetError::NotFound(_)),);

    std::fs::write(
        tmp_repo_dir.path().join(hash_foo.to_multibase_string()),
        b"foo",
    )
    .unwrap();

    std::fs::write(
        tmp_repo_dir.path().join(hash_bar.to_multibase_string()),
        b"bar",
    )
    .unwrap();

    assert!(repo.contains(&hash_foo).await.unwrap());
    assert!(repo.contains(&hash_bar).await.unwrap());
    assert_eq!(&repo.get_bytes(&hash_foo).await.unwrap()[..], b"foo");
    assert_eq!(&repo.get_bytes(&hash_bar).await.unwrap()[..], b"bar");
}

#[tokio::test]
async fn test_stream() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let http_server = HttpFileServer::new(tmp_repo_dir.path());
    let base_url = url::Url::parse(&format!("http://{}/", http_server.local_addr())).unwrap();
    let _srv_handle = tokio::spawn(http_server.run());
    let repo = ObjectRepositoryHttp::new(reqwest::Client::new(), base_url);

    let hash_foobar = Multihash::from_digest_sha3_256(b"foobar");

    assert_matches!(
        repo.get_stream(&hash_foobar).await.err().unwrap(),
        GetError::NotFound(_),
    );

    std::fs::write(
        tmp_repo_dir.path().join(hash_foobar.to_multibase_string()),
        b"foobar",
    )
    .unwrap();

    use tokio::io::AsyncReadExt;
    let mut stream = repo.get_stream(&hash_foobar).await.unwrap();
    let mut data = Vec::new();
    stream.read_to_end(&mut data).await.unwrap();

    assert_eq!(data, b"foobar");
}
