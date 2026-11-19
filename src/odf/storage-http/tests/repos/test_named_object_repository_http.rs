// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use odf_metadata::AccessError;
use odf_storage::{CompareAndSetNamedError, GetNamedError, NamedObjectRepository};
use opendatafabric_storage_http::*;
use test_utils::HttpFileServer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_basics_http() {
    let harness = HttpRepoHarness::new().await;

    assert_matches!(
        harness.repo.get("head").await,
        Err(GetNamedError::NotFound(_))
    );

    harness.write_served("head", b"foo");
    assert_eq!(&harness.repo.get("head").await.unwrap()[..], b"foo");

    harness.write_served("head", b"bar");
    assert_eq!(&harness.repo.get("head").await.unwrap()[..], b"bar");

    harness.remove_served("head");
    assert_matches!(
        harness.repo.get("head").await,
        Err(GetNamedError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compare_and_set_http_is_read_only() {
    let harness = HttpRepoHarness::new().await;
    harness.write_served("head", b"foo");

    assert_matches!(
        harness
            .repo
            .compare_and_set("head", Some(b"foo"), b"bar")
            .await,
        Err(CompareAndSetNamedError::Access(AccessError::ReadOnly(_)))
    );
    assert_eq!(&harness.repo.get("head").await.unwrap()[..], b"foo");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compare_and_set_ipfs_http_is_read_only() {
    let repo = NamedObjectRepositoryIpfsHttp::new(
        reqwest::Client::new(),
        url::Url::parse("http://localhost:8080/ipfs/").unwrap(),
    );

    assert_matches!(
        repo.compare_and_set("head", None, b"bar").await,
        Err(CompareAndSetNamedError::Access(AccessError::ReadOnly(_)))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A repository reading the files of a temporary directory over HTTP
struct HttpRepoHarness {
    repo: NamedObjectRepositoryHttp,
    tmp_repo_dir: tempfile::TempDir,
    _server_handle: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl HttpRepoHarness {
    async fn new() -> Self {
        let tmp_repo_dir = tempfile::tempdir().unwrap();
        let http_server = HttpFileServer::new(tmp_repo_dir.path()).await;
        let base_url = url::Url::parse(&format!("http://{}/", http_server.local_addr())).unwrap();
        let server_handle = tokio::spawn(http_server.run());

        Self {
            repo: NamedObjectRepositoryHttp::new(
                reqwest::Client::new(),
                base_url,
                Default::default(),
            ),
            tmp_repo_dir,
            _server_handle: server_handle,
        }
    }

    fn write_served(&self, name: &str, data: &[u8]) {
        std::fs::write(self.tmp_repo_dir.path().join(name), data).unwrap();
    }

    fn remove_served(&self, name: &str) {
        std::fs::remove_file(self.tmp_repo_dir.path().join(name)).unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
