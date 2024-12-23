// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use odf_storage::{GetNamedError, NamedObjectRepository};
use opendatafabric_storage_http::*;
use test_utils::HttpFileServer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_basics_http() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let http_server = HttpFileServer::new(tmp_repo_dir.path()).await;
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
