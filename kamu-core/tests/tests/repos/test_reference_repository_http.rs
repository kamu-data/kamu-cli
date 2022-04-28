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
async fn test_basics() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let http_server = HttpFileServer::new(tmp_repo_dir.path());
    let base_url = url::Url::parse(&format!("http://{}/", http_server.local_addr())).unwrap();
    let _srv_handle = tokio::spawn(http_server.run());
    let repo = ReferenceRepositoryHttp::new(reqwest::Client::new(), base_url);

    assert_matches!(
        repo.get(&BlockRef::Head).await,
        Err(GetRefError::NotFound(_))
    );

    std::fs::write(
        tmp_repo_dir.path().join("head"),
        Multihash::from_digest_sha3_256(b"foo").to_multibase_string(),
    )
    .unwrap();
    assert_eq!(
        repo.get(&BlockRef::Head).await.unwrap(),
        Multihash::from_digest_sha3_256(b"foo")
    );

    std::fs::write(
        tmp_repo_dir.path().join("head"),
        Multihash::from_digest_sha3_256(b"bar").to_multibase_string(),
    )
    .unwrap();
    assert_eq!(
        repo.get(&BlockRef::Head).await.unwrap(),
        Multihash::from_digest_sha3_256(b"bar")
    );

    std::fs::remove_file(tmp_repo_dir.path().join("head")).unwrap();
    assert_matches!(
        repo.get(&BlockRef::Head).await,
        Err(GetRefError::NotFound(_))
    );
}
