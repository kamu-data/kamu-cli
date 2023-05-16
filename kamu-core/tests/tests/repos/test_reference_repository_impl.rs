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
use kamu::infra::*;
use opendatafabric::*;

#[tokio::test]
async fn test_basics() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(tmp_repo_dir.path()));

    assert_matches!(
        repo.get(&BlockRef::Head).await,
        Err(GetRefError::NotFound(_))
    );

    repo.set(&BlockRef::Head, &Multihash::from_digest_sha3_256(b"foo"))
        .await
        .unwrap();
    assert_eq!(
        repo.get(&BlockRef::Head).await.unwrap(),
        Multihash::from_digest_sha3_256(b"foo")
    );

    repo.set(&BlockRef::Head, &Multihash::from_digest_sha3_256(b"bar"))
        .await
        .unwrap();
    assert_eq!(
        repo.get(&BlockRef::Head).await.unwrap(),
        Multihash::from_digest_sha3_256(b"bar")
    );

    repo.delete(&BlockRef::Head).await.unwrap();
    assert_matches!(
        repo.get(&BlockRef::Head).await,
        Err(GetRefError::NotFound(_))
    );

    repo.delete(&BlockRef::Head).await.unwrap();
}
