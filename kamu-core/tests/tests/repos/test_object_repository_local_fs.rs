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
use opendatafabric::Multihash;

use super::test_object_repository_shared;

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_bytes() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(tmp_repo_dir.path());
    test_object_repository_shared::test_insert_bytes(&repo).await;
}

#[tokio::test]
async fn test_insert_stream() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(tmp_repo_dir.path());
    let hash_foobar = Multihash::from_digest_sha3_256(b"foobar");

    assert_matches!(
        repo.get_stream(&hash_foobar).await.err().unwrap(),
        GetError::NotFound(_),
    );

    let cursor = std::io::Cursor::new(b"foobar");
    assert_eq!(
        repo.insert_stream(Box::new(cursor), InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash_foobar.clone(),
            already_existed: false
        }
    );

    use tokio::io::AsyncReadExt;
    let mut stream = repo.get_stream(&hash_foobar).await.unwrap();
    let mut data = Vec::new();
    stream.read_to_end(&mut data).await.unwrap();

    assert_eq!(data, b"foobar");
}

#[tokio::test]
async fn test_delete() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(tmp_repo_dir.path());
    test_object_repository_shared::test_delete(&repo, true).await;
}

#[tokio::test]
async fn test_insert_precomputed() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(tmp_repo_dir.path());
    test_object_repository_shared::test_insert_precomputed(&repo).await;
}

#[tokio::test]
async fn test_insert_expect() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(tmp_repo_dir.path());
    test_object_repository_shared::test_insert_expect(&repo).await;
}

/////////////////////////////////////////////////////////////////////////////////////////
