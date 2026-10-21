// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::time::{Duration, Instant};

use odf_metadata::*;
use odf_storage::testing::test_object_repository_shared;
use odf_storage::*;
use opendatafabric_storage_lfs::ObjectRepositoryLocalFSSha3;

use super::test_streams::{InterruptedStream, StalledStream};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_protocol() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ObjectRepositoryLocalFSSha3::new(tmp_repo_dir.path());

    assert_matches!(
        repo.protocol(),
        ObjectRepositoryProtocol::LocalFs { base_dir } if base_dir.as_path() == tmp_repo_dir.path(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_bytes() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ObjectRepositoryLocalFSSha3::new(tmp_repo_dir.path());

    test_object_repository_shared::test_insert_bytes(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_stream() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ObjectRepositoryLocalFSSha3::new(tmp_repo_dir.path());
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
        }
    );

    use tokio::io::AsyncReadExt;
    let mut stream = repo.get_stream(&hash_foobar).await.unwrap();
    let mut data = Vec::new();
    stream.read_to_end(&mut data).await.unwrap();

    assert_eq!(data, b"foobar");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_interrupted_insert_stream_leaves_no_staging_file() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ObjectRepositoryLocalFSSha3::new(tmp_repo_dir.path());

    assert_matches!(
        repo.insert_stream(
            Box::new(InterruptedStream::new(b"foo")),
            InsertOpts::default()
        )
        .await,
        Err(InsertError::Internal(_)),
    );

    assert_eq!(std::fs::read_dir(tmp_repo_dir.path()).unwrap().count(), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_cancelled_insert_stream_leaves_no_staging_file() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ObjectRepositoryLocalFSSha3::new(tmp_repo_dir.path());

    let mut insert =
        Box::pin(repo.insert_stream(Box::new(StalledStream::new(b"foo")), InsertOpts::default()));

    // Drive the write until the staging file exists, then cancel it
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::fs::read_dir(tmp_repo_dir.path()).unwrap().count() == 0 {
        tokio::select! {
            biased;
            res = &mut insert => panic!("Stalled insert completed: {res:?}"),
            () = tokio::task::yield_now() => {}
        }
        assert!(Instant::now() < deadline, "Staging file never appeared");
    }
    drop(insert);

    assert_eq!(std::fs::read_dir(tmp_repo_dir.path()).unwrap().count(), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ObjectRepositoryLocalFSSha3::new(tmp_repo_dir.path());

    test_object_repository_shared::test_delete(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_precomputed() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ObjectRepositoryLocalFSSha3::new(tmp_repo_dir.path());

    test_object_repository_shared::test_insert_precomputed(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_expect() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ObjectRepositoryLocalFSSha3::new(tmp_repo_dir.path());

    test_object_repository_shared::test_insert_expect(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
