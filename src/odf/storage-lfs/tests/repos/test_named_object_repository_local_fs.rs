// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use odf_storage::testing::test_named_object_repository_shared;
use odf_storage::*;
use opendatafabric_storage_lfs::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_basics_local_fs() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let repo = NamedObjectRepositoryLocalFS::new(tmp_dir.path());

    test_named_object_repository_shared::test_named_repository_operations(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compare_and_set_local_fs() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let repo = NamedObjectRepositoryLocalFS::new(tmp_dir.path());

    test_named_object_repository_shared::test_named_repository_compare_and_set(&repo).await;
    test_named_object_repository_shared::test_named_repository_compare_and_set_race(&repo).await;

    // The lock file goes with the write or the mismatch
    let entries: Vec<_> = std::fs::read_dir(tmp_dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(entries, Vec::<String>::new());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compare_and_set_creates_missing_root() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let repo = NamedObjectRepositoryLocalFS::new(tmp_dir.path().join("refs"));

    repo.compare_and_set("head", None, b"foo").await.unwrap();

    assert_eq!(&repo.get("head").await.unwrap()[..], b"foo");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_failed_set_leaves_no_staging_file() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let repo = NamedObjectRepositoryLocalFS::new(tmp_dir.path());

    // A non-empty directory under the target name makes the final rename fail
    std::fs::create_dir_all(tmp_dir.path().join("head").join("blocker")).unwrap();

    assert_matches!(
        repo.set("head", b"foo").await,
        Err(SetNamedError::Internal(_))
    );

    let entries: Vec<_> = std::fs::read_dir(tmp_dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(entries, ["head"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compare_and_set_reports_a_lock_left_behind() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let repo = NamedObjectRepositoryLocalFS::new(tmp_dir.path());
    repo.set("head", b"foo").await.unwrap();

    // An interrupted writer
    std::fs::write(tmp_dir.path().join("head.lock"), b"").unwrap();

    assert_matches!(
        repo.compare_and_set("head", Some(b"foo"), b"bar").await,
        Err(CompareAndSetNamedError::Internal(e)) if format!("{e:?}").contains("head.lock")
    );
    assert_eq!(&repo.get("head").await.unwrap()[..], b"foo");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
