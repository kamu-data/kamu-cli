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

use std::assert_matches::assert_matches;

#[tokio::test]
async fn test_insert_bytes() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let hash_foo = Multihash::from_digest_sha3_256(b"foo");
    let hash_bar = Multihash::from_digest_sha3_256(b"bar");

    let repo = ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(tmp_repo_dir.path());

    assert!(!repo.contains(&hash_foo).await.unwrap());
    assert_matches!(repo.get_bytes(&hash_foo).await, Err(GetError::NotFound(_)),);

    assert_eq!(
        repo.insert_bytes(b"foo", InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash_foo.clone(),
            already_existed: false
        }
    );
    assert_eq!(
        repo.insert_bytes(b"bar", InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash_bar.clone(),
            already_existed: false
        }
    );

    assert!(repo.contains(&hash_foo).await.unwrap());
    assert!(repo.contains(&hash_bar).await.unwrap());
    assert_eq!(&repo.get_bytes(&hash_foo).await.unwrap()[..], b"foo");
    assert_eq!(&repo.get_bytes(&hash_bar).await.unwrap()[..], b"bar");
}

#[tokio::test]
async fn test_insert_stream() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let hash_foobar = Multihash::from_digest_sha3_256(b"foobar");

    let repo = ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(tmp_repo_dir.path());

    assert_matches!(
        repo.get_stream(&hash_foobar).await.err().unwrap(),
        GetError::NotFound(_),
    );

    let mut cursor = std::io::Cursor::new(b"foobar");
    assert_eq!(
        repo.insert_stream(&mut cursor, InsertOpts::default())
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
    let hash_foo = Multihash::from_digest_sha3_256(b"foo");

    let repo = ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(tmp_repo_dir.path());
    assert_eq!(
        repo.insert_bytes(b"foo", InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash_foo.clone(),
            already_existed: false
        }
    );

    assert_eq!(&repo.get_bytes(&hash_foo).await.unwrap()[..], b"foo");

    assert_eq!(
        repo.insert_bytes(b"foo", InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash_foo.clone(),
            already_existed: true
        }
    );

    assert_eq!(&repo.get_bytes(&hash_foo).await.unwrap()[..], b"foo");

    repo.delete(&hash_foo).await.unwrap();

    assert_matches!(
        repo.get_bytes(&hash_foo).await,
        Err(GetError::NotFound(e)) if e.hash == hash_foo
    );
}

#[tokio::test]
async fn test_insert_precomputed() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let hash_foo = Multihash::from_digest_sha3_256(b"foo");
    let hash_bar = Multihash::from_digest_sha3_256(b"bar");

    let repo = ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(tmp_repo_dir.path());

    assert_eq!(
        repo.insert_bytes(
            b"foo",
            InsertOpts {
                precomputed_hash: Some(&hash_bar),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        InsertResult {
            hash: hash_bar.clone(),
            already_existed: false
        }
    );
    assert_eq!(&repo.get_bytes(&hash_bar).await.unwrap()[..], b"foo");
    assert_matches!(repo.get_bytes(&hash_foo).await, Err(GetError::NotFound(_)));
}

#[tokio::test]
async fn test_insert_expect() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let hash_foo = Multihash::from_digest_sha3_256(b"foo");
    let hash_bar = Multihash::from_digest_sha3_256(b"bar");

    let repo = ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(tmp_repo_dir.path());

    assert_eq!(
        repo.insert_bytes(
            b"foo",
            InsertOpts {
                expected_hash: Some(&hash_foo),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        InsertResult {
            hash: hash_foo.clone(),
            already_existed: false
        }
    );
    assert_eq!(&repo.get_bytes(&hash_foo).await.unwrap()[..], b"foo");

    assert_matches!(
        repo.insert_bytes(
            b"bar",
            InsertOpts {
                expected_hash: Some(&hash_foo),
                ..Default::default()
            },
        )
        .await,
        Err(InsertError::HashMismatch(HashMismatchError {
            expected,
            actual,
        })) if expected == hash_foo && actual == hash_bar
    );
    assert_matches!(repo.get_bytes(&hash_bar).await, Err(GetError::NotFound(_)));
}
