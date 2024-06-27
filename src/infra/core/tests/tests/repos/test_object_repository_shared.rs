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
use opendatafabric::Multihash;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_bytes(repo: &dyn ObjectRepository) {
    let hash_foo = Multihash::from_digest_sha3_256(b"foo");
    let hash_bar = Multihash::from_digest_sha3_256(b"bar");

    assert!(!repo.contains(&hash_foo).await.unwrap());
    assert_matches!(repo.get_bytes(&hash_foo).await, Err(GetError::NotFound(_)),);

    assert_eq!(
        repo.insert_bytes(b"foo", InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash_foo.clone(),
        }
    );
    assert_eq!(
        repo.insert_bytes(b"bar", InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash_bar.clone(),
        }
    );

    assert!(repo.contains(&hash_foo).await.unwrap());
    assert!(repo.contains(&hash_bar).await.unwrap());
    assert_eq!(&repo.get_bytes(&hash_foo).await.unwrap()[..], b"foo");
    assert_eq!(&repo.get_bytes(&hash_bar).await.unwrap()[..], b"bar");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete(repo: &dyn ObjectRepository) {
    let hash_foo = Multihash::from_digest_sha3_256(b"foo");

    assert_eq!(
        repo.insert_bytes(b"foo", InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash_foo.clone(),
        }
    );

    assert_eq!(&repo.get_bytes(&hash_foo).await.unwrap()[..], b"foo");

    assert_eq!(
        repo.insert_bytes(b"foo", InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash_foo.clone(),
        }
    );

    assert_eq!(&repo.get_bytes(&hash_foo).await.unwrap()[..], b"foo");

    repo.delete(&hash_foo).await.unwrap();

    assert_matches!(
        repo.get_bytes(&hash_foo).await,
        Err(GetError::NotFound(e)) if e.hash == hash_foo
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_precomputed(repo: &dyn ObjectRepository) {
    let hash_foo = Multihash::from_digest_sha3_256(b"foo");
    let hash_bar = Multihash::from_digest_sha3_256(b"bar");

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
        }
    );
    assert_eq!(&repo.get_bytes(&hash_bar).await.unwrap()[..], b"foo");
    assert_matches!(repo.get_bytes(&hash_foo).await, Err(GetError::NotFound(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_expect(repo: &dyn ObjectRepository) {
    let hash_foo = Multihash::from_digest_sha3_256(b"foo");
    let hash_bar = Multihash::from_digest_sha3_256(b"bar");

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
