// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use odf_metadata::formats::multiformats::Multibase;
use odf_metadata::*;
use odf_storage_lfs::NamedObjectRepositoryLocalFS;
use opendatafabric_storage::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_REF: &str = "test";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_basics() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(tmp_repo_dir.path()));

    assert_matches!(repo.get(TEST_REF).await, Err(GetRefError::NotFound(_)));

    repo.set(TEST_REF, &Multihash::from_digest_sha3_256(b"foo"))
        .await
        .unwrap();
    assert_eq!(
        repo.get(TEST_REF).await.unwrap(),
        Multihash::from_digest_sha3_256(b"foo")
    );

    repo.set(TEST_REF, &Multihash::from_digest_sha3_256(b"bar"))
        .await
        .unwrap();
    assert_eq!(
        repo.get(TEST_REF).await.unwrap(),
        Multihash::from_digest_sha3_256(b"bar")
    );

    repo.delete(TEST_REF).await.unwrap();
    assert_matches!(repo.get(TEST_REF).await, Err(GetRefError::NotFound(_)));

    repo.delete(TEST_REF).await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compare_and_set() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo = ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(tmp_repo_dir.path()));

    let foo = Multihash::from_digest_sha3_256(b"foo");
    let bar = Multihash::from_digest_sha3_256(b"bar");
    let baz = Multihash::from_digest_sha3_256(b"baz");

    repo.compare_and_set(TEST_REF, None, &foo).await.unwrap();
    repo.compare_and_set(TEST_REF, Some(&foo), &bar)
        .await
        .unwrap();
    assert_eq!(repo.get(TEST_REF).await.unwrap(), bar);

    assert_matches!(
        repo.compare_and_set(TEST_REF, Some(&foo), &baz).await,
        Err(CompareAndSetRefError::Mismatch(RefMismatchError { block_ref_name }))
            if block_ref_name == TEST_REF
    );
    assert_matches!(
        repo.compare_and_set(TEST_REF, None, &baz).await,
        Err(CompareAndSetRefError::Mismatch(_))
    );
    assert_eq!(repo.get(TEST_REF).await.unwrap(), bar);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compare_and_set_accepts_another_encoding_of_the_expected_hash() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let named_repo = NamedObjectRepositoryLocalFS::new(tmp_repo_dir.path());

    let foo = Multihash::from_digest_sha3_256(b"foo");
    let bar = Multihash::from_digest_sha3_256(b"bar");

    // Written by another writer in a non-default encoding
    named_repo
        .set(
            TEST_REF,
            foo.as_multibase()
                .encoding(Multibase::Base58Btc)
                .to_stack_string()
                .as_bytes(),
        )
        .await
        .unwrap();

    let repo = ReferenceRepositoryImpl::new(named_repo);
    repo.compare_and_set(TEST_REF, Some(&foo), &bar)
        .await
        .unwrap();
    assert_eq!(repo.get(TEST_REF).await.unwrap(), bar);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
