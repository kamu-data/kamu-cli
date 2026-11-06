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
    let harness = RefRepoHarness::new();
    let repo = &harness.repo;

    let foo = Multihash::from_digest_sha3_256(b"foo");
    let bar = Multihash::from_digest_sha3_256(b"bar");
    let baz = Multihash::from_digest_sha3_256(b"baz");

    assert_matches!(
        repo.compare_and_set(TEST_REF, Some(&foo), &bar).await,
        Err(CompareAndSetRefError::Mismatch(RefMismatchError { block_ref_name }))
            if block_ref_name == TEST_REF,
        "missing ref expecting a hash"
    );
    assert_matches!(
        repo.get(TEST_REF).await,
        Err(GetRefError::NotFound(_)),
        "missing ref after a mismatch"
    );

    repo.compare_and_set(TEST_REF, None, &foo).await.unwrap();
    repo.compare_and_set(TEST_REF, Some(&foo), &bar)
        .await
        .unwrap();
    assert_eq!(repo.get(TEST_REF).await.unwrap(), bar);

    assert_matches!(
        repo.compare_and_set(TEST_REF, Some(&foo), &baz).await,
        Err(CompareAndSetRefError::Mismatch(RefMismatchError { block_ref_name }))
            if block_ref_name == TEST_REF,
        "stored ref expecting a stale hash"
    );
    assert_matches!(
        repo.compare_and_set(TEST_REF, None, &baz).await,
        Err(CompareAndSetRefError::Mismatch(_)),
        "stored ref expecting none"
    );
    assert_eq!(repo.get(TEST_REF).await.unwrap(), bar);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compare_and_set_accepts_another_encoding_of_the_expected_hash() {
    let harness = RefRepoHarness::new();

    let foo = Multihash::from_digest_sha3_256(b"foo");
    let bar = Multihash::from_digest_sha3_256(b"bar");

    // Written by another writer in a non-default encoding
    harness
        .store_raw(
            foo.as_multibase()
                .encoding(Multibase::Base58Btc)
                .to_stack_string()
                .as_bytes(),
        )
        .await;

    harness
        .repo
        .compare_and_set(TEST_REF, Some(&foo), &bar)
        .await
        .unwrap();
    assert_eq!(harness.repo.get(TEST_REF).await.unwrap(), bar);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compare_and_set_rejects_a_stored_ref_that_is_not_a_hash() {
    let harness = RefRepoHarness::new();
    harness.store_raw(b"not-a-hash").await;

    assert_matches!(
        harness
            .repo
            .compare_and_set(
                TEST_REF,
                Some(&Multihash::from_digest_sha3_256(b"foo")),
                &Multihash::from_digest_sha3_256(b"bar"),
            )
            .await,
        Err(CompareAndSetRefError::Internal(_))
    );
    assert_eq!(harness.stored_raw().await, b"not-a-hash");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compare_and_set_passes_storage_errors_through() {
    let foo = Multihash::from_digest_sha3_256(b"foo");

    let repo = RefRepoHarness::repo_failing_cas_with(|| CompareAndSetNamedError::NotSupported);
    assert_matches!(
        repo.compare_and_set(TEST_REF, None, &foo).await,
        Err(CompareAndSetRefError::NotSupported)
    );

    let repo = RefRepoHarness::repo_failing_cas_with(|| AccessError::ReadOnly(None).into());
    assert_matches!(
        repo.compare_and_set(TEST_REF, None, &foo).await,
        Err(CompareAndSetRefError::Access(AccessError::ReadOnly(_)))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A reference repository over a local directory, which the harness can also
/// write to directly, as another writer would
struct RefRepoHarness {
    repo: ReferenceRepositoryImpl<NamedObjectRepositoryLocalFS>,
    tmp_repo_dir: tempfile::TempDir,
}

impl RefRepoHarness {
    fn new() -> Self {
        let tmp_repo_dir = tempfile::tempdir().unwrap();
        Self {
            repo: ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(
                tmp_repo_dir.path(),
            )),
            tmp_repo_dir,
        }
    }

    /// A reference repository over storage whose conditional write fails with
    /// `cas_error`; it is never read or written otherwise
    fn repo_failing_cas_with(
        cas_error: fn() -> CompareAndSetNamedError,
    ) -> ReferenceRepositoryImpl<NamedObjectRepositoryFailingCas> {
        ReferenceRepositoryImpl::new(NamedObjectRepositoryFailingCas { cas_error })
    }

    async fn store_raw(&self, data: &[u8]) {
        self.raw_repo().set(TEST_REF, data).await.unwrap();
    }

    async fn stored_raw(&self) -> Vec<u8> {
        self.raw_repo().get(TEST_REF).await.unwrap().to_vec()
    }

    fn raw_repo(&self) -> NamedObjectRepositoryLocalFS {
        NamedObjectRepositoryLocalFS::new(self.tmp_repo_dir.path())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct NamedObjectRepositoryFailingCas {
    cas_error: fn() -> CompareAndSetNamedError,
}

#[async_trait::async_trait]
impl NamedObjectRepository for NamedObjectRepositoryFailingCas {
    async fn get(&self, _name: &str) -> Result<bytes::Bytes, GetNamedError> {
        unreachable!()
    }

    async fn set(&self, _name: &str, _data: &[u8]) -> Result<(), SetNamedError> {
        unreachable!()
    }

    async fn compare_and_set(
        &self,
        _name: &str,
        _expected: Option<&[u8]>,
        _data: &[u8],
    ) -> Result<(), CompareAndSetNamedError> {
        Err((self.cas_error)())
    }

    async fn delete(&self, _name: &str) -> Result<(), DeleteNamedError> {
        unreachable!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
