// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use odf::dataset::*;
use odf::metadata::*;
use odf::storage::lfs::NamedObjectRepositoryLocalFS;
use odf::storage::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_ref_cas_reports_the_actual_head() {
    let harness = RefRepoHarness::new();

    harness.set_head(None, &harness.hash_a).await.unwrap();

    assert_matches!(
        harness.set_head(Some(&harness.hash_b), &harness.hash_c).await,
        Err(SetChainRefError::CASFailed(RefCASError { expected, actual, .. }))
            if expected == Some(harness.hash_b.clone()) && actual == Some(harness.hash_a.clone())
    );
    assert_matches!(
        harness.set_head(None, &harness.hash_c).await,
        Err(SetChainRefError::CASFailed(RefCASError {
            expected: None,
            ..
        }))
    );
    assert_eq!(harness.head().await, harness.hash_a);

    harness
        .set_head(Some(&harness.hash_a), &harness.hash_b)
        .await
        .unwrap();
    assert_eq!(harness.head().await, harness.hash_b);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_ref_without_conditional_writes_checks_before_writing() {
    let harness = RefRepoHarness::without_conditional_writes();

    harness.set_head(None, &harness.hash_a).await.unwrap();

    assert_matches!(
        harness.set_head(Some(&harness.hash_b), &harness.hash_c).await,
        Err(SetChainRefError::CASFailed(RefCASError { actual, .. }))
            if actual == Some(harness.hash_a.clone())
    );
    assert_eq!(harness.head().await, harness.hash_a);

    harness
        .set_head(Some(&harness.hash_a), &harness.hash_b)
        .await
        .unwrap();
    assert_eq!(harness.head().await, harness.hash_b);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_ref_without_check_overwrites() {
    let harness = RefRepoHarness::new();

    harness.set_head(None, &harness.hash_a).await.unwrap();
    harness
        .ref_repo
        .set_ref(&BlockRef::Head, &harness.hash_b, None)
        .await
        .unwrap();

    assert_eq!(harness.head().await, harness.hash_b);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct RefRepoHarness {
    ref_repo: Box<dyn MetadataChainReferenceRepository>,
    hash_a: Multihash,
    hash_b: Multihash,
    hash_c: Multihash,
    _tmp_dir: tempfile::TempDir,
}

impl RefRepoHarness {
    fn new() -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        let ref_repo = Box::new(MetadataChainReferenceRepositoryImpl::new(
            ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(tmp_dir.path())),
        ));
        Self::with_ref_repo(ref_repo, tmp_dir)
    }

    fn without_conditional_writes() -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        let ref_repo = Box::new(MetadataChainReferenceRepositoryImpl::new(
            ReferenceRepositoryImpl::new(NamedObjectRepositoryWithoutConditionalWrites(
                NamedObjectRepositoryLocalFS::new(tmp_dir.path()),
            )),
        ));
        Self::with_ref_repo(ref_repo, tmp_dir)
    }

    fn with_ref_repo(
        ref_repo: Box<dyn MetadataChainReferenceRepository>,
        tmp_dir: tempfile::TempDir,
    ) -> Self {
        Self {
            ref_repo,
            hash_a: Multihash::from_digest_sha3_256(b"a"),
            hash_b: Multihash::from_digest_sha3_256(b"b"),
            hash_c: Multihash::from_digest_sha3_256(b"c"),
            _tmp_dir: tmp_dir,
        }
    }

    async fn set_head(
        &self,
        expected: Option<&Multihash>,
        hash: &Multihash,
    ) -> Result<(), SetChainRefError> {
        self.ref_repo
            .set_ref(&BlockRef::Head, hash, Some(expected))
            .await
    }

    async fn head(&self) -> Multihash {
        self.ref_repo.get_ref(&BlockRef::Head).await.unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Keeps the default `compare_and_set`, which reports `NotSupported`
struct NamedObjectRepositoryWithoutConditionalWrites(NamedObjectRepositoryLocalFS);

#[async_trait::async_trait]
impl NamedObjectRepository for NamedObjectRepositoryWithoutConditionalWrites {
    async fn get(&self, name: &str) -> Result<bytes::Bytes, GetNamedError> {
        self.0.get(name).await
    }

    async fn set(&self, name: &str, data: &[u8]) -> Result<(), SetNamedError> {
        self.0.set(name, data).await
    }

    async fn delete(&self, name: &str) -> Result<(), DeleteNamedError> {
        self.0.delete(name).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
