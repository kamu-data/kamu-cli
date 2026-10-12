// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::path::PathBuf;

use odf_metadata::*;
use odf_storage::testing::test_object_repository_shared;
use odf_storage::*;
use opendatafabric_storage_lfs::{ObjectRepositoryCachingLocalFs, ObjectRepositoryLocalFSSha3};
use tokio::io::AsyncReadExt;

use super::test_streams::InterruptedStream;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_bytes() {
    let harness = CachingRepoHarness::new();

    test_object_repository_shared::test_insert_bytes(&harness.repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete() {
    let harness = CachingRepoHarness::new();

    test_object_repository_shared::test_delete(&harness.repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_precomputed() {
    let harness = CachingRepoHarness::new();

    test_object_repository_shared::test_insert_precomputed(&harness.repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_expect() {
    let harness = CachingRepoHarness::new();

    test_object_repository_shared::test_insert_expect(&harness.repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_get_bytes_fills_cache() {
    let harness = CachingRepoHarness::new();
    let hash = harness.insert_into_wrapped_only(b"foobar").await;
    assert!(!harness.is_cached(&hash));

    assert_eq!(harness.repo.get_bytes(&hash).await.unwrap(), &b"foobar"[..]);

    assert_eq!(harness.cached_bytes(&hash), b"foobar");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_get_stream_fills_cache() {
    let harness = CachingRepoHarness::new();
    let hash = harness.insert_into_wrapped_only(b"foobar").await;
    assert!(!harness.is_cached(&hash));

    assert_eq!(harness.read_stream(&hash).await.unwrap(), b"foobar");

    assert_eq!(harness.cached_bytes(&hash), b"foobar");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_cache_hit_does_not_reach_wrapped() {
    let mut wrapped = MockObjectRepository::new();
    wrapped.expect_insert_bytes().times(1).returning(|data, _| {
        Ok(InsertResult {
            hash: Multihash::from_digest_sha3_256(data),
        })
    });
    wrapped.expect_contains().times(0);
    wrapped.expect_get_size().times(0);
    wrapped.expect_get_bytes().times(0);
    wrapped.expect_get_stream().times(0);

    let harness = CachingRepoHarness::with_wrapped(wrapped);
    let hash = harness
        .repo
        .insert_bytes(b"foobar", InsertOpts::default())
        .await
        .unwrap()
        .hash;

    assert!(harness.repo.contains(&hash).await.unwrap());
    assert_eq!(harness.repo.get_size(&hash).await.unwrap(), 6);
    assert_eq!(harness.repo.get_bytes(&hash).await.unwrap(), &b"foobar"[..]);
    assert_eq!(harness.read_stream(&hash).await.unwrap(), b"foobar");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_interrupted_stream_leaves_no_cache_entry() {
    let hash = Multihash::from_digest_sha3_256(b"foobar");

    let mut wrapped = MockObjectRepository::new();
    let mut first_call = true;
    wrapped.expect_get_stream().times(2).returning(move |_| {
        if std::mem::take(&mut first_call) {
            Ok(Box::new(InterruptedStream::new(b"foo")))
        } else {
            Ok(Box::new(std::io::Cursor::new(b"foobar")))
        }
    });

    let harness = CachingRepoHarness::with_wrapped(wrapped);

    assert_matches!(harness.read_stream(&hash).await, Err(GetError::Internal(_)));
    assert_eq!(harness.cache_dir_entries(), Vec::<String>::new());

    assert_eq!(harness.read_stream(&hash).await.unwrap(), b"foobar");
    assert_eq!(harness.cached_bytes(&hash), b"foobar");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CachingRepoHarness<WrappedRepo> {
    repo: ObjectRepositoryCachingLocalFs<WrappedRepo>,
    cache_dir: PathBuf,
    wrapped_dir: PathBuf,
    _tmp: tempfile::TempDir,
}

impl CachingRepoHarness<ObjectRepositoryLocalFSSha3> {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let wrapped_dir = tmp.path().join("wrapped");
        Self::build(
            ObjectRepositoryLocalFSSha3::new(&wrapped_dir),
            wrapped_dir,
            tmp,
        )
    }

    /// Puts an object into the wrapped repository, bypassing the cache
    async fn insert_into_wrapped_only(&self, data: &[u8]) -> Multihash {
        ObjectRepositoryLocalFSSha3::new(&self.wrapped_dir)
            .insert_bytes(data, InsertOpts::default())
            .await
            .unwrap()
            .hash
    }
}

impl CachingRepoHarness<MockObjectRepository> {
    fn with_wrapped(wrapped: MockObjectRepository) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let wrapped_dir = tmp.path().join("wrapped");
        Self::build(wrapped, wrapped_dir, tmp)
    }
}

impl<WrappedRepo> CachingRepoHarness<WrappedRepo>
where
    WrappedRepo: ObjectRepository,
{
    fn build(wrapped: WrappedRepo, wrapped_dir: PathBuf, tmp: tempfile::TempDir) -> Self {
        let cache_dir = tmp.path().join("cache");
        Self {
            repo: ObjectRepositoryCachingLocalFs::new(wrapped, &cache_dir),
            cache_dir,
            wrapped_dir,
            _tmp: tmp,
        }
    }

    async fn read_stream(&self, hash: &Multihash) -> Result<Vec<u8>, GetError> {
        let mut stream = self.repo.get_stream(hash).await?;
        let mut data = Vec::new();
        stream.read_to_end(&mut data).await.unwrap();
        Ok(data)
    }

    fn cache_path(&self, hash: &Multihash) -> PathBuf {
        self.cache_dir.join(hash.as_multibase().to_stack_string())
    }

    fn is_cached(&self, hash: &Multihash) -> bool {
        self.cache_path(hash).is_file()
    }

    fn cached_bytes(&self, hash: &Multihash) -> Vec<u8> {
        std::fs::read(self.cache_path(hash)).unwrap()
    }

    /// File names in the cache directory, including staging files
    fn cache_dir_entries(&self) -> Vec<String> {
        if !self.cache_dir.exists() {
            return Vec::new();
        }
        let mut entries: Vec<_> = std::fs::read_dir(&self.cache_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        entries.sort();
        entries
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
