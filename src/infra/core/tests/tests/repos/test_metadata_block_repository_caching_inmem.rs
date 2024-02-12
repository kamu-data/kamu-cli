// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu::{
    MetadataBlockRepositoryCachingInMem,
    MetadataBlockRepositoryExt,
    MetadataBlockRepositoryImplWithCache,
    ObjectRepositoryInMemory,
    ObjectRepositoryLocalFSSha3,
};
use kamu_core::{
    BlockNotFoundError,
    ContainsBlockError,
    GetBlockError,
    InsertBlockError,
    InsertBlockResult,
    InsertOpts,
    MetadataBlockRepository,
    ObjectRepository,
};
use opendatafabric::{MetadataBlock, Multihash};

use super::test_metadata_block_repository_shared;

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_block() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let obj_repo = ObjectRepositoryLocalFSSha3::new(tmp_repo_dir.path());
    let repo = MetadataBlockRepositoryImplWithCache::new(obj_repo);

    test_metadata_block_repository_shared::test_insert_block(&repo).await;
}

#[tokio::test]
async fn test_insert_block_cached_if_no_error() {
    let (block, hash) = test_metadata_block_repository_shared::create_block();
    let mut wrapped_mock_repo = MockMetadataBlockRepository::new();

    wrapped_mock_repo
        .expect_insert_block()
        .times(1)
        .returning(|_, _| {
            let (_, hash) = test_metadata_block_repository_shared::create_block();

            Ok(InsertBlockResult { hash })
        });
    wrapped_mock_repo
        .expect_get_block()
        // We guarantee that the block will be taken from the cache
        .times(0)
        .returning(|_| {
            let (_, hash) = test_metadata_block_repository_shared::create_block();

            Err(GetBlockError::NotFound(BlockNotFoundError { hash }))
        });
    wrapped_mock_repo
        .expect_contains_block()
        // We guarantee that the block will be taken from the cache
        .times(0)
        .returning(|_| Ok(false));

    let repo = MockMetadataBlockRepositoryWithCache::new_wrapped(wrapped_mock_repo);

    assert_matches!(
        repo.insert_block(&block, InsertOpts::default()).await,
        Ok(..)
    );
    assert_eq!(repo.get_block(&hash).await.unwrap(), block);
    assert!(repo.contains_block(&hash).await.unwrap());
}

/////////////////////////////////////////////////////////////////////////////////////////
// MockMetadataBlockRepository
/////////////////////////////////////////////////////////////////////////////////////////

// In this case, `ObjectRepositoryInMemory` is passed to keep compiler happy
type MockMetadataBlockRepositoryWithCache =
    MetadataBlockRepositoryCachingInMem<MockMetadataBlockRepository, ObjectRepositoryInMemory>;

/////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    MetadataBlockRepository {}

    #[async_trait::async_trait]
    impl MetadataBlockRepository for MetadataBlockRepository {
        async fn contains_block(&self, hash: &Multihash) -> Result<bool, ContainsBlockError>;

        async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError>;

        async fn insert_block<'a>(
            &'a self,
            block: &MetadataBlock,
            options: InsertOpts<'a>,
        ) -> Result<InsertBlockResult, InsertBlockError>;

        fn as_object_repo(&self) -> &dyn ObjectRepository;
    }
}

impl<ObjRepo> MetadataBlockRepositoryExt<ObjRepo> for MockMetadataBlockRepository
where
    ObjRepo: ObjectRepository + Sync + Send,
{
    fn new(_obj_repo: ObjRepo) -> Self {
        Self::new()
    }
}
