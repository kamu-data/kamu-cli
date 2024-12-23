// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use odf_metadata::*;
use odf_storage_lfs::ObjectRepositoryLocalFSSha3;
use opendatafabric_storage::*;

use super::test_metadata_block_repository_shared;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_block() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let obj_repo = ObjectRepositoryLocalFSSha3::new(tmp_repo_dir.path());
    let repo = MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(obj_repo));

    test_metadata_block_repository_shared::test_insert_block(&repo).await;
}

#[tokio::test]
async fn test_insert_block_cached_if_no_error() {
    let mut wrapped_mock_repo = MockMetadataBlockRepository::new();

    wrapped_mock_repo
        .expect_insert_block_data()
        .times(1)
        .returning(|_, _| {
            let (_, _, hash, _) = test_metadata_block_repository_shared::create_block();

            Ok(InsertBlockResult { hash })
        });
    wrapped_mock_repo
        .expect_get_block()
        // We guarantee that the block will be taken from the cache
        .times(0)
        .returning(|_| {
            let (_, _, hash, _) = test_metadata_block_repository_shared::create_block();

            Err(GetBlockError::NotFound(BlockNotFoundError { hash }))
        });
    wrapped_mock_repo
        .expect_get_block_data()
        // We guarantee that the block will be taken from the cache
        //
        // Two calls, these are checks for a block before insert in
        // test_metadata_block_repository_shared::test_insert_block()
        .times(2)
        .returning(|_| {
            let (_, _, hash, _) = test_metadata_block_repository_shared::create_block();

            Err(GetBlockDataError::NotFound(BlockNotFoundError { hash }))
        });
    wrapped_mock_repo
        .expect_contains_block()
        // We guarantee that the block will be taken from the cache
        .times(0)
        .returning(|_| Ok(false));

    let repo = MetadataBlockRepositoryCachingInMem::new(wrapped_mock_repo);

    test_metadata_block_repository_shared::test_insert_block(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MockMetadataBlockRepository
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    MetadataBlockRepository {}

    #[async_trait::async_trait]
    impl MetadataBlockRepository for MetadataBlockRepository {
        async fn contains_block(&self, hash: &Multihash) -> Result<bool, ContainsBlockError>;

        async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError>;

        async fn get_block_data(&self, hash: &Multihash) -> Result<Bytes, GetBlockDataError>;

        async fn get_block_size(&self, hash: &Multihash) -> Result<u64, GetBlockDataError>;

        async fn insert_block<'a>(
            &'a self,
            block: &MetadataBlock,
            options: InsertOpts<'a>,
        ) -> Result<InsertBlockResult, InsertBlockError>;

        async fn insert_block_data<'a>(
            &'a self,
            block_data: &'a [u8],
            options: InsertOpts<'a>,
        ) -> Result<InsertBlockResult, InsertBlockError>;
    }
}
