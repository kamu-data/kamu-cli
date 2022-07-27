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
use kamu::testing::*;
use opendatafabric::*;

use chrono::{TimeZone, Utc};
use std::assert_matches::assert_matches;
use std::path::Path;

fn init_chain(root: &Path) -> impl MetadataChain {
    let blocks_dir = root.join("blocks");
    let refs_dir = root.join("refs");
    std::fs::create_dir(&blocks_dir).unwrap();
    std::fs::create_dir(&refs_dir).unwrap();

    let obj_repo = ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(blocks_dir);
    let ref_repo = ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(refs_dir));

    MetadataChainImpl::new(obj_repo, ref_repo)
}

#[tokio::test]
async fn test_empty() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());
    assert_matches!(
        chain.get_ref(&BlockRef::Head).await,
        Err(GetRefError::NotFound(_))
    );
}

#[tokio::test]
async fn test_append_and_get() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block_1 =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build();

    let hash_1 = chain
        .append(block_1.clone(), AppendOpts::default())
        .await
        .unwrap();

    assert_eq!(chain.get_ref(&BlockRef::Head).await.unwrap(), hash_1);

    let block_2 = MetadataFactory::metadata_block(MetadataFactory::add_data().build())
        .prev(&hash_1, block_1.sequence_number.unwrap())
        .build();

    let hash_2 = chain
        .append(block_2.clone(), AppendOpts::default())
        .await
        .unwrap();

    assert_eq!(chain.get_ref(&BlockRef::Head).await.unwrap(), hash_2);

    assert_eq!(chain.get_block(&hash_1).await.unwrap(), block_1);
    assert_eq!(chain.get_block(&hash_2).await.unwrap(), block_2);
}

#[tokio::test]
async fn test_set_ref() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block_1 =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build();

    let hash_1 = chain
        .append(
            block_1.clone(),
            AppendOpts {
                update_ref: None,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_matches!(
        chain.get_ref(&BlockRef::Head).await,
        Err(GetRefError::NotFound(_))
    );

    assert_matches!(
        chain
            .set_ref(
                &BlockRef::Head,
                &Multihash::from_digest_sha3_256(b"does-not-exist"),
                SetRefOpts::default(),
            )
            .await,
        Err(SetRefError::BlockNotFound(_))
    );

    assert_matches!(
        chain
            .set_ref(
                &BlockRef::Head,
                &hash_1,
                SetRefOpts {
                    validate_block_present: false,
                    check_ref_is: Some(Some(&Multihash::from_digest_sha3_256(b"does-not-exist"))),
                }
            )
            .await,
        Err(SetRefError::CASFailed(_))
    );

    chain
        .set_ref(&BlockRef::Head, &hash_1, SetRefOpts::default())
        .await
        .unwrap();
    assert_eq!(chain.get_ref(&BlockRef::Head).await.unwrap(), hash_1);
}

#[tokio::test]
async fn test_append_hash_mismatch() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block_1 =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build();

    let bad_hash = Multihash::from_digest_sha3_256(b"does-not-exist");
    assert_matches!(
        chain
            .append(
                block_1,
                AppendOpts {
                    expected_hash: Some(&bad_hash),
                    ..Default::default()
                }
            )
            .await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::HashMismatch(_)
        ))
    );
}

#[tokio::test]
async fn test_append_prev_block_not_found() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block_1 =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build();
    let block_1_sequence_number = block_1.sequence_number.unwrap();

    let hash_1 = chain.append(block_1, AppendOpts::default()).await.unwrap();

    assert_eq!(chain.get_ref(&BlockRef::Head).await.unwrap(), hash_1);

    let block_2 = MetadataFactory::metadata_block(MetadataFactory::add_data().build())
        .prev(
            &Multihash::from_digest_sha3_256(b"does-not-exist"),
            block_1_sequence_number,
        )
        .build();

    assert_matches!(
        chain.append(block_2, AppendOpts::default()).await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::PrevBlockNotFound(_)
        ))
    );

    assert_eq!(chain.get_ref(&BlockRef::Head).await.unwrap(), hash_1);
}

#[tokio::test]
async fn test_append_unexpected_ref() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block_1 =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build();
    let block_1_sequence_number = block_1.sequence_number.unwrap();

    let hash_1 = chain.append(block_1, AppendOpts::default()).await.unwrap();

    assert_eq!(chain.get_ref(&BlockRef::Head).await.unwrap(), hash_1);

    let block_2 = MetadataFactory::metadata_block(MetadataFactory::add_data().build())
        .prev(&hash_1, block_1_sequence_number)
        .build();

    let invalid_hash = Multihash::from_digest_sha3_256(b"does-not-exist");
    assert_matches!(
        chain
            .append(
                block_2,
                AppendOpts {
                    check_ref_is: Some(Some(&invalid_hash)),
                    ..Default::default()
                }
            )
            .await,
        Err(AppendError::RefCASFailed(_))
    );

    assert_eq!(chain.get_ref(&BlockRef::Head).await.unwrap(), hash_1);
}

#[tokio::test]
async fn test_append_first_block_not_seed() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block_1 = MetadataFactory::metadata_block(MetadataFactory::add_data().build()).build();

    assert_matches!(
        chain.append(block_1, AppendOpts::default()).await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::FirstBlockMustBeSeed
        ))
    );
}

#[tokio::test]
async fn test_append_seed_block_not_first() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block_1 =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build();
    let block_1_sequence_number = block_1.sequence_number.unwrap();

    let hash_1 = chain.append(block_1, AppendOpts::default()).await.unwrap();

    let block_2 = MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
        .prev(&hash_1, block_1_sequence_number)
        .build();

    assert_matches!(
        chain.append(block_2, AppendOpts::default()).await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::AppendingSeedBlockToNonEmptyChain
        ))
    );
}

#[tokio::test]
async fn test_append_system_time_non_monotonic() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block_1 =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build();
    let block_1_sequence_number = block_1.sequence_number.unwrap();

    let hash_1 = chain.append(block_1, AppendOpts::default()).await.unwrap();

    let block_2 = MetadataFactory::metadata_block(MetadataFactory::add_data().build())
        .prev(&hash_1, block_1_sequence_number)
        .system_time(Utc.ymd(2000, 1, 1).and_hms(12, 0, 0))
        .build();

    assert_matches!(
        chain.append(block_2, AppendOpts::default()).await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::SystemTimeIsNotMonotonic
        ))
    );
}

#[tokio::test]
async fn test_iter_blocks() {
    use tokio_stream::StreamExt;

    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block_1 =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build();
    let hash_1 = chain
        .append(block_1.clone(), AppendOpts::default())
        .await
        .unwrap();

    let block_2 = MetadataFactory::metadata_block(MetadataFactory::add_data().build())
        .prev(&hash_1, block_1.sequence_number.unwrap())
        .build();
    let hash_2 = chain
        .append(block_2.clone(), AppendOpts::default())
        .await
        .unwrap();

    let block_3 = MetadataFactory::metadata_block(MetadataFactory::add_data().build())
        .prev(&hash_2, block_2.sequence_number.unwrap())
        .build();
    let hash_3 = chain
        .append(block_3.clone(), AppendOpts::default())
        .await
        .unwrap();

    // Full range
    let hashed_blocks: Result<Vec<_>, _> = chain
        .iter_blocks_interval(&hash_3, None, false)
        .collect()
        .await;

    assert_eq!(
        hashed_blocks.unwrap(),
        [
            (hash_3.clone(), block_3.clone()),
            (hash_2.clone(), block_2.clone()),
            (hash_1.clone(), block_1.clone())
        ]
    );

    // Tailed
    let hashed_blocks: Result<Vec<_>, _> = chain
        .iter_blocks_interval(&hash_3, Some(&hash_1), false)
        .collect()
        .await;

    assert_eq!(
        hashed_blocks.unwrap(),
        [
            (hash_3.clone(), block_3.clone()),
            (hash_2.clone(), block_2.clone())
        ]
    );

    let bad_hash = Multihash::from_digest_sha3_256(b"does-not-exist");

    // Tail not found
    let hashed_blocks: Result<Vec<_>, _> = chain
        .iter_blocks_interval(&hash_3, Some(&bad_hash), false)
        .collect()
        .await;
    assert_matches!(hashed_blocks, Err(IterBlocksError::InvalidInterval(_)));

    // Try ignoring divergence
    let hashed_blocks: Result<Vec<_>, _> = chain
        .iter_blocks_interval(&hash_3, Some(&bad_hash), true)
        .collect()
        .await;
    assert_eq!(
        hashed_blocks.unwrap(),
        [
            (hash_3.clone(), block_3.clone()),
            (hash_2.clone(), block_2.clone()),
            (hash_1.clone(), block_1.clone())
        ]
    );
}
