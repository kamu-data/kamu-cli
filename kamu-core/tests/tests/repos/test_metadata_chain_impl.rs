// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::path::Path;

use chrono::{TimeZone, Utc};
use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;

fn init_chain(root: &Path) -> impl MetadataChain {
    let blocks_dir = root.join("blocks");
    let refs_dir = root.join("refs");
    std::fs::create_dir(&blocks_dir).unwrap();
    std::fs::create_dir(&refs_dir).unwrap();

    let obj_repo =
        ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(blocks_dir /* unknown yet */);
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
    assert_eq!(0, block_1.sequence_number);

    let block_2 = MetadataFactory::metadata_block(MetadataFactory::add_data().build())
        .prev(&hash_1, block_1.sequence_number)
        .build();

    let hash_2 = chain
        .append(block_2.clone(), AppendOpts::default())
        .await
        .unwrap();

    assert_eq!(chain.get_ref(&BlockRef::Head).await.unwrap(), hash_2);
    assert_eq!(1, block_2.sequence_number);

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
    let block_1_sequence_number = block_1.sequence_number;

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
async fn test_append_prev_block_sequence_integrity_broken() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block_1 =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build();

    let hash_1 = chain.append(block_1, AppendOpts::default()).await.unwrap();

    assert_eq!(chain.get_ref(&BlockRef::Head).await.unwrap(), hash_1);

    let block_2 =
        MetadataFactory::metadata_block(MetadataFactory::add_data().interval(0, 9).build())
            .prev(&hash_1, 0)
            .build();

    let hash_2 = chain.append(block_2, AppendOpts::default()).await.unwrap();

    let block_too_low =
        MetadataFactory::metadata_block(MetadataFactory::add_data().interval(10, 19).build())
            .prev(&hash_2, 0 /* should be 1 */)
            .build();

    assert_matches!(
        chain.append(block_too_low, AppendOpts::default()).await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::SequenceIntegrity(SequenceIntegrityError {
                prev_block_hash,
                prev_block_sequence_number,
                next_block_sequence_number
            })
        ))
        if prev_block_hash.as_ref() == Some(&hash_2) && prev_block_sequence_number == Some(1) && next_block_sequence_number == 1
    );

    let block_too_high =
        MetadataFactory::metadata_block(MetadataFactory::add_data().interval(10, 19).build())
            .prev(&hash_2, 2 /* should be 1 */)
            .build();

    assert_matches!(
        chain.append(block_too_high, AppendOpts::default()).await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::SequenceIntegrity(SequenceIntegrityError {
                prev_block_hash,
                prev_block_sequence_number,
                next_block_sequence_number
            })
        ))
        if prev_block_hash.as_ref() == Some(&hash_2) && prev_block_sequence_number == Some(1) && next_block_sequence_number == 3
    );

    let block_just_right =
        MetadataFactory::metadata_block(MetadataFactory::add_data().interval(10, 19).build())
            .prev(&hash_2, 1)
            .build();

    let hash_just_right = chain
        .append(block_just_right, AppendOpts::default())
        .await
        .unwrap();

    assert_eq!(
        chain.get_ref(&BlockRef::Head).await.unwrap(),
        hash_just_right
    );
}

#[tokio::test]
async fn test_append_unexpected_ref() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block_1 =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build();
    let block_1_sequence_number = block_1.sequence_number;

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
    let block_1_sequence_number = block_1.sequence_number;

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
    let block_1_sequence_number = block_1.sequence_number;

    let hash_1 = chain.append(block_1, AppendOpts::default()).await.unwrap();

    let block_2 = MetadataFactory::metadata_block(MetadataFactory::add_data().build())
        .prev(&hash_1, block_1_sequence_number)
        .system_time(Utc.with_ymd_and_hms(2000, 1, 1, 12, 0, 0).unwrap())
        .build();

    assert_matches!(
        chain.append(block_2, AppendOpts::default()).await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::SystemTimeIsNotMonotonic
        ))
    );
}

#[tokio::test]
async fn test_append_watermark_non_monotonic() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build();

    let hash = chain.append(block, AppendOpts::default()).await.unwrap();

    // output_watermart = None
    let block = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .some_output_data()
            .interval(0, 9)
            .build(),
    )
    .prev(&hash, 0)
    .build();

    let hash = chain.append(block, AppendOpts::default()).await.unwrap();

    // output_watermart = Some(2000-01-01)
    let block = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .some_output_data()
            .interval(10, 19)
            .watermark(Utc.with_ymd_and_hms(2000, 1, 1, 12, 0, 0).unwrap())
            .build(),
    )
    .prev(&hash, 1)
    .build();

    let hash = chain.append(block, AppendOpts::default()).await.unwrap();

    // output_watermart = None
    let block = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .some_output_data()
            .interval(20, 29)
            .build(),
    )
    .prev(&hash, 2)
    .build();

    assert_matches!(
        chain.append(block, AppendOpts::default()).await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::WatermarkIsNotMonotonic
        ))
    );

    // output_watermart = Some(1988-01-01)
    let block = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .some_output_data()
            .interval(20, 29)
            .watermark(Utc.with_ymd_and_hms(1988, 1, 1, 12, 0, 0).unwrap())
            .build(),
    )
    .prev(&hash, 2)
    .build();

    assert_matches!(
        chain.append(block, AppendOpts::default()).await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::WatermarkIsNotMonotonic
        ))
    );

    // output_watermart = Some(2020-01-01)
    let block = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .some_output_data()
            .interval(20, 29)
            .watermark(Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap())
            .build(),
    )
    .prev(&hash, 2)
    .build();

    chain.append(block, AppendOpts::default()).await.unwrap();
}

#[tokio::test]
async fn test_append_add_data_empty_commit() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build();

    let hash = chain.append(block, AppendOpts::default()).await.unwrap();

    let add_data = AddDataBuilder::empty()
        .some_output_data()
        .some_output_checkpoint()
        .some_output_watermark()
        .build();
    let block = MetadataFactory::metadata_block(add_data.clone())
        .prev(&hash, 0)
        .build();

    let hash = chain.append(block, AppendOpts::default()).await.unwrap();

    // No data, same checkpoint, watermark, and source state
    let block = MetadataFactory::metadata_block(
        AddDataBuilder::empty()
            .output_checkpoint(add_data.output_checkpoint)
            .output_watermark(add_data.output_watermark)
            .source_state(add_data.source_state)
            .build(),
    )
    .prev(&hash, 1)
    .build();

    assert_matches!(
        chain.append(block, AppendOpts::default()).await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::InvalidEvent(_)
        ))
    );
}

#[tokio::test]
async fn test_append_execute_query_empty_commit() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let block =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build()).build();

    let hash = chain.append(block, AppendOpts::default()).await.unwrap();

    let execute_query = MetadataFactory::execute_query()
        .some_output_data()
        .some_output_checkpoint()
        .some_output_watermark()
        .build();
    let block = MetadataFactory::metadata_block(execute_query.clone())
        .prev(&hash, 0)
        .build();

    let hash = chain.append(block, AppendOpts::default()).await.unwrap();

    // No data, same checkpoint and watermark
    let block = MetadataFactory::metadata_block(
        MetadataFactory::execute_query()
            .output_checkpoint(execute_query.output_checkpoint)
            .output_watermark(execute_query.output_watermark)
            .build(),
    )
    .prev(&hash, 1)
    .build();

    assert_matches!(
        chain.append(block, AppendOpts::default()).await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::InvalidEvent(_)
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

    let block_2 =
        MetadataFactory::metadata_block(MetadataFactory::add_data().interval(0, 9).build())
            .prev(&hash_1, block_1.sequence_number)
            .build();
    let hash_2 = chain
        .append(block_2.clone(), AppendOpts::default())
        .await
        .unwrap();

    let block_3 =
        MetadataFactory::metadata_block(MetadataFactory::add_data().interval(10, 19).build())
            .prev(&hash_2, block_2.sequence_number)
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
