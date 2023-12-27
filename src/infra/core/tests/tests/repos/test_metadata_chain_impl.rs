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
use kamu::testing::*;
use kamu::*;
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

    let block_2 = MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
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

    let hash = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    let hash = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
                .prev(&hash, 0)
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();
    assert_eq!(chain.get_ref(&BlockRef::Head).await.unwrap(), hash);

    let hash = chain
        .append(
            MetadataFactory::metadata_block(
                MetadataFactory::add_data()
                    .new_offset_interval(0, 9)
                    .build(),
            )
            .prev(&hash, 1)
            .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    let block_too_low = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(10, 19)
            .build(),
    )
    .prev(&hash, 1 /* should be 2 */)
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
        if prev_block_hash.as_ref() == Some(&hash) && prev_block_sequence_number == Some(2) && next_block_sequence_number == 2
    );

    let block_too_high = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(10, 19)
            .build(),
    )
    .prev(&hash, 3 /* should be 2 */)
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
        if prev_block_hash.as_ref() == Some(&hash) && prev_block_sequence_number == Some(2) && next_block_sequence_number == 4
    );

    let block_just_right = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(10, 19)
            .build(),
    )
    .prev(&hash, 2)
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

    let hash = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();
    assert_eq!(chain.get_ref(&BlockRef::Head).await.unwrap(), hash);

    let invalid_hash = Multihash::from_digest_sha3_256(b"does-not-exist");
    assert_matches!(
        chain
            .append(
                MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
                    .prev(&hash, 0)
                    .build(),
                AppendOpts {
                    check_ref_is: Some(Some(&invalid_hash)),
                    ..Default::default()
                }
            )
            .await,
        Err(AppendError::RefCASFailed(_))
    );

    assert_eq!(chain.get_ref(&BlockRef::Head).await.unwrap(), hash);
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

    let hash = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    let hash = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
                .prev(&hash, 0)
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    // output_watermart = None
    let block = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .some_new_data()
            .new_offset_interval(0, 9)
            .build(),
    )
    .prev(&hash, 1)
    .build();

    let hash = chain.append(block, AppendOpts::default()).await.unwrap();

    // output_watermart = Some(2000-01-01)
    let block = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .some_new_data()
            .new_offset_interval(10, 19)
            .new_watermark(Some(Utc.with_ymd_and_hms(2000, 1, 1, 12, 0, 0).unwrap()))
            .build(),
    )
    .prev(&hash, 2)
    .build();

    let hash = chain.append(block, AppendOpts::default()).await.unwrap();

    // output_watermart = None
    let block = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .some_new_data()
            .new_offset_interval(20, 29)
            .build(),
    )
    .prev(&hash, 3)
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
            .some_new_data()
            .new_offset_interval(20, 29)
            .new_watermark(Some(Utc.with_ymd_and_hms(1988, 1, 1, 12, 0, 0).unwrap()))
            .build(),
    )
    .prev(&hash, 3)
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
            .some_new_data()
            .new_offset_interval(20, 29)
            .new_watermark(Some(Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap()))
            .build(),
    )
    .prev(&hash, 3)
    .build();

    chain.append(block, AppendOpts::default()).await.unwrap();
}

#[tokio::test]
async fn test_append_add_data_empty_commit() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let hash = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    let hash = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
                .prev(&hash, 0)
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    let add_data = AddDataBuilder::empty()
        .some_new_data()
        .some_new_checkpoint()
        .some_new_watermark()
        .build();
    let block = MetadataFactory::metadata_block(add_data.clone())
        .prev(&hash, 1)
        .build();

    let hash = chain.append(block, AppendOpts::default()).await.unwrap();

    // No data, same checkpoint, watermark, and source state
    let block = MetadataFactory::metadata_block(
        AddDataBuilder::empty()
            .prev_offset(add_data.last_offset())
            .prev_checkpoint(Some(
                add_data
                    .new_checkpoint
                    .as_ref()
                    .unwrap()
                    .physical_hash
                    .clone(),
            ))
            .new_checkpoint(add_data.new_checkpoint)
            .new_watermark(add_data.new_watermark)
            .new_source_state(add_data.new_source_state)
            .build(),
    )
    .prev(&hash, 2)
    .build();

    assert_matches!(
        chain.append(block, AppendOpts::default()).await,
        Err(AppendError::InvalidBlock(AppendValidationError::NoOpEvent(
            _
        )))
    );
}

#[tokio::test]
async fn test_append_execute_query_empty_commit() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let head = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    let head = chain
        .append(
            MetadataFactory::metadata_block(
                MetadataFactory::set_transform()
                    .inputs_from_aliases_and_seeded_ids(["foo", "bar"])
                    .build(),
            )
            .prev(&head, 0)
            .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    let head = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
                .prev(&head, 1)
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    let execute_query = MetadataFactory::execute_query()
        .empty_query_inputs_from_seeded_ids(["foo", "bar"])
        .some_new_data()
        .some_new_checkpoint()
        .some_new_watermark()
        .build();
    let block = MetadataFactory::metadata_block(execute_query.clone())
        .prev(&head, 2)
        .build();

    let head = chain.append(block, AppendOpts::default()).await.unwrap();

    // No data, same checkpoint and watermark
    let block = MetadataFactory::metadata_block(
        MetadataFactory::execute_query()
            .empty_query_inputs_from_seeded_ids(["foo", "bar"])
            .prev_offset(execute_query.last_offset())
            .prev_checkpoint(Some(
                execute_query
                    .new_checkpoint
                    .as_ref()
                    .unwrap()
                    .physical_hash
                    .clone(),
            ))
            .new_checkpoint(execute_query.new_checkpoint)
            .new_watermark(execute_query.new_watermark)
            .build(),
    )
    .prev(&head, 3)
    .build();

    assert_matches!(
        chain.append(block, AppendOpts::default()).await,
        Err(AppendError::InvalidBlock(AppendValidationError::NoOpEvent(
            _
        )))
    );
}

#[test_log::test(tokio::test)]
async fn test_append_add_push_source_requires_explicit_schema() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let hash = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    let res = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::add_push_source().build())
                .prev(&hash, 0)
                .build(),
            AppendOpts::default(),
        )
        .await;
    assert_matches!(
        res,
        Err(AppendError::InvalidBlock(
            AppendValidationError::InvalidEvent(_)
        ))
    );

    chain
        .append(
            MetadataFactory::metadata_block(
                MetadataFactory::add_push_source()
                    .read(ReadStepNdJson {
                        schema: Some(vec!["a STRING".to_string(), "b STRING".to_string()]),
                        ..Default::default()
                    })
                    .build(),
            )
            .prev(&hash, 0)
            .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();
}

#[test_log::test(tokio::test)]
async fn test_append_add_data_must_be_preceeded_by_schema() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let head = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    // Accepts one with empty data (in case when we infer schema, but upon initial
    // pull only source state has been set)
    let head = chain
        .append(
            MetadataFactory::metadata_block(
                MetadataFactory::add_data().some_new_source_state().build(),
            )
            .prev(&head, 0)
            .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    // Rejects one with data when schema is not set yet
    assert_matches!(
        chain
            .append(
                MetadataFactory::metadata_block(
                    MetadataFactory::add_data()
                        .some_new_data_with_offset(0, 9)
                        .some_new_source_state()
                        .build(),
                )
                .prev(&head, 1)
                .build(),
                AppendOpts::default(),
            )
            .await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::InvalidEvent(..)
        ))
    );

    // Schema is now set
    let head = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
                .prev(&head, 1)
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    // Accepts one with data
    chain
        .append(
            MetadataFactory::metadata_block(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(0, 9)
                    .some_new_source_state()
                    .build(),
            )
            .prev(&head, 2)
            .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();
}

#[test_log::test(tokio::test)]
async fn test_append_execute_query_must_be_preceeded_by_schema() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain = init_chain(tmp_dir.path());

    let head = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Derivative).build())
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    let head = chain
        .append(
            MetadataFactory::metadata_block(
                MetadataFactory::set_transform()
                    .inputs_from_aliases_and_seeded_ids(["foo", "bar"])
                    .build(),
            )
            .prev(&head, 0)
            .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    // Accepts one with empty data (in case when we infer schema, but upon initial
    // transform only checkpoint was produced)
    // TODO: we should make engines to always return the schema
    let head: Multihash = chain
        .append(
            MetadataFactory::metadata_block(
                MetadataFactory::execute_query()
                    .empty_query_inputs_from_seeded_ids(["foo", "bar"])
                    .new_checkpoint(Some(Checkpoint {
                        physical_hash: Multihash::from_digest_sha3_256(b"foo"),
                        size: 1,
                    }))
                    .build(),
            )
            .prev(&head, 1)
            .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    // Rejects one with data when schema is not set yet
    assert_matches!(
        chain
            .append(
                MetadataFactory::metadata_block(
                    MetadataFactory::execute_query()
                        .empty_query_inputs_from_seeded_ids(["foo", "bar"])
                        .prev_checkpoint(Some(Multihash::from_digest_sha3_256(b"foo")))
                        .new_checkpoint(Some(Checkpoint {
                            physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                            size: 1,
                        }))
                        .some_new_data_with_offset(0, 9)
                        .build(),
                )
                .prev(&head, 2)
                .build(),
                AppendOpts::default(),
            )
            .await,
        Err(AppendError::InvalidBlock(
            AppendValidationError::InvalidEvent(..)
        ))
    );

    // Schema is now set
    let head = chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
                .prev(&head, 2)
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    // Accepts one with data
    chain
        .append(
            MetadataFactory::metadata_block(
                MetadataFactory::execute_query()
                    .empty_query_inputs_from_seeded_ids(["foo", "bar"])
                    .prev_checkpoint(Some(Multihash::from_digest_sha3_256(b"foo")))
                    .new_checkpoint(Some(Checkpoint {
                        physical_hash: Multihash::from_digest_sha3_256(b"bar"),
                        size: 1,
                    }))
                    .some_new_data_with_offset(0, 9)
                    .build(),
            )
            .prev(&head, 3)
            .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();
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

    let block_2 = MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
        .prev(&hash_1, block_1.sequence_number)
        .build();
    let hash_2 = chain
        .append(block_2.clone(), AppendOpts::default())
        .await
        .unwrap();

    let block_3 = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(0, 9)
            .build(),
    )
    .prev(&hash_2, block_2.sequence_number)
    .build();
    let hash_3 = chain
        .append(block_3.clone(), AppendOpts::default())
        .await
        .unwrap();

    let block_4 = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(10, 19)
            .build(),
    )
    .prev(&hash_3, block_3.sequence_number)
    .build();
    let hash_4 = chain
        .append(block_4.clone(), AppendOpts::default())
        .await
        .unwrap();

    // Full range
    let hashed_blocks: Result<Vec<_>, _> = chain
        .iter_blocks_interval(&hash_4, None, false)
        .collect()
        .await;

    assert_eq!(
        hashed_blocks.unwrap(),
        [
            (hash_4.clone(), block_4.clone()),
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

    // Tailed (inclusive)
    let hashed_blocks: Result<Vec<_>, _> = chain
        .iter_blocks_interval_inclusive(&hash_3, &hash_2, false)
        .collect()
        .await;

    assert_eq!(
        hashed_blocks.unwrap(),
        [
            (hash_3.clone(), block_3.clone()),
            (hash_2.clone(), block_2.clone()),
        ]
    );

    // Tail not found
    let bad_hash = Multihash::from_digest_sha3_256(b"does-not-exist");

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
