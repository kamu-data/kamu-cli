// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use kamu_datasets::*;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use odf::metadata::testing::MetadataFactory;

use super::use_cases::dataset_base_use_case_harness::{
    DatasetBaseUseCaseHarness,
    DatasetBaseUseCaseHarnessOpts,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_always_next() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    let iteration_order = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            None,
            |_| odf::dataset::MetadataVisitorDecision::Next,
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (5, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (4, odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE),
            (3, odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA),
            (2, odf::metadata::MetadataEventTypeFlags::SET_LICENSE),
            (1, odf::metadata::MetadataEventTypeFlags::SET_INFO),
            (0, odf::metadata::MetadataEventTypeFlags::SEED),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_always_stop() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    let iteration_order = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            None,
            |_| odf::dataset::MetadataVisitorDecision::Stop,
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order,
        vec![(7, odf::metadata::MetadataEventTypeFlags::ADD_DATA)] /* first is unconditional */
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_static_key_blocks() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    let iteration_order = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            None,
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::SET_INFO
                        | odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA,
                )
            },
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (3, odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA),
            (1, odf::metadata::MetadataEventTypeFlags::SET_INFO),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_dynamic_key_blocks() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    let iteration_order = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            None,
            |block_sequence_number| {
                if block_sequence_number > 3 {
                    odf::dataset::MetadataVisitorDecision::NextOfType(
                        odf::metadata::MetadataEventTypeFlags::SET_LICENSE,
                    )
                } else {
                    odf::dataset::MetadataVisitorDecision::NextOfType(
                        odf::metadata::MetadataEventTypeFlags::SET_INFO,
                    )
                }
            },
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (2, odf::metadata::MetadataEventTypeFlags::SET_LICENSE),
            (1, odf::metadata::MetadataEventTypeFlags::SET_INFO),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_missing_key_block() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    let iteration_order = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            None,
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::SET_VOCAB,
                )
            },
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA), /* first is unconditional */
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_partially_missing_key_blocks() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    let iteration_order = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            None,
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::SET_VOCAB
                        | odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE,
                )
            },
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA), /* first is unconditional */
            (4, odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_with_data() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    let iteration_order_1 = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            None,
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::ADD_DATA,
                )
            },
        )
        .await;

    let iteration_order_2 = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            None,
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::ADD_DATA
                        | odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA,
                )
            },
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order_1,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (5, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
        ]
    );

    pretty_assertions::assert_eq!(
        iteration_order_2,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (5, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (3, odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_new_key_blocks_after_data() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    // commit 2 more key blocks

    foo_created
        .dataset
        .commit_event(
            odf::MetadataEvent::SetInfo(MetadataFactory::set_info().build()),
            odf::dataset::CommitOpts {
                check_object_refs: false, // Cheating a little
                ..odf::dataset::CommitOpts::default()
            },
        )
        .await
        .unwrap();

    let commit_result = foo_created
        .dataset
        .commit_event(
            odf::MetadataEvent::SetLicense(MetadataFactory::set_license().build()),
            odf::dataset::CommitOpts {
                check_object_refs: false, // Cheating a little
                ..odf::dataset::CommitOpts::default()
            },
        )
        .await
        .unwrap();

    let iteration_order = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &commit_result.new_head,
            None,
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::SET_INFO,
                )
            },
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (9, odf::metadata::MetadataEventTypeFlags::SET_LICENSE), // first always recorded
            (8, odf::metadata::MetadataEventTypeFlags::SET_INFO),
            (1, odf::metadata::MetadataEventTypeFlags::SET_INFO),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_new_key_blocks_between_data_parts() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    // commit 2 more key blocks

    foo_created
        .dataset
        .commit_event(
            odf::MetadataEvent::SetInfo(MetadataFactory::set_info().build()),
            odf::dataset::CommitOpts {
                check_object_refs: false, // Cheating a little
                ..odf::dataset::CommitOpts::default()
            },
        )
        .await
        .unwrap();

    foo_created
        .dataset
        .commit_event(
            odf::MetadataEvent::SetLicense(MetadataFactory::set_license().build()),
            odf::dataset::CommitOpts {
                check_object_refs: false, // Cheating a little
                ..odf::dataset::CommitOpts::default()
            },
        )
        .await
        .unwrap();

    // commit more data
    let commit_result = foo_created
        .dataset
        .commit_event(
            odf::MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(20, 25)
                    .prev_checkpoint(Some(odf::Multihash::from_digest_sha3_256(b"checkpoint-3")))
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-4"),
                        size: 1,
                    }))
                    .build(),
            ),
            odf::dataset::CommitOpts {
                check_object_refs: false, // Cheating a little
                ..odf::dataset::CommitOpts::default()
            },
        )
        .await
        .unwrap();

    let iteration_order = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &commit_result.new_head,
            None,
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::SET_LICENSE,
                )
            },
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (10, odf::metadata::MetadataEventTypeFlags::ADD_DATA), // first always recorded
            (9, odf::metadata::MetadataEventTypeFlags::SET_LICENSE),
            (2, odf::metadata::MetadataEventTypeFlags::SET_LICENSE),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_limited_tail_next() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    let iteration_order = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            Some(3),
            |_| odf::dataset::MetadataVisitorDecision::Next,
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (5, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (4, odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE),
            (3, odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_limited_tail_flags_with_early_abort() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    let iteration_order_1 = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            Some(3),
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE
                        | odf::metadata::MetadataEventTypeFlags::SET_LICENSE,
                )
            },
        )
        .await;

    let iteration_order_2 = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            Some(2),
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE
                        | odf::metadata::MetadataEventTypeFlags::SET_LICENSE,
                )
            },
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order_1,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA), // fetched always
            (4, odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE),
        ]
    );

    pretty_assertions::assert_eq!(
        iteration_order_2,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA), // fetched always
            (4, odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE),
            (2, odf::metadata::MetadataEventTypeFlags::SET_LICENSE),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_limit_both_tail_and_head() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    let head_block = foo_created
        .dataset
        .as_metadata_chain()
        .get_block(&foo_created.head)
        .await
        .unwrap();

    let pre_head_block = foo_created
        .dataset
        .as_metadata_chain()
        .get_block(head_block.prev_block_hash.as_ref().unwrap())
        .await
        .unwrap();

    let iteration_order = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            pre_head_block.prev_block_hash.as_ref().unwrap(),
            Some(2),
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA
                        | odf::metadata::MetadataEventTypeFlags::SET_INFO,
                )
            },
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (5, odf::metadata::MetadataEventTypeFlags::ADD_DATA), // fetched always
            (3, odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Data Block Iteration Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_data_blocks_only_iteration() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    let iteration_order = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            None,
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::DATA_BLOCK,
                )
            },
        )
        .await;

    // With page-based caching, DATA_BLOCK requests should only return data blocks
    // without forcing a full chain scan
    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (5, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_execute_transform_only_iteration() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("derivative"));
    let derivative_created = harness.create_derivative_dataset(&alias).await;

    let iteration_order = harness
        .iterate_through(
            derivative_created.dataset.as_ref(),
            &derivative_created.head,
            None,
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::EXECUTE_TRANSFORM,
                )
            },
        )
        .await;

    // With page-based caching, EXECUTE_TRANSFORM requests should only return
    // transform blocks
    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (5, odf::metadata::MetadataEventTypeFlags::EXECUTE_TRANSFORM),
            (4, odf::metadata::MetadataEventTypeFlags::EXECUTE_TRANSFORM),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_key_and_data_blocks_iteration() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("derivative"));
    let derivative_created = harness.create_derivative_dataset(&alias).await;

    let iteration_order = harness
        .iterate_through(
            derivative_created.dataset.as_ref(),
            &derivative_created.head,
            None,
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::EXECUTE_TRANSFORM
                        | odf::metadata::MetadataEventTypeFlags::SET_TRANSFORM,
                )
            },
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (5, odf::metadata::MetadataEventTypeFlags::EXECUTE_TRANSFORM),
            (4, odf::metadata::MetadataEventTypeFlags::EXECUTE_TRANSFORM),
            (3, odf::metadata::MetadataEventTypeFlags::SET_TRANSFORM),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_data_blocks_with_tail_limit() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    // Test DATA_BLOCK iteration with tail limit at sequence 6
    // This should stop at the tail and not scan earlier blocks
    let iteration_order = harness
        .iterate_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            Some(6), // Tail limit - don't go below sequence 6
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::DATA_BLOCK,
                )
            },
        )
        .await;

    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_page_boundary_stress_with_many_data_blocks() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("large"));
    let large_created = harness.create_dataset_with_many_data_blocks(&alias).await;

    // Pure data
    let iteration_order_1 = harness
        .iterate_through(
            large_created.dataset.as_ref(),
            &large_created.head,
            None,
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::DATA_BLOCK,
                )
            },
        )
        .await;

    // Should efficiently iterate through many data blocks using page caching
    // This tests page boundary handling across the 100-block threshold
    assert_eq!(iteration_order_1.len(), 150); // Expecting exactly 150 ADD_DATA blocks

    // Verify all returned blocks are ADD_DATA and in descending order
    // Starting from block 153 (150 data + 3 setup blocks) down to block 4
    for (i, (seq_num, event_type)) in iteration_order_1.iter().enumerate() {
        assert_eq!(*event_type, odf::metadata::MetadataEventTypeFlags::ADD_DATA);
        assert_eq!(*seq_num, 153 - i as u64); // Should be 153, 152, 151, ..., 4
    }

    // Mix data and key blocks
    let iteration_order_2 = harness
        .iterate_through(
            large_created.dataset.as_ref(),
            &large_created.head,
            None,
            |_| {
                odf::dataset::MetadataVisitorDecision::NextOfType(
                    odf::metadata::MetadataEventTypeFlags::DATA_BLOCK
                        | odf::metadata::MetadataEventTypeFlags::SET_INFO,
                )
            },
        )
        .await;

    // Should efficiently iterate through many data blocks and SET_INFO blocks using
    // page caching
    assert_eq!(iteration_order_2.len(), 151); // 150 ADD_DATA + 1 SET_INFO block

    // Verify all returned blocks are either ADD_DATA or SET_INFO and in descending
    // order Starting from block 153 (150 data + 3 setup blocks) down to block 4
    for (i, (seq_num, event_type)) in iteration_order_2.iter().enumerate() {
        if *event_type == odf::metadata::MetadataEventTypeFlags::ADD_DATA {
            assert_eq!(*seq_num, 153 - i as u64); // Should be 153, 152, 151, ..., 4
        } else if *event_type == odf::metadata::MetadataEventTypeFlags::SET_INFO {
            // SET_INFO blocks should be at sequence 3 and 1
            assert!(*seq_num == 1);
        } else {
            panic!("Unexpected event type encountered");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// iter_blocks_interval Tests - Testing caching behavior without hints
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iter_blocks_interval_full_chain() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    // Iterate through entire chain from head to genesis without any tail limit
    let iteration_order = harness
        .iter_blocks_interval_through(foo_created.dataset.as_ref(), &foo_created.head, None, false)
        .await;

    // Should return all blocks in reverse chronological order (newest first)
    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (5, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (4, odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE),
            (3, odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA),
            (2, odf::metadata::MetadataEventTypeFlags::SET_LICENSE),
            (1, odf::metadata::MetadataEventTypeFlags::SET_INFO),
            (0, odf::metadata::MetadataEventTypeFlags::SEED),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iter_blocks_interval_with_tail_boundary() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    // Get block 5's hash to use as tail boundary
    let (block_5_hash, _block_5) = harness
        .get_block_by_sequence(foo_created.dataset.as_ref(), 5)
        .await;

    // Iterate from head to block 5 (exclusive - block 5 should not be included)
    let iteration_order = harness
        .iter_blocks_interval_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            Some(&block_5_hash),
            false,
        )
        .await;

    // Should return blocks 7 and 6 only (tail boundary is exclusive)
    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iter_blocks_interval_single_block() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    // Get head block to use as both head and tail (tail is exclusive)
    let head_block = foo_created
        .dataset
        .as_metadata_chain()
        .get_block(&foo_created.head)
        .await
        .unwrap();

    // Use previous block as tail boundary to get only the head block
    let tail_hash = head_block.prev_block_hash.as_ref().unwrap();

    let iteration_order = harness
        .iter_blocks_interval_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            Some(tail_hash),
            false,
        )
        .await;

    // Should return only the head block
    pretty_assertions::assert_eq!(
        iteration_order,
        vec![(7, odf::metadata::MetadataEventTypeFlags::ADD_DATA),]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iter_blocks_interval_page_caching_stress() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("large"));
    let large_created = harness.create_dataset_with_many_data_blocks(&alias).await;

    // Test full chain iteration to see caching behavior with many blocks
    let iteration_order = harness
        .iter_blocks_interval_through(
            large_created.dataset.as_ref(),
            &large_created.head,
            None,
            false,
        )
        .await;

    // Should return all blocks: 150 data blocks + 3 setup blocks + 1 seed = 154
    // total
    assert_eq!(iteration_order.len(), 154);

    // Verify all blocks are in descending order
    for (i, (seq_num, _)) in iteration_order.iter().enumerate() {
        assert_eq!(*seq_num, 153 - i as u64); // Should be 153, 152, 151, ..., 0
    }

    // First 150 should be ADD_DATA blocks
    for (seq_num, event_type) in iteration_order.iter().take(150) {
        assert_eq!(*event_type, odf::metadata::MetadataEventTypeFlags::ADD_DATA);
        assert!(*seq_num >= 4); // Data blocks start from sequence 4
    }

    // Last block should be SEED
    let (last_seq, last_event) = iteration_order.last().unwrap();
    assert_eq!(*last_seq, 0);
    assert_eq!(*last_event, odf::metadata::MetadataEventTypeFlags::SEED);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iter_blocks_interval_with_specific_range() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("large"));
    let large_created = harness.create_dataset_with_many_data_blocks(&alias).await;

    // Get specific blocks to define a range
    let (block_100_hash, _block_100) = harness
        .get_block_by_sequence(large_created.dataset.as_ref(), 100)
        .await;

    let (block_50_hash, _block_50) = harness
        .get_block_by_sequence(large_created.dataset.as_ref(), 50)
        .await;

    // Test iteration from block 100 to block 50 (exclusive)
    let iteration_order = harness
        .iter_blocks_interval_through(
            large_created.dataset.as_ref(),
            &block_100_hash,
            Some(&block_50_hash),
            false,
        )
        .await;

    // Should return blocks 100 down to 51 (50 blocks total)
    assert_eq!(iteration_order.len(), 50);

    // Verify sequence and that all are ADD_DATA
    for (i, (seq_num, event_type)) in iteration_order.iter().enumerate() {
        assert_eq!(*seq_num, 100 - i as u64); // Should be 100, 99, 98, ..., 51
        assert_eq!(*event_type, odf::metadata::MetadataEventTypeFlags::ADD_DATA);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iter_blocks_interval_across_page_boundary() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("large"));
    let large_created = harness.create_dataset_with_many_data_blocks(&alias).await;

    // Test iteration that crosses the 100-block page boundary
    // Start from block 120 and go down to block 80 (crossing the page cache
    // boundary)
    let (block_120_hash, _block_120) = harness
        .get_block_by_sequence(large_created.dataset.as_ref(), 120)
        .await;

    let (block_80_hash, _block_80) = harness
        .get_block_by_sequence(large_created.dataset.as_ref(), 80)
        .await;

    let iteration_order = harness
        .iter_blocks_interval_through(
            large_created.dataset.as_ref(),
            &block_120_hash,
            Some(&block_80_hash),
            false,
        )
        .await;

    // Should return blocks 120 down to 81 (40 blocks total)
    assert_eq!(iteration_order.len(), 40);

    // Verify sequence and that all are ADD_DATA
    for (i, (seq_num, event_type)) in iteration_order.iter().enumerate() {
        assert_eq!(*seq_num, 120 - i as u64); // Should be 120, 119, 118, ..., 81
        assert_eq!(*event_type, odf::metadata::MetadataEventTypeFlags::ADD_DATA);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iter_blocks_interval_mixed_block_types() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("derivative"));
    let derivative_created = harness.create_derivative_dataset(&alias).await;

    // Iterate through entire derivative dataset chain
    let iteration_order = harness
        .iter_blocks_interval_through(
            derivative_created.dataset.as_ref(),
            &derivative_created.head,
            None,
            false,
        )
        .await;

    // Should return all blocks including EXECUTE_TRANSFORM and setup blocks
    // Derivative dataset has: 2 EXECUTE_TRANSFORM + 3 setup + 1 seed = 6 total
    assert_eq!(iteration_order.len(), 6);

    // Verify the expected sequence of block types
    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (5, odf::metadata::MetadataEventTypeFlags::EXECUTE_TRANSFORM),
            (4, odf::metadata::MetadataEventTypeFlags::EXECUTE_TRANSFORM),
            (3, odf::metadata::MetadataEventTypeFlags::SET_TRANSFORM),
            (2, odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA),
            (1, odf::metadata::MetadataEventTypeFlags::SET_INFO),
            (0, odf::metadata::MetadataEventTypeFlags::SEED),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iter_blocks_interval_reverse_interval_error() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("large"));
    let large_created = harness.create_dataset_with_many_data_blocks(&alias).await;

    // Test reverse interval: from block 80 to block 120 (invalid - reverse order)
    // This should fail because we're trying to go from earlier to later block
    let (block_80_hash, _block_80) = harness
        .get_block_by_sequence(large_created.dataset.as_ref(), 80)
        .await;

    let (block_120_hash, _block_120) = harness
        .get_block_by_sequence(large_created.dataset.as_ref(), 120)
        .await;

    // Test with ignore_missing_tail = false (should error)
    let result = harness
        .try_iter_blocks_interval_through(
            large_created.dataset.as_ref(),
            &block_80_hash,
            Some(&block_120_hash),
            false,
        )
        .await;

    // Should get an error because block 120 is not reachable from block 80 going
    // backwards
    assert!(result.is_err(), "Expected error for reverse interval");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iter_blocks_interval_reverse_interval_ignore_missing_tail() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("large"));
    let large_created = harness.create_dataset_with_many_data_blocks(&alias).await;

    // Test reverse interval: from block 80 to block 120 (invalid - reverse order)
    // This time with ignore_missing_tail = true (should not error)
    let (block_80_hash, _block_80) = harness
        .get_block_by_sequence(large_created.dataset.as_ref(), 80)
        .await;

    let (block_120_hash, _block_120) = harness
        .get_block_by_sequence(large_created.dataset.as_ref(), 120)
        .await;

    // Test with ignore_missing_tail = true (should iterate from 80 to genesis)
    let iteration_order = harness
        .iter_blocks_interval_through(
            large_created.dataset.as_ref(),
            &block_80_hash,
            Some(&block_120_hash),
            true, // ignore_missing_tail = true
        )
        .await;

    // Should return blocks from 80 down to 0 (genesis) since tail is not
    // encountered and ignored
    assert_eq!(iteration_order.len(), 81); // blocks 80, 79, 78, ..., 0

    // Verify sequence and that all are in descending order
    for (i, (seq_num, _event_type)) in iteration_order.iter().enumerate() {
        assert_eq!(*seq_num, 80 - i as u64); // Should be 80, 79, 78, ..., 0
    }

    // First block should be at sequence 80
    assert_eq!(iteration_order.first().unwrap().0, 80);

    // Last block should be SEED at sequence 0
    let (last_seq, last_event) = iteration_order.last().unwrap();
    assert_eq!(*last_seq, 0);
    assert_eq!(*last_event, odf::metadata::MetadataEventTypeFlags::SEED);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iter_blocks_interval_nonexistent_tail_error() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    // Create a fake hash that doesn't exist in the chain
    let fake_hash = odf::Multihash::from_digest_sha3_256(b"nonexistent-block");

    // Test with ignore_missing_tail = false (should error)
    let result = harness
        .try_iter_blocks_interval_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            Some(&fake_hash),
            false,
        )
        .await;

    // Should get an error because the fake hash is not reachable
    assert!(result.is_err(), "Expected error for nonexistent tail");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iter_blocks_interval_nonexistent_tail_ignore() {
    let harness = IterateWithHintsHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_root_dataset(&alias).await;

    // Create a fake hash that doesn't exist in the chain
    let fake_hash = odf::Multihash::from_digest_sha3_256(b"nonexistent-block");

    // Test with ignore_missing_tail = true (should iterate to genesis)
    let iteration_order = harness
        .iter_blocks_interval_through(
            foo_created.dataset.as_ref(),
            &foo_created.head,
            Some(&fake_hash),
            true, // ignore_missing_tail = true
        )
        .await;

    // Should return all blocks from head to genesis since tail is not found and
    // ignored
    pretty_assertions::assert_eq!(
        iteration_order,
        vec![
            (7, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (5, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (4, odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE),
            (3, odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA),
            (2, odf::metadata::MetadataEventTypeFlags::SET_LICENSE),
            (1, odf::metadata::MetadataEventTypeFlags::SET_INFO),
            (0, odf::metadata::MetadataEventTypeFlags::SEED),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct IterateWithHintsHarness {
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    create_dataset_use_case: Arc<dyn CreateDatasetFromSnapshotUseCase>,
}

impl IterateWithHintsHarness {
    async fn new() -> Self {
        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts::default()).await;

        let mut b =
            CatalogBuilder::new_chained(dataset_base_use_case_harness.intermediate_catalog());

        let catalog = b
            .add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<CreateDatasetUseCaseHelper>()
            .build();

        Self {
            dataset_base_use_case_harness,
            create_dataset_use_case: catalog.get_one().unwrap(),
        }
    }

    async fn create_root_dataset(&self, alias: &odf::DatasetAlias) -> CreateDatasetResult {
        use odf::metadata::testing::MetadataFactory;
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(
                MetadataFactory::set_info()
                    .description("test")
                    .keyword("xxx")
                    .keyword("yyy")
                    .build(),
            )
            .push_event(MetadataFactory::set_license().build())
            .push_event(MetadataFactory::set_data_schema().build())
            .push_event(MetadataFactory::set_polling_source().build())
            .build();

        let create_result = self
            .create_dataset_use_case
            .execute(snapshot, CreateDatasetUseCaseOptions::default())
            .await
            .unwrap();

        create_result
            .dataset
            .commit_event(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(0, 9)
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-1"),
                        size: 1,
                    }))
                    .build()
                    .into(),
                odf::dataset::CommitOpts {
                    check_object_refs: false, // Cheating a little
                    ..odf::dataset::CommitOpts::default()
                },
            )
            .await
            .unwrap();

        create_result
            .dataset
            .commit_event(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(10, 14)
                    .prev_checkpoint(Some(odf::Multihash::from_digest_sha3_256(b"checkpoint-1")))
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-2"),
                        size: 1,
                    }))
                    .build()
                    .into(),
                odf::dataset::CommitOpts {
                    check_object_refs: false, // Cheating a little
                    ..odf::dataset::CommitOpts::default()
                },
            )
            .await
            .unwrap();

        let commit_result = create_result
            .dataset
            .commit_event(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(15, 19)
                    .prev_checkpoint(Some(odf::Multihash::from_digest_sha3_256(b"checkpoint-2")))
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-3"),
                        size: 1,
                    }))
                    .build()
                    .into(),
                odf::dataset::CommitOpts {
                    check_object_refs: false, // Cheating a little
                    ..odf::dataset::CommitOpts::default()
                },
            )
            .await
            .unwrap();

        CreateDatasetResult {
            dataset: create_result.dataset,
            dataset_handle: create_result.dataset_handle,
            head: commit_result.new_head,
        }
    }

    async fn create_derivative_dataset(&self, alias: &odf::DatasetAlias) -> CreateDatasetResult {
        // First create a root dataset to use as input
        let input_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("input"));
        let input_created = self.create_root_dataset(&input_alias).await;

        use odf::metadata::testing::MetadataFactory;
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(odf::DatasetKind::Derivative)
            .push_event(
                MetadataFactory::set_info()
                    .description("derivative test")
                    .build(),
            )
            .push_event(MetadataFactory::set_data_schema().build())
            .push_event(
                MetadataFactory::set_transform()
                    .inputs_from_refs([&input_alias])
                    .build(),
            )
            .build();

        let create_result = self
            .create_dataset_use_case
            .execute(snapshot, CreateDatasetUseCaseOptions::default())
            .await
            .unwrap();

        // Add two EXECUTE_TRANSFORM events
        create_result
            .dataset
            .commit_event(
                MetadataFactory::execute_transform()
                    .empty_query_inputs_from_particular_ids([input_created
                        .dataset_handle
                        .id
                        .clone()])
                    .some_new_data_with_offset(0, 9)
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(
                            b"transform-checkpoint-1",
                        ),
                        size: 1,
                    }))
                    .build()
                    .into(),
                odf::dataset::CommitOpts {
                    check_object_refs: false,
                    ..odf::dataset::CommitOpts::default()
                },
            )
            .await
            .unwrap();

        let commit_result = create_result
            .dataset
            .commit_event(
                MetadataFactory::execute_transform()
                    .empty_query_inputs_from_particular_ids([input_created
                        .dataset_handle
                        .id
                        .clone()])
                    .some_new_data_with_offset(10, 19)
                    .prev_checkpoint(Some(odf::Multihash::from_digest_sha3_256(
                        b"transform-checkpoint-1",
                    )))
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(
                            b"transform-checkpoint-2",
                        ),
                        size: 1,
                    }))
                    .build()
                    .into(),
                odf::dataset::CommitOpts {
                    check_object_refs: false,
                    ..odf::dataset::CommitOpts::default()
                },
            )
            .await
            .unwrap();

        CreateDatasetResult {
            dataset: create_result.dataset,
            dataset_handle: create_result.dataset_handle,
            head: commit_result.new_head,
        }
    }

    async fn create_dataset_with_many_data_blocks(
        &self,
        alias: &odf::DatasetAlias,
    ) -> CreateDatasetResult {
        use odf::metadata::testing::MetadataFactory;
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(
                MetadataFactory::set_info()
                    .description("large dataset with many data blocks")
                    .build(),
            )
            .push_event(MetadataFactory::set_data_schema().build())
            .push_event(MetadataFactory::set_license().build())
            .build();

        let create_result = self
            .create_dataset_use_case
            .execute(snapshot, CreateDatasetUseCaseOptions::default())
            .await
            .unwrap();

        // Add 150 ADD_DATA events to test page caching across the 100-block boundary
        let mut last_head = create_result.head.clone();
        let mut prev_checkpoint: Option<odf::Multihash> = None;

        for i in 0..150 {
            let start_offset = i * 10;
            let end_offset = start_offset + 9;

            let current_checkpoint =
                odf::Multihash::from_digest_sha3_256(format!("checkpoint-{i}").as_bytes());

            let mut add_data_builder = MetadataFactory::add_data()
                .some_new_data_with_offset(start_offset, end_offset)
                .new_checkpoint(Some(odf::Checkpoint {
                    physical_hash: current_checkpoint.clone(),
                    size: 1,
                }));

            // Chain checkpoints properly
            if let Some(prev_cp) = prev_checkpoint.as_ref() {
                add_data_builder = add_data_builder.prev_checkpoint(Some(prev_cp.clone()));
            }

            let commit_result = create_result
                .dataset
                .commit_event(
                    add_data_builder.build().into(),
                    odf::dataset::CommitOpts {
                        check_object_refs: false,
                        ..odf::dataset::CommitOpts::default()
                    },
                )
                .await
                .unwrap();

            last_head = commit_result.new_head;
            prev_checkpoint = Some(current_checkpoint);
        }

        CreateDatasetResult {
            dataset: create_result.dataset,
            dataset_handle: create_result.dataset_handle,
            head: last_head,
        }
    }

    async fn iterate_through<F>(
        &self,
        dataset: &dyn odf::Dataset,
        head: &odf::Multihash,
        maybe_tail_sequence_number: Option<u64>,
        mut hint_callback: F,
    ) -> Vec<(u64, odf::metadata::MetadataEventTypeFlags)>
    where
        F: FnMut(u64) -> odf::dataset::MetadataVisitorDecision,
    {
        let mut current_block = dataset.as_metadata_chain().get_block(head).await.unwrap();

        let mut iteration_order = Vec::new();
        loop {
            let hint = hint_callback(current_block.sequence_number);
            iteration_order.push((
                current_block.sequence_number,
                odf::metadata::MetadataEventTypeFlags::from(&current_block.event),
            ));

            if hint == odf::dataset::MetadataVisitorDecision::Stop {
                break;
            }

            let Some((_, block)) = dataset
                .as_metadata_chain()
                .get_preceding_block_with_hint(&current_block, maybe_tail_sequence_number, hint)
                .await
                .unwrap()
            else {
                break;
            };

            current_block = block;
        }

        iteration_order
    }

    async fn iter_blocks_interval_through(
        &self,
        dataset: &dyn odf::Dataset,
        head_boundary: &odf::Multihash,
        tail_boundary: Option<&odf::Multihash>,
        ignore_missing_tail: bool,
    ) -> Vec<(u64, odf::metadata::MetadataEventTypeFlags)> {
        use futures::StreamExt;

        let mut stream = dataset.as_metadata_chain().iter_blocks_interval(
            head_boundary.into(),
            tail_boundary.map(Into::into),
            ignore_missing_tail,
        );

        let mut iteration_order = Vec::new();
        while let Some(block_result) = stream.next().await {
            let (_hash, block) = block_result.unwrap();
            iteration_order.push((
                block.sequence_number,
                odf::metadata::MetadataEventTypeFlags::from(&block.event),
            ));
        }

        iteration_order
    }

    async fn try_iter_blocks_interval_through(
        &self,
        dataset: &dyn odf::Dataset,
        head_boundary: &odf::Multihash,
        tail_boundary: Option<&odf::Multihash>,
        ignore_missing_tail: bool,
    ) -> Result<Vec<(u64, odf::metadata::MetadataEventTypeFlags)>, Box<dyn std::error::Error>> {
        use futures::StreamExt;

        let mut stream = dataset.as_metadata_chain().iter_blocks_interval(
            head_boundary.into(),
            tail_boundary.map(Into::into),
            ignore_missing_tail,
        );

        let mut iteration_order = Vec::new();
        while let Some(block_result) = stream.next().await {
            match block_result {
                Ok((_hash, block)) => {
                    iteration_order.push((
                        block.sequence_number,
                        odf::metadata::MetadataEventTypeFlags::from(&block.event),
                    ));
                }
                Err(e) => {
                    return Err(Box::new(e));
                }
            }
        }

        Ok(iteration_order)
    }

    async fn get_block_by_sequence(
        &self,
        dataset: &dyn odf::Dataset,
        sequence_number: u64,
    ) -> (odf::Multihash, odf::MetadataBlock) {
        use futures::StreamExt;

        let mut stream = dataset.as_metadata_chain().iter_blocks_interval(
            (&odf::dataset::BlockRef::Head).into(),
            None,
            false,
        );

        while let Some(block_result) = stream.next().await {
            let (hash, block) = block_result.unwrap();
            if block.sequence_number == sequence_number {
                return (hash, block);
            }
        }

        panic!("Block with sequence number {sequence_number} not found");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
