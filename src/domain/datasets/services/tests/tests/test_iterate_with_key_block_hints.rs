// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
    let harness = IterateKeyBlocksHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_test_dataset(&alias).await;

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
    let harness = IterateKeyBlocksHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_test_dataset(&alias).await;

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
        vec![(6, odf::metadata::MetadataEventTypeFlags::ADD_DATA)] /* first is unconditional */
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_static_key_blocks() {
    let harness = IterateKeyBlocksHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_test_dataset(&alias).await;

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
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (3, odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA),
            (1, odf::metadata::MetadataEventTypeFlags::SET_INFO),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_dynamic_key_blocks() {
    let harness = IterateKeyBlocksHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_test_dataset(&alias).await;

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
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (2, odf::metadata::MetadataEventTypeFlags::SET_LICENSE),
            (1, odf::metadata::MetadataEventTypeFlags::SET_INFO),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_missing_key_block() {
    let harness = IterateKeyBlocksHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_test_dataset(&alias).await;

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
        vec![(6, odf::metadata::MetadataEventTypeFlags::ADD_DATA), /* first is unconditional */]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_partially_missing_key_blocks() {
    let harness = IterateKeyBlocksHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_test_dataset(&alias).await;

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
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA), /* first is unconditional */
            (4, odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_with_data() {
    let harness = IterateKeyBlocksHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_test_dataset(&alias).await;

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
                        | odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE,
                )
            },
        )
        .await;

    // Note: ADD_DATA is not a key block, it's not indexed, so we scan all blocks
    pretty_assertions::assert_eq!(
        iteration_order_1,
        vec![
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (5, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (4, odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE),
            (3, odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA),
            (2, odf::metadata::MetadataEventTypeFlags::SET_LICENSE),
            (1, odf::metadata::MetadataEventTypeFlags::SET_INFO),
            (0, odf::metadata::MetadataEventTypeFlags::SEED),
        ]
    );

    // With key blocks or without, ADD_DATA block forces to scan the whole chain
    assert_eq!(iteration_order_1, iteration_order_2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_new_key_blocks_after_data() {
    let harness = IterateKeyBlocksHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_test_dataset(&alias).await;

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
            (8, odf::metadata::MetadataEventTypeFlags::SET_LICENSE), // first always recorded
            (7, odf::metadata::MetadataEventTypeFlags::SET_INFO),
            (1, odf::metadata::MetadataEventTypeFlags::SET_INFO),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_new_key_blocks_between_data_parts() {
    let harness = IterateKeyBlocksHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_test_dataset(&alias).await;

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
                    .some_new_data_with_offset(15, 20)
                    .prev_checkpoint(Some(odf::Multihash::from_digest_sha3_256(b"checkpoint-2")))
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-3"),
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
            (9, odf::metadata::MetadataEventTypeFlags::ADD_DATA), // first always recorded
            (8, odf::metadata::MetadataEventTypeFlags::SET_LICENSE),
            (2, odf::metadata::MetadataEventTypeFlags::SET_LICENSE),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_limited_tail_next() {
    let harness = IterateKeyBlocksHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_test_dataset(&alias).await;

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
    let harness = IterateKeyBlocksHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_test_dataset(&alias).await;

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
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA), // fetched always
            (4, odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE),
        ]
    );

    pretty_assertions::assert_eq!(
        iteration_order_2,
        vec![
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA), // fetched always
            (4, odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE),
            (2, odf::metadata::MetadataEventTypeFlags::SET_LICENSE),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_limit_both_tail_and_head() {
    let harness = IterateKeyBlocksHarness::new().await;

    let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = harness.create_test_dataset(&alias).await;

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
            (4, odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE), // fetched always
            (3, odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct IterateKeyBlocksHarness {
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
}

impl IterateKeyBlocksHarness {
    async fn new() -> Self {
        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts::default()).await;

        Self {
            dataset_base_use_case_harness,
        }
    }

    async fn create_test_dataset(&self, alias: &odf::DatasetAlias) -> CreateDatasetResult {
        let mut b = CatalogBuilder::new_chained(self.catalog());
        b.add::<CreateDatasetFromSnapshotUseCaseImpl>();
        b.add::<CreateDatasetUseCaseHelper>();

        let catalog = b.build();
        let use_case = catalog
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

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

        let create_result = use_case
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

        let commit_result = create_result
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

        CreateDatasetResult {
            dataset: create_result.dataset,
            dataset_handle: create_result.dataset_handle,
            head: commit_result.new_head,
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
