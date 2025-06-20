// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use kamu::testing::BaseRepoHarness;
use kamu_core::TenancyConfig;
use kamu_datasets::{DatasetIncrementQueryService, DatasetIntervalIncrement};
use kamu_datasets_services::DatasetIncrementQueryServiceImpl;
use odf::metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_initial_increment() {
    let harness = DatasetIncrementQueryServiceHarness::new();

    let foo = harness
        .create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::new_unchecked("foo"),
        ))
        .await;

    // "foo" initially has Seed and SetPollingSource events

    let increment_between = harness
        .dataset_increment_query_service
        .get_increment_between(&foo.dataset_handle.id, None, &foo.head)
        .await
        .unwrap();
    assert_eq!(
        increment_between,
        DatasetIntervalIncrement {
            num_blocks: 2,
            num_records: 0,
            updated_watermark: None
        }
    );

    let increment_since = harness
        .dataset_increment_query_service
        .get_increment_since(&foo.dataset_handle.id, None)
        .await
        .unwrap();
    assert_eq!(
        increment_since,
        DatasetIntervalIncrement {
            num_blocks: 2,
            num_records: 0,
            updated_watermark: None
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_no_changes_with_same_bounds() {
    let harness = DatasetIncrementQueryServiceHarness::new();

    let foo = harness
        .create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::new_unchecked("foo"),
        ))
        .await;

    let increment_between = harness
        .dataset_increment_query_service
        .get_increment_between(&foo.dataset_handle.id, Some(&foo.head), &foo.head)
        .await
        .unwrap();
    assert_eq!(increment_between, DatasetIntervalIncrement::default());

    let increment_since = harness
        .dataset_increment_query_service
        .get_increment_since(&foo.dataset_handle.id, Some(&foo.head))
        .await
        .unwrap();
    assert_eq!(increment_since, DatasetIntervalIncrement::default());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_add_data_differences() {
    let harness = DatasetIncrementQueryServiceHarness::new();

    let foo = harness
        .create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::new_unchecked("foo"),
        ))
        .await;

    // Commit SetDataSchema and 2 data nodes

    let commit_result_1 = foo
        .dataset
        .commit_event(
            odf::MetadataEvent::SetDataSchema(MetadataFactory::set_data_schema().build()),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let new_watermark_time = Utc::now();

    let commit_result_2 = foo
        .dataset
        .commit_event(
            odf::MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(0, 9)
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-1"),
                        size: 1,
                    }))
                    .some_new_source_state()
                    .build(),
            ),
            odf::dataset::CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let commit_result_3 = foo
        .dataset
        .commit_event(
            odf::MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(10, 14)
                    .prev_checkpoint(Some(odf::Multihash::from_digest_sha3_256(b"checkpoint-1")))
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-2"),
                        size: 1,
                    }))
                    .new_watermark(Some(new_watermark_time))
                    .some_new_source_state()
                    .build(),
            ),
            odf::dataset::CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let between_cases = [
        // SetPollingSource -> SetDataSchema
        (
            Some(commit_result_1.old_head.as_ref().unwrap()),
            &commit_result_1.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 0,
                updated_watermark: None,
            },
        ),
        // SetDataSchema -> odf::metadata::AddData #1
        (
            Some(&commit_result_1.new_head),
            &commit_result_2.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 10,
                updated_watermark: None,
            },
        ),
        // odf::metadata::AddData #1 -> odf::metadata::AddData #2
        (
            Some(&commit_result_2.new_head),
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // SetDataSchema -> odf::metadata::AddData #2
        (
            Some(&commit_result_1.new_head),
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 2,
                num_records: 15,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // SetPollingSource -> odf::metadata::AddData #1
        (
            Some(commit_result_1.old_head.as_ref().unwrap()),
            &commit_result_2.new_head,
            DatasetIntervalIncrement {
                num_blocks: 2,
                num_records: 10,
                updated_watermark: None,
            },
        ),
        // SetPollingSource -> odf::metadata::AddData #2
        (
            Some(commit_result_1.old_head.as_ref().unwrap()),
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 3,
                num_records: 15,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // Initial -> odf::metadata::AddData #2
        (
            None,
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 5,
                num_records: 15,
                updated_watermark: Some(new_watermark_time),
            },
        ),
    ];

    harness
        .check_between_cases(&foo.dataset_handle.id, &between_cases)
        .await;

    let since_cases = [
        // Since beginning
        (
            None,
            DatasetIntervalIncrement {
                num_blocks: 5,
                num_records: 15,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // Since SetPollingSource
        (
            Some(commit_result_1.old_head.as_ref().unwrap()),
            DatasetIntervalIncrement {
                num_blocks: 3,
                num_records: 15,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // Since SetDataSchema
        (
            Some(&commit_result_1.new_head),
            DatasetIntervalIncrement {
                num_blocks: 2,
                num_records: 15,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // Since odf::metadata::AddData #1
        (
            Some(&commit_result_2.new_head),
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: Some(new_watermark_time),
            },
        ),
    ];

    harness
        .check_since_cases(&foo.dataset_handle.id, &since_cases)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_execute_transform_differences() {
    let harness = DatasetIncrementQueryServiceHarness::new();

    let foo = harness
        .create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::new_unchecked("foo"),
        ))
        .await;
    let bar = harness
        .create_derived_dataset(
            &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar")),
            vec![foo.dataset_handle.as_local_ref()],
        )
        .await;

    // Commit SetDataSchema and 2 trasnform data nodes

    let new_watermark_time = Utc::now();

    let commit_result_1 = bar
        .dataset
        .commit_event(
            odf::MetadataEvent::SetDataSchema(MetadataFactory::set_data_schema().build()),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let commit_result_2 = bar
        .dataset
        .commit_event(
            odf::MetadataEvent::ExecuteTransform(
                MetadataFactory::execute_transform()
                    .empty_query_inputs_from_particular_ids([foo.dataset_handle.id.clone()])
                    .some_new_data_with_offset(0, 14)
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-1"),
                        size: 1,
                    }))
                    .build(),
            ),
            odf::dataset::CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let commit_result_3 = bar
        .dataset
        .commit_event(
            odf::MetadataEvent::ExecuteTransform(
                MetadataFactory::execute_transform()
                    .empty_query_inputs_from_particular_ids([foo.dataset_handle.id.clone()])
                    .some_new_data_with_offset(15, 19)
                    .prev_checkpoint(Some(odf::Multihash::from_digest_sha3_256(b"checkpoint-1")))
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-2"),
                        size: 1,
                    }))
                    .new_watermark(Some(new_watermark_time))
                    .build(),
            ),
            odf::dataset::CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let between_cases = [
        // SetTransform -> SetDataSchema
        (
            Some(commit_result_1.old_head.as_ref().unwrap()),
            &commit_result_1.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 0,
                updated_watermark: None,
            },
        ),
        // SetDataSchema -> ExecuteTransform #1
        (
            Some(commit_result_2.old_head.as_ref().unwrap()),
            &commit_result_2.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 15,
                updated_watermark: None,
            },
        ),
        // ExecuteTransform #1 -> ExecuteTransform #2
        (
            Some(commit_result_3.old_head.as_ref().unwrap()),
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // SetDataSchema -> ExecuteTransform #2
        (
            Some(&commit_result_1.new_head),
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 2,
                num_records: 20,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // SetTransform -> ExecuteTransform #1
        (
            Some(commit_result_1.old_head.as_ref().unwrap()),
            &commit_result_2.new_head,
            DatasetIntervalIncrement {
                num_blocks: 2,
                num_records: 15,
                updated_watermark: None,
            },
        ),
        // SetTransform -> ExecuteTransform #2
        (
            Some(commit_result_1.old_head.as_ref().unwrap()),
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 3,
                num_records: 20,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // Initial -> ExecuteTransform #2
        (
            None,
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 5,
                num_records: 20,
                updated_watermark: Some(new_watermark_time),
            },
        ),
    ];

    harness
        .check_between_cases(&bar.dataset_handle.id, &between_cases)
        .await;

    let since_cases = [
        // Since beginning
        (
            None,
            DatasetIntervalIncrement {
                num_blocks: 5,
                num_records: 20,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // Since SetTransform
        (
            Some(commit_result_1.old_head.as_ref().unwrap()),
            DatasetIntervalIncrement {
                num_blocks: 3,
                num_records: 20,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // Since SetDataSchema
        (
            Some(&commit_result_1.new_head),
            DatasetIntervalIncrement {
                num_blocks: 2,
                num_records: 20,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // Since ExecuteTransform #1
        (
            Some(&commit_result_2.new_head),
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: Some(new_watermark_time),
            },
        ),
    ];

    harness
        .check_since_cases(&bar.dataset_handle.id, &since_cases)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_multiple_watermarks_within_interval() {
    let harness = DatasetIncrementQueryServiceHarness::new();

    let foo = harness
        .create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::new_unchecked("foo"),
        ))
        .await;

    // Commit SetDataSchema and 2 data nodes each having a watermark

    let commit_result_1 = foo
        .dataset
        .commit_event(
            odf::MetadataEvent::SetDataSchema(MetadataFactory::set_data_schema().build()),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let watermark_1_time = Utc::now();

    let commit_result_2 = foo
        .dataset
        .commit_event(
            odf::MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(0, 9)
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-1"),
                        size: 1,
                    }))
                    .new_watermark(Some(watermark_1_time))
                    .some_new_source_state()
                    .build(),
            ),
            odf::dataset::CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let watermark_2_time = Utc::now();

    let commit_result_3 = foo
        .dataset
        .commit_event(
            odf::MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(10, 24)
                    .prev_checkpoint(Some(odf::Multihash::from_digest_sha3_256(b"checkpoint-1")))
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-2"),
                        size: 1,
                    }))
                    .new_watermark(Some(watermark_2_time))
                    .some_new_source_state()
                    .build(),
            ),
            odf::dataset::CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let between_cases = [
        // SetPollingSource -> SetDataSchema
        (
            Some(commit_result_1.old_head.as_ref().unwrap()),
            &commit_result_1.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 0,
                updated_watermark: None,
            },
        ),
        // SetDataSchema -> odf::metadata::AddData #1
        (
            Some(commit_result_2.old_head.as_ref().unwrap()),
            &commit_result_2.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 10,
                updated_watermark: Some(watermark_1_time),
            },
        ),
        // odf::metadata::AddData #1 -> odf::metadata::AddData #2
        (
            Some(commit_result_3.old_head.as_ref().unwrap()),
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 15,
                updated_watermark: Some(watermark_2_time),
            },
        ),
        // Initial -> odf::metadata::AddData #2
        (
            None,
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 5,
                num_records: 25,
                updated_watermark: Some(watermark_2_time),
            },
        ),
    ];

    harness
        .check_between_cases(&foo.dataset_handle.id, &between_cases)
        .await;

    let since_cases = [
        // Since beginning
        (
            None,
            DatasetIntervalIncrement {
                num_blocks: 5,
                num_records: 25,
                updated_watermark: Some(watermark_2_time),
            },
        ),
        // Since odf::metadata::AddData #1
        (
            Some(&commit_result_2.new_head),
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 15,
                updated_watermark: Some(watermark_2_time),
            },
        ),
    ];

    harness
        .check_since_cases(&foo.dataset_handle.id, &since_cases)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_older_watermark_before_interval() {
    let harness = DatasetIncrementQueryServiceHarness::new();

    let foo = harness
        .create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::new_unchecked("foo"),
        ))
        .await;

    // Commit SetDataSchema and 3 data nodes, with #1,3 containing watermark

    foo.dataset
        .commit_event(
            odf::MetadataEvent::SetDataSchema(MetadataFactory::set_data_schema().build()),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let watermark_1_time = Utc::now();

    let commit_result_2 = foo
        .dataset
        .commit_event(
            odf::MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(0, 9)
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-1"),
                        size: 1,
                    }))
                    .new_watermark(Some(watermark_1_time))
                    .some_new_source_state()
                    .build(),
            ),
            odf::dataset::CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let commit_result_3 = foo
        .dataset
        .commit_event(
            odf::MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(10, 24)
                    .prev_checkpoint(Some(odf::Multihash::from_digest_sha3_256(b"checkpoint-1")))
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-2"),
                        size: 1,
                    }))
                    .new_watermark(Some(watermark_1_time))
                    .some_new_source_state()
                    .build(),
            ),
            odf::dataset::CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let watermark_2_time = Utc::now();

    let commit_result_4 = foo
        .dataset
        .commit_event(
            odf::MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(25, 36)
                    .prev_checkpoint(Some(odf::Multihash::from_digest_sha3_256(b"checkpoint-2")))
                    .new_checkpoint(Some(odf::Checkpoint {
                        physical_hash: odf::Multihash::from_digest_sha3_256(b"checkpoint-3"),
                        size: 1,
                    }))
                    .new_watermark(Some(watermark_2_time))
                    .some_new_source_state()
                    .build(),
            ),
            odf::dataset::CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let between_cases = [
        // SetDataSchema -> odf::metadata::AddData #1
        (
            Some(commit_result_2.old_head.as_ref().unwrap()),
            &commit_result_2.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 10,
                updated_watermark: Some(watermark_1_time),
            },
        ),
        // odf::metadata::AddData #1 -> odf::metadata::AddData #2
        (
            Some(commit_result_3.old_head.as_ref().unwrap()),
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 15,
                updated_watermark: None,
            },
        ),
        // odf::metadata::AddData #2 -> odf::metadata::AddData #3
        (
            Some(commit_result_4.old_head.as_ref().unwrap()),
            &commit_result_4.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 12,
                updated_watermark: Some(watermark_2_time),
            },
        ),
        // Initial -> odf::metadata::AddData #2
        (
            None,
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 5,
                num_records: 25,
                updated_watermark: Some(watermark_1_time),
            },
        ),
        // Initial -> odf::metadata::AddData #3
        (
            None,
            &commit_result_4.new_head,
            DatasetIntervalIncrement {
                num_blocks: 6,
                num_records: 37,
                updated_watermark: Some(watermark_2_time),
            },
        ),
    ];

    harness
        .check_between_cases(&foo.dataset_handle.id, &between_cases)
        .await;

    let since_cases = [
        // Since beginning
        (
            None,
            DatasetIntervalIncrement {
                num_blocks: 6,
                num_records: 37,
                updated_watermark: Some(watermark_2_time),
            },
        ),
        // Since odf::metadata::AddData #1
        (
            Some(&commit_result_2.new_head),
            DatasetIntervalIncrement {
                num_blocks: 2,
                num_records: 27,
                updated_watermark: Some(watermark_2_time),
            },
        ),
    ];

    harness
        .check_since_cases(&foo.dataset_handle.id, &since_cases)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct DatasetIncrementQueryServiceHarness {
    base_repo_harness: BaseRepoHarness,
    dataset_increment_query_service: Arc<dyn DatasetIncrementQueryService>,
}

impl DatasetIncrementQueryServiceHarness {
    fn new() -> Self {
        let base_repo_harness = BaseRepoHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<DatasetIncrementQueryServiceImpl>()
            .build();

        let dataset_increment_query_service = catalog
            .get_one::<dyn DatasetIncrementQueryService>()
            .unwrap();

        Self {
            base_repo_harness,
            dataset_increment_query_service,
        }
    }

    async fn check_between_cases(
        &self,
        dataset_id: &odf::DatasetID,
        between_cases: &[(
            Option<&odf::Multihash>,
            &odf::Multihash,
            DatasetIntervalIncrement,
        )],
    ) {
        for (index, (old_head, new_head, expected_increment)) in between_cases.iter().enumerate() {
            assert_eq!(
                self.dataset_increment_query_service
                    .get_increment_between(dataset_id, *old_head, new_head,)
                    .await
                    .unwrap(),
                *expected_increment,
                "Checking between-case #{index}"
            );
        }
    }

    async fn check_since_cases(
        &self,
        dataset_id: &odf::DatasetID,
        since_cases: &[(Option<&odf::Multihash>, DatasetIntervalIncrement)],
    ) {
        for (index, (old_head, expected_increment)) in since_cases.iter().enumerate() {
            assert_eq!(
                self.dataset_increment_query_service
                    .get_increment_since(dataset_id, *old_head)
                    .await
                    .unwrap(),
                *expected_increment,
                "Checking since-case #{index}"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
