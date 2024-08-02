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
use dill::Component;
use kamu::testing::MetadataFactory;
use kamu::{DatasetChangesServiceImpl, DatasetRepositoryLocalFs, DatasetRepositoryWriter};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{
    CommitOpts,
    CreateDatasetResult,
    DatasetChangesService,
    DatasetIntervalIncrement,
    DatasetRepository,
};
use opendatafabric::{
    Checkpoint,
    DatasetAlias,
    DatasetID,
    DatasetKind,
    DatasetName,
    MetadataEvent,
    Multihash,
};
use tempfile::TempDir;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_initial_increment() {
    let harness = DatasetChangesHarness::new();
    let foo_result = harness.create_root_dataset("foo").await;

    // "foo" initially has Seed and SetPollingSource events

    let increment_between = harness
        .dataset_changes_service
        .get_increment_between(&foo_result.dataset_handle.id, None, &foo_result.head)
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
        .dataset_changes_service
        .get_increment_since(&foo_result.dataset_handle.id, None)
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
    let harness = DatasetChangesHarness::new();
    let foo_result = harness.create_root_dataset("foo").await;

    let increment_between = harness
        .dataset_changes_service
        .get_increment_between(
            &foo_result.dataset_handle.id,
            Some(&foo_result.head),
            &foo_result.head,
        )
        .await
        .unwrap();
    assert_eq!(increment_between, DatasetIntervalIncrement::default());

    let increment_since = harness
        .dataset_changes_service
        .get_increment_since(&foo_result.dataset_handle.id, Some(&foo_result.head))
        .await
        .unwrap();
    assert_eq!(increment_since, DatasetIntervalIncrement::default());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_add_data_differences() {
    let harness = DatasetChangesHarness::new();
    let foo_result = harness.create_root_dataset("foo").await;

    let dataset = harness
        .dataset_repo
        .get_dataset_by_handle(&foo_result.dataset_handle);

    // Commit SetDataSchema and 2 data nodes

    let commit_result_1 = dataset
        .commit_event(
            MetadataEvent::SetDataSchema(MetadataFactory::set_data_schema().build()),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let new_watermark_time = Utc::now();

    let commit_result_2 = dataset
        .commit_event(
            MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(0, 9)
                    .new_checkpoint(Some(Checkpoint {
                        physical_hash: Multihash::from_digest_sha3_256(b"checkpoint-1"),
                        size: 1,
                    }))
                    .some_new_source_state()
                    .build(),
            ),
            CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let commit_result_3 = dataset
        .commit_event(
            MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(10, 14)
                    .prev_checkpoint(Some(Multihash::from_digest_sha3_256(b"checkpoint-1")))
                    .new_checkpoint(Some(Checkpoint {
                        physical_hash: Multihash::from_digest_sha3_256(b"checkpoint-2"),
                        size: 1,
                    }))
                    .new_watermark(Some(new_watermark_time))
                    .some_new_source_state()
                    .build(),
            ),
            CommitOpts {
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
        // SetDataSchema -> AddData #1
        (
            Some(&commit_result_1.new_head),
            &commit_result_2.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 10,
                updated_watermark: None,
            },
        ),
        // AddData #1 -> AddData #2
        (
            Some(&commit_result_2.new_head),
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // SetDataSchema -> AddData #2
        (
            Some(&commit_result_1.new_head),
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 2,
                num_records: 15,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // SetPollingSource -> AddData #1
        (
            Some(commit_result_1.old_head.as_ref().unwrap()),
            &commit_result_2.new_head,
            DatasetIntervalIncrement {
                num_blocks: 2,
                num_records: 10,
                updated_watermark: None,
            },
        ),
        // SetPollingSource -> AddData #2
        (
            Some(commit_result_1.old_head.as_ref().unwrap()),
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 3,
                num_records: 15,
                updated_watermark: Some(new_watermark_time),
            },
        ),
        // Initial -> AddData #2
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
        .check_between_cases(&foo_result.dataset_handle.id, &between_cases)
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
        // Since AddData #1
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
        .check_since_cases(&foo_result.dataset_handle.id, &since_cases)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_execute_transform_differences() {
    let harness = DatasetChangesHarness::new();
    harness.create_root_dataset("foo").await;
    let bar_result = harness.create_derived_dataset("bar", vec!["foo"]).await;

    let bar_dataset = harness
        .dataset_repo
        .get_dataset_by_handle(&bar_result.dataset_handle);

    // Commit SetDataSchema and 2 trasnform data nodes

    let new_watermark_time = Utc::now();

    let commit_result_1 = bar_dataset
        .commit_event(
            MetadataEvent::SetDataSchema(MetadataFactory::set_data_schema().build()),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let commit_result_2 = bar_dataset
        .commit_event(
            MetadataEvent::ExecuteTransform(
                MetadataFactory::execute_transform()
                    .empty_query_inputs_from_seeded_ids(["foo"])
                    .some_new_data_with_offset(0, 14)
                    .new_checkpoint(Some(Checkpoint {
                        physical_hash: Multihash::from_digest_sha3_256(b"checkpoint-1"),
                        size: 1,
                    }))
                    .build(),
            ),
            CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let commit_result_3 = bar_dataset
        .commit_event(
            MetadataEvent::ExecuteTransform(
                MetadataFactory::execute_transform()
                    .empty_query_inputs_from_seeded_ids(["foo"])
                    .some_new_data_with_offset(15, 19)
                    .prev_checkpoint(Some(Multihash::from_digest_sha3_256(b"checkpoint-1")))
                    .new_checkpoint(Some(Checkpoint {
                        physical_hash: Multihash::from_digest_sha3_256(b"checkpoint-2"),
                        size: 1,
                    }))
                    .new_watermark(Some(new_watermark_time))
                    .build(),
            ),
            CommitOpts {
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
        .check_between_cases(&bar_result.dataset_handle.id, &between_cases)
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
        .check_since_cases(&bar_result.dataset_handle.id, &since_cases)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_multiple_watermarks_within_interval() {
    let harness = DatasetChangesHarness::new();
    let foo_result = harness.create_root_dataset("foo").await;

    let dataset = harness
        .dataset_repo
        .get_dataset_by_handle(&foo_result.dataset_handle);

    // Commit SetDataSchema and 2 data nodes each having a watermark

    let commit_result_1 = dataset
        .commit_event(
            MetadataEvent::SetDataSchema(MetadataFactory::set_data_schema().build()),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let watermark_1_time = Utc::now();

    let commit_result_2 = dataset
        .commit_event(
            MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(0, 9)
                    .new_checkpoint(Some(Checkpoint {
                        physical_hash: Multihash::from_digest_sha3_256(b"checkpoint-1"),
                        size: 1,
                    }))
                    .new_watermark(Some(watermark_1_time))
                    .some_new_source_state()
                    .build(),
            ),
            CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let watermark_2_time = Utc::now();

    let commit_result_3 = dataset
        .commit_event(
            MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(10, 24)
                    .prev_checkpoint(Some(Multihash::from_digest_sha3_256(b"checkpoint-1")))
                    .new_checkpoint(Some(Checkpoint {
                        physical_hash: Multihash::from_digest_sha3_256(b"checkpoint-2"),
                        size: 1,
                    }))
                    .new_watermark(Some(watermark_2_time))
                    .some_new_source_state()
                    .build(),
            ),
            CommitOpts {
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
        // SetDataSchema -> AddData #1
        (
            Some(commit_result_2.old_head.as_ref().unwrap()),
            &commit_result_2.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 10,
                updated_watermark: Some(watermark_1_time),
            },
        ),
        // AddData #1 -> AddData #2
        (
            Some(commit_result_3.old_head.as_ref().unwrap()),
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 15,
                updated_watermark: Some(watermark_2_time),
            },
        ),
        // Initial -> AddData #2
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
        .check_between_cases(&foo_result.dataset_handle.id, &between_cases)
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
        // Since AddData #1
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
        .check_since_cases(&foo_result.dataset_handle.id, &since_cases)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_older_watermark_before_interval() {
    let harness = DatasetChangesHarness::new();
    let foo_result = harness.create_root_dataset("foo").await;

    let dataset = harness
        .dataset_repo
        .get_dataset_by_handle(&foo_result.dataset_handle);

    // Commit SetDataSchema and 3 data nodes, with #1,3 containing watermark

    dataset
        .commit_event(
            MetadataEvent::SetDataSchema(MetadataFactory::set_data_schema().build()),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let watermark_1_time = Utc::now();

    let commit_result_2 = dataset
        .commit_event(
            MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(0, 9)
                    .new_checkpoint(Some(Checkpoint {
                        physical_hash: Multihash::from_digest_sha3_256(b"checkpoint-1"),
                        size: 1,
                    }))
                    .new_watermark(Some(watermark_1_time))
                    .some_new_source_state()
                    .build(),
            ),
            CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let commit_result_3 = dataset
        .commit_event(
            MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(10, 24)
                    .prev_checkpoint(Some(Multihash::from_digest_sha3_256(b"checkpoint-1")))
                    .new_checkpoint(Some(Checkpoint {
                        physical_hash: Multihash::from_digest_sha3_256(b"checkpoint-2"),
                        size: 1,
                    }))
                    .new_watermark(Some(watermark_1_time))
                    .some_new_source_state()
                    .build(),
            ),
            CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let watermark_2_time = Utc::now();

    let commit_result_4 = dataset
        .commit_event(
            MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .some_new_data_with_offset(25, 36)
                    .prev_checkpoint(Some(Multihash::from_digest_sha3_256(b"checkpoint-2")))
                    .new_checkpoint(Some(Checkpoint {
                        physical_hash: Multihash::from_digest_sha3_256(b"checkpoint-3"),
                        size: 1,
                    }))
                    .new_watermark(Some(watermark_2_time))
                    .some_new_source_state()
                    .build(),
            ),
            CommitOpts {
                check_object_refs: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let between_cases = [
        // SetDataSchema -> AddData #1
        (
            Some(commit_result_2.old_head.as_ref().unwrap()),
            &commit_result_2.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 10,
                updated_watermark: Some(watermark_1_time),
            },
        ),
        // AddData #1 -> AddData #2
        (
            Some(commit_result_3.old_head.as_ref().unwrap()),
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 15,
                updated_watermark: None,
            },
        ),
        // AddData #2 -> AddData #3
        (
            Some(commit_result_4.old_head.as_ref().unwrap()),
            &commit_result_4.new_head,
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 12,
                updated_watermark: Some(watermark_2_time),
            },
        ),
        // Initial -> AddData #2
        (
            None,
            &commit_result_3.new_head,
            DatasetIntervalIncrement {
                num_blocks: 5,
                num_records: 25,
                updated_watermark: Some(watermark_1_time),
            },
        ),
        // Initial -> AddData #3
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
        .check_between_cases(&foo_result.dataset_handle.id, &between_cases)
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
        // Since AddData #1
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
        .check_since_cases(&foo_result.dataset_handle.id, &since_cases)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetChangesHarness {
    _workdir: TempDir,
    _catalog: dill::Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    dataset_changes_service: Arc<dyn DatasetChangesService>,
}

impl DatasetChangesHarness {
    fn new() -> Self {
        let workdir = tempfile::tempdir().unwrap();
        let datasets_dir = workdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<SystemTimeSourceDefault>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<DatasetChangesServiceImpl>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
        let dataset_repo_writer = catalog.get_one::<dyn DatasetRepositoryWriter>().unwrap();

        let dataset_changes_service = catalog.get_one::<dyn DatasetChangesService>().unwrap();

        Self {
            _workdir: workdir,
            _catalog: catalog,
            dataset_repo,
            dataset_repo_writer,
            dataset_changes_service,
        }
    }

    async fn create_root_dataset(&self, dataset_name: &str) -> CreateDatasetResult {
        let alias = DatasetAlias::new(None, DatasetName::new_unchecked(dataset_name));
        let create_result = self
            .dataset_repo_writer
            .create_dataset(
                &alias,
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(DatasetKind::Root)
                        .id_from(alias.dataset_name.as_str())
                        .build(),
                )
                .build_typed(),
            )
            .await
            .unwrap();

        let commit_result = create_result
            .dataset
            .commit_event(
                MetadataEvent::SetPollingSource(MetadataFactory::set_polling_source().build()),
                CommitOpts::default(),
            )
            .await
            .unwrap();

        CreateDatasetResult {
            dataset_handle: create_result.dataset_handle,
            dataset: create_result.dataset,
            head: commit_result.new_head,
        }
    }

    async fn create_derived_dataset(
        &self,
        dataset_name: &str,
        input_dataset_names: Vec<&str>,
    ) -> CreateDatasetResult {
        self.dataset_repo_writer
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(DatasetAlias::new(
                        None,
                        DatasetName::new_unchecked(dataset_name),
                    ))
                    .kind(DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_aliases_and_seeded_ids(input_dataset_names)
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap()
            .create_dataset_result
    }

    async fn check_between_cases(
        &self,
        dataset_id: &DatasetID,
        between_cases: &[(Option<&Multihash>, &Multihash, DatasetIntervalIncrement)],
    ) {
        for (index, (old_head, new_head, expected_increment)) in between_cases.iter().enumerate() {
            assert_eq!(
                self.dataset_changes_service
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
        dataset_id: &DatasetID,
        since_cases: &[(Option<&Multihash>, DatasetIntervalIncrement)],
    ) {
        for (index, (old_head, expected_increment)) in since_cases.iter().enumerate() {
            assert_eq!(
                self.dataset_changes_service
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
