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
use event_bus::EventBus;
use kamu::testing::MetadataFactory;
use kamu::{DatasetChangesServiceImpl, DatasetRepositoryLocalFs, DependencyGraphServiceInMemory};
use kamu_core::{
    auth,
    CommitOpts,
    CreateDatasetResult,
    CurrentAccountSubject,
    DatasetChangesService,
    DatasetIntervalIncrement,
    DatasetRepository,
};
use opendatafabric::{
    Checkpoint,
    DatasetAlias,
    DatasetKind,
    DatasetName,
    MetadataEvent,
    Multihash,
};
use tempfile::TempDir;

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_add_data_differences() {
    let harness = DatasetChangesHarness::new();
    let foo_result = harness.create_root_dataset("foo").await;

    let dataset = harness
        .dataset_repo
        .get_dataset(&foo_result.dataset_handle.as_local_ref())
        .await
        .unwrap();

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

    let between_checks = [
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

    for (old_head, new_head, expected_increment) in between_checks {
        assert_eq!(
            harness
                .dataset_changes_service
                .get_increment_between(&foo_result.dataset_handle.id, old_head, new_head,)
                .await
                .unwrap(),
            expected_increment
        );
    }

    let since_checks = [
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

    for (old_head, expected_increment) in since_checks {
        assert_eq!(
            harness
                .dataset_changes_service
                .get_increment_since(&foo_result.dataset_handle.id, old_head)
                .await
                .unwrap(),
            expected_increment
        );
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_execute_transform_differences() {
    let harness = DatasetChangesHarness::new();
    harness.create_root_dataset("foo").await;
    let bar_result = harness.create_derived_dataset("bar", vec!["foo"]).await;

    let bar_dataset = harness
        .dataset_repo
        .get_dataset(&bar_result.dataset_handle.as_local_ref())
        .await
        .unwrap();

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

    let between_checks = [
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

    for (old_head, new_head, expected_increment) in between_checks {
        assert_eq!(
            harness
                .dataset_changes_service
                .get_increment_between(&bar_result.dataset_handle.id, old_head, new_head,)
                .await
                .unwrap(),
            expected_increment
        );
    }

    let since_checks = [
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

    for (old_head, expected_increment) in since_checks {
        assert_eq!(
            harness
                .dataset_changes_service
                .get_increment_since(&bar_result.dataset_handle.id, old_head)
                .await
                .unwrap(),
            expected_increment
        );
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO:
//  - more than 1 watermark on the interval => pick latest updated
//  - had watermark earlier than interval:
//       - updated within => pick updated
//       - propagated older => pick nothing

/////////////////////////////////////////////////////////////////////////////////////////

struct DatasetChangesHarness {
    _workdir: TempDir,
    _catalog: dill::Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_changes_service: Arc<dyn DatasetChangesService>,
}

impl DatasetChangesHarness {
    fn new() -> Self {
        let workdir = tempfile::tempdir().unwrap();
        let datasets_dir = workdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add::<DatasetChangesServiceImpl>()
            .add::<DependencyGraphServiceInMemory>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

        let dataset_changes_service = catalog.get_one::<dyn DatasetChangesService>().unwrap();

        Self {
            _workdir: workdir,
            _catalog: catalog,
            dataset_repo,
            dataset_changes_service,
        }
    }

    async fn create_root_dataset(&self, dataset_name: &str) -> CreateDatasetResult {
        let alias = DatasetAlias::new(None, DatasetName::new_unchecked(dataset_name));
        let create_result = self
            .dataset_repo
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
        self.dataset_repo
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
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
