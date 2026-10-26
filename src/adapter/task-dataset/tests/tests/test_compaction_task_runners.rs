// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu_adapter_task_dataset::{
    DatasetHeadMovedError,
    HardCompactDatasetTaskRunner,
    ResetDatasetToMetadataTaskRunner,
    TaskDefinitionDatasetHardCompact,
    TaskDefinitionDatasetResetToMetadata,
    TaskErrorDatasetCommit,
};
use kamu_core::testing::ReportedHeads;
use kamu_core::*;
use kamu_datasets::{CreateDatasetResult, ResolvedDataset};
use kamu_datasets_services::DatasetHeadCommitterImpl;
use kamu_task_system::{TaskDefinition, TaskOutcome, TaskRunner};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_hard_compact_head_moved_concurrently_fails_recoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let reported_heads = ReportedHeads::default();

    let harness = CompactionTaskRunnersHarness::new(
        MockCompactionExecutor::new()
            .make_expect_compaction_success(alias_foo.clone(), reported_heads.clone()),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let compacted_head = harness.append_unreferenced_block(&foo, "compacted").await;
    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
    reported_heads.set(&foo.head, &compacted_head);
    harness.move_head(&foo, &foo.head, &concurrent_head).await;

    let outcome = harness.run_hard_compact(&foo).await;

    assert_eq!(
        TaskOutcome::Failed(
            TaskErrorDatasetCommit::HeadMoved(DatasetHeadMovedError {
                dataset_id: foo.dataset_handle.id.clone(),
                expected_head: Some(foo.head.clone()),
                actual_head: Some(concurrent_head.clone()),
            })
            .into_task_error()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_to_metadata_head_moved_concurrently_fails_recoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let reported_heads = ReportedHeads::default();

    let harness = CompactionTaskRunnersHarness::new(
        MockCompactionExecutor::new()
            .make_expect_compaction_success(alias_foo.clone(), reported_heads.clone()),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let compacted_head = harness.append_unreferenced_block(&foo, "compacted").await;
    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
    reported_heads.set(&foo.head, &compacted_head);
    harness.move_head(&foo, &foo.head, &concurrent_head).await;

    let outcome = harness.run_reset_to_metadata(&foo).await;

    assert_eq!(
        TaskOutcome::Failed(
            TaskErrorDatasetCommit::HeadMoved(DatasetHeadMovedError {
                dataset_id: foo.dataset_handle.id.clone(),
                expected_head: Some(foo.head.clone()),
                actual_head: Some(concurrent_head.clone()),
            })
            .into_task_error()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct CompactionTaskRunnersHarness {
    base_use_case_harness: BaseUseCaseHarness,
    hard_compact_runner: Arc<dyn TaskRunner>,
    reset_to_metadata_runner: Arc<dyn TaskRunner>,
}

impl CompactionTaskRunnersHarness {
    fn new(mock_compaction_executor: MockCompactionExecutor) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new());

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<HardCompactDatasetTaskRunner>()
            .add::<ResetDatasetToMetadataTaskRunner>()
            .add::<DatasetHeadCommitterImpl>()
            .add_value(mock_compaction_executor)
            .bind::<dyn CompactionExecutor, MockCompactionExecutor>();

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        Self {
            base_use_case_harness,
            hard_compact_runner: catalog.get_one::<HardCompactDatasetTaskRunner>().unwrap(),
            reset_to_metadata_runner: catalog
                .get_one::<ResetDatasetToMetadataTaskRunner>()
                .unwrap(),
        }
    }

    async fn run_hard_compact(&self, created: &CreateDatasetResult) -> TaskOutcome {
        self.hard_compact_runner
            .run_task(TaskDefinition::new(TaskDefinitionDatasetHardCompact {
                target: ResolvedDataset::from_created(created),
                compaction_plan: Self::compaction_plan(created),
            }))
            .await
            .unwrap()
    }

    async fn run_reset_to_metadata(&self, created: &CreateDatasetResult) -> TaskOutcome {
        self.reset_to_metadata_runner
            .run_task(TaskDefinition::new(TaskDefinitionDatasetResetToMetadata {
                target: ResolvedDataset::from_created(created),
                compaction_metadata_only_plan: Self::compaction_plan(created),
            }))
            .await
            .unwrap()
    }

    /// The executor is mocked, so the plan only has to name the planned head
    fn compaction_plan(created: &CreateDatasetResult) -> CompactionPlan {
        CompactionPlan {
            seed: created.head.clone(),
            old_head: created.head.clone(),
            old_num_blocks: 2,
            offset_column_name: "offset".to_string(),
            data_slice_batches: Vec::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
