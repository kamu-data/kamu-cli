// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::TypeId;
use std::sync::Arc;

use kamu::DatasetUpdateStepsImpl;
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu_adapter_task_dataset::{
    CompactionDatasetTaskRunner,
    DatasetHeadMovedError,
    TaskDefinitionDatasetHardCompact,
    TaskDefinitionDatasetResetToMetadata,
    TaskErrorDatasetCommit,
    TaskResultDatasetHardCompact,
    TaskResultDatasetResetToMetadata,
};
use kamu_core::testing::ReportedHeads;
use kamu_core::*;
use kamu_datasets::{CreateDatasetResult, ResolvedDataset};
use kamu_datasets_services::DatasetHeadCommitterImpl;
use kamu_task_system::{TaskDefinition, TaskOutcome, TaskRunner, TaskRunnerMeta};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_one_runner_serves_both_compaction_task_types() {
    let harness = CompactionTaskRunnersHarness::new(MockCompactionExecutor::new());

    for task_type in [
        TaskDefinitionDatasetHardCompact::TASK_TYPE,
        TaskDefinitionDatasetResetToMetadata::TASK_TYPE,
    ] {
        assert_eq!(
            harness.runner_types_for(task_type),
            vec![TypeId::of::<CompactionDatasetTaskRunner>()],
            "{task_type}",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_hard_compact_commits_head_and_reports_hard_compact_result() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let reported_heads = ReportedHeads::default();

    let harness = CompactionTaskRunnersHarness::new(
        MockCompactionExecutor::new()
            .make_expect_compaction_success(alias_foo.clone(), reported_heads.clone()),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let compacted_head = harness.append_unreferenced_block(&foo, "compacted").await;
    reported_heads.set(&foo.head, &compacted_head);

    let outcome = harness.run_hard_compact(&foo).await;

    assert_eq!(
        TaskOutcome::Success(
            TaskResultDatasetHardCompact {
                compaction_result: CompactionTaskRunnersHarness::compaction_success(
                    &foo.head,
                    &compacted_head
                ),
            }
            .into_task_result()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&foo).await, compacted_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_to_metadata_commits_head_and_reports_reset_to_metadata_result() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let reported_heads = ReportedHeads::default();

    let harness = CompactionTaskRunnersHarness::new(
        MockCompactionExecutor::new()
            .make_expect_compaction_success(alias_foo.clone(), reported_heads.clone()),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let compacted_head = harness.append_unreferenced_block(&foo, "compacted").await;
    reported_heads.set(&foo.head, &compacted_head);

    let outcome = harness.run_reset_to_metadata(&foo).await;

    assert_eq!(
        TaskOutcome::Success(
            TaskResultDatasetResetToMetadata {
                compaction_metadata_only_result: CompactionTaskRunnersHarness::compaction_success(
                    &foo.head,
                    &compacted_head
                ),
            }
            .into_task_result()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&foo).await, compacted_head);
}

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
    catalog: dill::Catalog,
}

impl CompactionTaskRunnersHarness {
    fn new(mock_compaction_executor: MockCompactionExecutor) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new());

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<CompactionDatasetTaskRunner>()
            .add::<DatasetUpdateStepsImpl>()
            .add::<DatasetHeadCommitterImpl>()
            .add_value(mock_compaction_executor)
            .bind::<dyn CompactionExecutor, MockCompactionExecutor>();

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        Self {
            base_use_case_harness,
            catalog: b.build(),
        }
    }

    /// Implementations registered for the task type, looked up the way the
    /// task agent does
    fn runner_types_for(&self, task_type: &str) -> Vec<TypeId> {
        self.catalog
            .builders_for_with_meta::<dyn TaskRunner, _>(|meta: &TaskRunnerMeta| {
                meta.task_type == task_type
            })
            .map(|builder| dill::Builder::instance_type(&builder).id)
            .collect()
    }

    fn runner_for(&self, task_type: &str) -> Arc<dyn TaskRunner> {
        self.catalog
            .builders_for_with_meta::<dyn TaskRunner, _>(|meta: &TaskRunnerMeta| {
                meta.task_type == task_type
            })
            .next()
            .unwrap()
            .get(&self.catalog)
            .unwrap()
    }

    async fn run_hard_compact(&self, created: &CreateDatasetResult) -> TaskOutcome {
        self.runner_for(TaskDefinitionDatasetHardCompact::TASK_TYPE)
            .run_task(TaskDefinition::new(TaskDefinitionDatasetHardCompact {
                target: ResolvedDataset::from_created(created),
                compaction_plan: Self::compaction_plan(created),
            }))
            .await
            .unwrap()
    }

    async fn run_reset_to_metadata(&self, created: &CreateDatasetResult) -> TaskOutcome {
        self.runner_for(TaskDefinitionDatasetResetToMetadata::TASK_TYPE)
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

    /// What the mocked executor reports
    fn compaction_success(
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> CompactionResult {
        CompactionResult::Success {
            old_head: old_head.clone(),
            new_head: new_head.clone(),
            old_num_blocks: 2,
            new_num_blocks: 1,
            breaking: true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
