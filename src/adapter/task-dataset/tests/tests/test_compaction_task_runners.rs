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

use internal_error::ErrorIntoInternal;
use kamu::testing::{BaseRepoHarness, BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::{
    CompactionJobImpl,
    CompactionPlannerImpl,
    EngineConfigDatafusionEmbeddedCompaction,
    ObjectStoreRegistryImpl,
};
use kamu_adapter_task_dataset::{
    CompactionDatasetTaskRunner,
    DatasetRefMovedError,
    TaskDefinitionDatasetHardCompact,
    TaskDefinitionDatasetResetToMetadata,
    TaskErrorDatasetCommit,
    TaskResultDatasetHardCompact,
    TaskResultDatasetResetToMetadata,
};
use kamu_core::testing::ReportedHeads;
use kamu_core::*;
use kamu_datasets::CreateDatasetResult;
use kamu_datasets_services::testing::DatasetReferenceRace;
use kamu_task_system::{
    TaskDefinition,
    TaskError,
    TaskOutcome,
    TaskResult,
    TaskRunner,
    TaskRunnerMeta,
};
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
async fn test_compaction_commits_head_and_reports_result_of_its_task_type() {
    for task_type in [
        TaskDefinitionDatasetHardCompact::TASK_TYPE,
        TaskDefinitionDatasetResetToMetadata::TASK_TYPE,
    ] {
        let (harness, foo, compacted_head) =
            CompactionTaskRunnersHarness::new_compacting_new_root(&BaseRepoHarness::alias("foo"))
                .await;

        let outcome = harness.run(task_type, &foo).await;

        assert_eq!(
            TaskOutcome::Success(CompactionTaskRunnersHarness::task_result(
                task_type,
                CompactionTaskRunnersHarness::compaction_success(&foo.head, &compacted_head),
            )),
            outcome,
            "{task_type}",
        );
        assert_eq!(harness.get_head(&foo).await, compacted_head, "{task_type}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_hard_compact_ref_moved_concurrently_fails_recoverably() {
    let (harness, foo, _) =
        CompactionTaskRunnersHarness::new_compacting_new_root(&BaseRepoHarness::alias("foo")).await;
    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
    harness.move_head(&foo, &foo.head, &concurrent_head).await;

    let outcome = harness.run_hard_compact(&foo).await;

    assert_eq!(
        TaskOutcome::Failed(
            TaskErrorDatasetCommit::RefMoved(DatasetRefMovedError {
                dataset_id: foo.dataset_handle.id.clone(),
                block_ref: odf::BlockRef::Head,
                expected_block_hash: Some(foo.head.clone()),
                actual_block_hash: Some(concurrent_head.clone()),
            })
            .into_task_error()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_hard_compact_of_dataset_deleted_during_merge_fails_unrecoverably() {
    let alias_foo = BaseRepoHarness::alias("foo");
    let reported_heads = ReportedHeads::default();

    let harness = CompactionTaskRunnersHarness::new(
        CompactionTaskRunnersHarness::executor_deleting_target(&alias_foo, &reported_heads),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let compacted_head = harness.append_unreferenced_block(&foo, "compacted").await;
    reported_heads.set(&foo.head, &compacted_head);

    let outcome = harness.run_hard_compact(&foo).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_hard_compact_not_allowed_fails_unrecoverably() {
    let (harness, foo, _) =
        CompactionTaskRunnersHarness::new_compacting_new_root(&BaseRepoHarness::alias("foo")).await;
    harness.deny_next_update();

    let outcome = harness.run_hard_compact(&foo).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_hard_compact_execution_failure_fails_recoverably() {
    let alias_foo = BaseRepoHarness::alias("foo");

    let harness = CompactionTaskRunnersHarness::new(
        CompactionTaskRunnersHarness::failing_executor(&alias_foo),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;

    let outcome = harness.run_hard_compact(&foo).await;

    assert_eq!(TaskOutcome::Failed(TaskError::empty_recoverable()), outcome);
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_hard_compact_of_derived_dataset_fails_unrecoverably() {
    let alias_foo = BaseRepoHarness::alias("foo");
    let alias_bar = BaseRepoHarness::alias("bar");

    let harness = CompactionTaskRunnersHarness::new(MockCompactionExecutor::new());
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let outcome = harness.run_hard_compact(&bar).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
    assert_eq!(harness.get_head(&bar).await, bar.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_hard_compact_with_nothing_to_do_succeeds_without_moving_head() {
    let alias_foo = BaseRepoHarness::alias("foo");

    let harness = CompactionTaskRunnersHarness::new(
        MockCompactionExecutor::new().make_expect_compaction_nothing_to_do(alias_foo.clone()),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;

    let outcome = harness.run_hard_compact(&foo).await;

    assert_eq!(
        TaskOutcome::Success(
            TaskResultDatasetHardCompact {
                compaction_result: CompactionResult::NothingToDo,
            }
            .into_task_result()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct CompactionTaskRunnersHarness {
    base_use_case_harness: BaseUseCaseHarness,
    catalog: dill::Catalog,
    reference_race: Arc<DatasetReferenceRace>,
}

impl CompactionTaskRunnersHarness {
    /// Tasks run as the node, which may run every operation
    fn new(mock_compaction_executor: MockCompactionExecutor) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .as_task_node()
                .with_racing_references(),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<CompactionDatasetTaskRunner>()
            .add::<CompactionJobImpl>()
            .add::<CompactionPlannerImpl>()
            .add_value(EngineConfigDatafusionEmbeddedCompaction::default())
            .add::<ObjectStoreRegistryImpl>()
            .add_value(mock_compaction_executor)
            .bind::<dyn CompactionExecutor, MockCompactionExecutor>();

        let catalog = b.build();

        Self {
            base_use_case_harness,
            reference_race: catalog.get_one().unwrap(),
            catalog,
        }
    }

    fn deny_next_update(&self) {
        self.reference_race.deny_next_update();
    }

    /// Creates root `alias` and expects one compaction of it reporting a new
    /// head, appended unreferenced; returns the dataset and that head
    async fn new_compacting_new_root(
        alias: &odf::DatasetAlias,
    ) -> (Self, CreateDatasetResult, odf::Multihash) {
        let reported_heads = ReportedHeads::default();
        let harness = Self::new(
            MockCompactionExecutor::new()
                .make_expect_compaction_success(alias.clone(), reported_heads.clone()),
        );
        let created = harness.create_root_dataset(alias).await;
        let compacted_head = harness
            .append_unreferenced_block(&created, "compacted")
            .await;
        reported_heads.set(&created.head, &compacted_head);
        (harness, created, compacted_head)
    }

    /// Expects one compaction of `alias` that fails
    fn failing_executor(alias: &odf::DatasetAlias) -> MockCompactionExecutor {
        let alias = alias.clone();
        let mut mock_compaction_executor = MockCompactionExecutor::new();
        mock_compaction_executor
            .expect_execute()
            .withf(move |target, _, _| target.get_alias() == &alias)
            .times(1)
            .returning(|_, _, _| Err("Cannot write the compacted slice".int_err().into()));
        mock_compaction_executor
    }

    /// Expects one compaction of `alias` that deletes the dataset's storage
    /// before it reports success, as another writer deleting the dataset
    /// would
    fn executor_deleting_target(
        alias: &odf::DatasetAlias,
        heads: &ReportedHeads,
    ) -> MockCompactionExecutor {
        let alias = alias.clone();
        let heads = heads.clone();
        let mut mock_compaction_executor = MockCompactionExecutor::new();
        mock_compaction_executor
            .expect_execute()
            .withf(move |target, _, _| target.get_alias() == &alias)
            .times(1)
            .returning(move |target, _, _| {
                std::fs::remove_dir_all(target.get_storage_internal_url().to_file_path().unwrap())
                    .unwrap();
                let (old_head, new_head) = heads.get();
                Ok(Self::compaction_success(&old_head, &new_head))
            });
        mock_compaction_executor
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
        self.run(TaskDefinitionDatasetHardCompact::TASK_TYPE, created)
            .await
    }

    /// Runs a compaction task of `task_type` over `created`
    async fn run(&self, task_type: &str, created: &CreateDatasetResult) -> TaskOutcome {
        let target = created.dataset_handle.clone();
        let task_definition = match task_type {
            TaskDefinitionDatasetHardCompact::TASK_TYPE => {
                TaskDefinition::new(TaskDefinitionDatasetHardCompact {
                    target,
                    options: CompactionOptions::default(),
                })
            }
            TaskDefinitionDatasetResetToMetadata::TASK_TYPE => {
                TaskDefinition::new(TaskDefinitionDatasetResetToMetadata { target })
            }
            other => panic!("Not a compaction task type: {other}"),
        };

        self.runner_for(task_type)
            .run_task(task_definition)
            .await
            .unwrap()
    }

    /// The result a compaction task of `task_type` reports
    fn task_result(task_type: &str, compaction_result: CompactionResult) -> TaskResult {
        match task_type {
            TaskDefinitionDatasetHardCompact::TASK_TYPE => {
                TaskResultDatasetHardCompact { compaction_result }.into_task_result()
            }
            TaskDefinitionDatasetResetToMetadata::TASK_TYPE => TaskResultDatasetResetToMetadata {
                compaction_metadata_only_result: compaction_result,
            }
            .into_task_result(),
            other => panic!("Not a compaction task type: {other}"),
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
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
