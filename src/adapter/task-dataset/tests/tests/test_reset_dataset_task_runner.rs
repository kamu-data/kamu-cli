// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::testing::{BaseRepoHarness, BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::{ResetExecutorImpl, ResetJobImpl, ResetPlannerImpl};
use kamu_adapter_task_dataset::{
    DatasetRefMovedError,
    DatasetResetOldHeadMismatch,
    ResetDatasetTaskRunner,
    TaskDefinitionDatasetReset,
    TaskErrorDatasetCommit,
    TaskErrorDatasetReset,
};
use kamu_datasets::{CreateDatasetResult, DatasetReferenceUpdater};
use kamu_datasets_services::testing::RacingDatasetReferenceUpdater;
use kamu_task_system::{TaskDefinition, TaskError, TaskOutcome, TaskRunner};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_ref_moved_after_planning_fails_recoverably() {
    let alias_foo = BaseRepoHarness::alias("foo");

    let harness = ResetDatasetTaskRunnerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
    harness.move_head_before_next_update(concurrent_head.clone());

    let outcome = harness.run_reset(&foo, Some(foo.head.clone())).await;

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
async fn test_reset_with_mismatching_old_head_fails_unrecoverably() {
    let alias_foo = BaseRepoHarness::alias("foo");

    let harness = ResetDatasetTaskRunnerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    let not_the_head = odf::Multihash::from_digest_sha3_256(b"not-the-head");

    let outcome = harness.run_reset(&foo, Some(not_the_head.clone())).await;

    let TaskOutcome::Failed(task_error) = outcome else {
        panic!("Expected a failed reset, got {outcome:?}");
    };
    assert_eq!(
        TaskErrorDatasetReset::OldHeadMismatch(DatasetResetOldHeadMismatch {
            expected_head: not_the_head,
            actual_head: foo.head.clone(),
        })
        .into_task_error(),
        task_error
    );
    assert!(!task_error.recoverable);
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_not_allowed_fails_unrecoverably() {
    let alias_foo = BaseRepoHarness::alias("foo");

    let harness = ResetDatasetTaskRunnerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness.deny_next_update();

    let outcome = harness.run_reset(&foo, Some(foo.head.clone())).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_of_dataset_deleted_after_planning_fails_unrecoverably() {
    let alias_foo = BaseRepoHarness::alias("foo");

    let harness = ResetDatasetTaskRunnerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness.delete_dataset(&foo).await;

    let outcome = harness.run_reset(&foo, Some(foo.head.clone())).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct ResetDatasetTaskRunnerHarness {
    base_use_case_harness: BaseUseCaseHarness,
    catalog: dill::Catalog,
    runner: Arc<dyn TaskRunner>,
    racing_reference_updater: Arc<RacingDatasetReferenceUpdater>,
}

impl ResetDatasetTaskRunnerHarness {
    /// Tasks run as the node, which may run every operation
    fn new() -> Self {
        let base_use_case_harness =
            BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new().as_task_node());

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<ResetDatasetTaskRunner>()
            .add::<ResetJobImpl>()
            .add::<ResetPlannerImpl>()
            .add::<ResetExecutorImpl>()
            .add_value(RacingDatasetReferenceUpdater::default())
            .bind::<dyn DatasetReferenceUpdater, RacingDatasetReferenceUpdater>();

        let catalog = b.build();

        Self {
            base_use_case_harness,
            runner: catalog.get_one::<ResetDatasetTaskRunner>().unwrap(),
            racing_reference_updater: catalog.get_one().unwrap(),
            catalog,
        }
    }

    fn deny_next_update(&self) {
        self.racing_reference_updater.deny_next_update();
    }

    /// Another writer moves `HEAD` to `concurrent_head` after the reset was
    /// planned
    fn move_head_before_next_update(&self, concurrent_head: odf::Multihash) {
        self.racing_reference_updater
            .move_reference_before_next_update(&self.catalog, concurrent_head);
    }

    /// Resets `created` to its `Seed`, expecting `HEAD` at `old_head_hash`
    async fn run_reset(
        &self,
        created: &CreateDatasetResult,
        old_head_hash: Option<odf::Multihash>,
    ) -> TaskOutcome {
        self.runner
            .run_task(TaskDefinition::new(TaskDefinitionDatasetReset {
                dataset_handle: created.dataset_handle.clone(),
                new_head_hash: None,
                old_head_hash,
            }))
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
