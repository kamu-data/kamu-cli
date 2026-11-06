// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::ResetExecutorImpl;
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu_adapter_task_dataset::{
    DatasetHeadMovedError,
    ResetDatasetTaskRunner,
    TaskDefinitionDatasetReset,
    TaskErrorDatasetCommit,
};
use kamu_core::ResetPlan;
use kamu_datasets::{CreateDatasetResult, DatasetHeadCommitter};
use kamu_datasets_services::testing::RacingDatasetHeadCommitter;
use kamu_task_system::{TaskDefinition, TaskError, TaskOutcome, TaskRunner};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_head_moved_after_planning_fails_recoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = ResetDatasetTaskRunnerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    let reset_plan = ResetPlan {
        old_head: Some(foo.head.clone()),
        new_head: foo.head.clone(),
    };
    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
    harness.move_head(&foo, &foo.head, &concurrent_head).await;

    let outcome = harness.run_reset(&foo, reset_plan).await;

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
async fn test_reset_not_allowed_fails_unrecoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = ResetDatasetTaskRunnerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    let reset_head = harness.append_unreferenced_block(&foo, "reset to").await;
    let reset_plan = ResetPlan {
        old_head: Some(foo.head.clone()),
        new_head: reset_head,
    };
    harness.deny_next_commit();

    let outcome = harness.run_reset(&foo, reset_plan).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct ResetDatasetTaskRunnerHarness {
    base_use_case_harness: BaseUseCaseHarness,
    runner: Arc<dyn TaskRunner>,
    racing_committer: Arc<RacingDatasetHeadCommitter>,
}

impl ResetDatasetTaskRunnerHarness {
    fn new() -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new());

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<ResetDatasetTaskRunner>()
            .add::<ResetExecutorImpl>()
            .add_value(RacingDatasetHeadCommitter::default())
            .bind::<dyn DatasetHeadCommitter, RacingDatasetHeadCommitter>();

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        Self {
            base_use_case_harness,
            runner: catalog.get_one::<ResetDatasetTaskRunner>().unwrap(),
            racing_committer: catalog.get_one().unwrap(),
        }
    }

    fn deny_next_commit(&self) {
        self.racing_committer.deny_next_commit();
    }

    async fn run_reset(&self, created: &CreateDatasetResult, reset_plan: ResetPlan) -> TaskOutcome {
        self.runner
            .run_task(TaskDefinition::new(TaskDefinitionDatasetReset {
                dataset_handle: created.dataset_handle.clone(),
                reset_plan,
            }))
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
