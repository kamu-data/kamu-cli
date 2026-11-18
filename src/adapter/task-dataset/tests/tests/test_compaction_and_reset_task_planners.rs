// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu_adapter_task_dataset::{
    CompactionDatasetTaskPlanner,
    LogicalPlanDatasetHardCompact,
    LogicalPlanDatasetReset,
    LogicalPlanDatasetResetToMetadata,
    ResetDatasetTaskPlanner,
    TaskDefinitionDatasetHardCompact,
    TaskDefinitionDatasetResetToMetadata,
};
use kamu_core::CompactionOptions;
use kamu_datasets::CreateDatasetResult;
use kamu_task_system::{
    LogicalPlan,
    PrepareTaskDefinitionError,
    TaskDefinition,
    TaskDefinitionPlanner,
    TaskError,
    TaskID,
};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_hard_compaction_targets_the_dataset_with_its_slice_limits() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let harness = CompactionAndResetTaskPlannersHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;

    let task_definition = harness
        .plan_hard_compaction_with(&foo, Some(100), Some(10))
        .await
        .unwrap();

    let task_compact = task_definition
        .downcast::<TaskDefinitionDatasetHardCompact>()
        .unwrap();
    assert_eq!(foo.dataset_handle, task_compact.target);
    assert_eq!(
        CompactionOptions {
            max_slice_size: Some(100),
            max_slice_records: Some(10),
            keep_metadata_only: false,
        },
        task_compact.options
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_planning_for_deleted_dataset_fails_unrecoverably() {
    for planned_task in [
        PlannedTask::HardCompaction,
        PlannedTask::Reset,
        PlannedTask::ResetToMetadata,
    ] {
        let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
        let harness = CompactionAndResetTaskPlannersHarness::new();
        let foo = harness.create_root_dataset(&alias_foo).await;
        harness.delete_dataset(&foo).await;

        let failure = harness.plan(planned_task, &foo).await.unwrap_err();

        assert_eq!(
            TaskError::empty_unrecoverable(),
            failure,
            "{planned_task:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_to_metadata_of_derived_dataset_targets_the_dataset() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let harness = CompactionAndResetTaskPlannersHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let task_definition = harness.plan_reset_to_metadata(&bar).await.unwrap();

    let task_reset = task_definition
        .downcast::<TaskDefinitionDatasetResetToMetadata>()
        .unwrap();
    assert_eq!(bar.dataset_handle, task_reset.target);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
enum PlannedTask {
    HardCompaction,
    Reset,
    ResetToMetadata,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct CompactionAndResetTaskPlannersHarness {
    base_use_case_harness: BaseUseCaseHarness,
    catalog: dill::Catalog,
}

impl CompactionAndResetTaskPlannersHarness {
    fn new() -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new());

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<CompactionDatasetTaskPlanner>()
            .add::<ResetDatasetTaskPlanner>();

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        Self {
            base_use_case_harness,
            catalog: b.build(),
        }
    }

    async fn plan(
        &self,
        planned_task: PlannedTask,
        created: &CreateDatasetResult,
    ) -> Result<TaskDefinition, TaskError> {
        match planned_task {
            PlannedTask::HardCompaction => self.plan_hard_compaction(created).await,
            PlannedTask::Reset => self.plan_reset(created).await,
            PlannedTask::ResetToMetadata => self.plan_reset_to_metadata(created).await,
        }
    }

    async fn plan_hard_compaction(
        &self,
        created: &CreateDatasetResult,
    ) -> Result<TaskDefinition, TaskError> {
        self.plan_hard_compaction_with(created, None, None).await
    }

    async fn plan_hard_compaction_with(
        &self,
        created: &CreateDatasetResult,
        max_slice_size: Option<u64>,
        max_slice_records: Option<u64>,
    ) -> Result<TaskDefinition, TaskError> {
        let logical_plan = LogicalPlanDatasetHardCompact {
            dataset_id: created.dataset_handle.id.clone(),
            max_slice_size,
            max_slice_records,
        }
        .into_logical_plan();
        self.prepare::<CompactionDatasetTaskPlanner>(&logical_plan)
            .await
    }

    async fn plan_reset(&self, created: &CreateDatasetResult) -> Result<TaskDefinition, TaskError> {
        let logical_plan = LogicalPlanDatasetReset {
            dataset_id: created.dataset_handle.id.clone(),
            new_head_hash: None,
            old_head_hash: None,
        }
        .into_logical_plan();
        self.prepare::<ResetDatasetTaskPlanner>(&logical_plan).await
    }

    async fn plan_reset_to_metadata(
        &self,
        created: &CreateDatasetResult,
    ) -> Result<TaskDefinition, TaskError> {
        let logical_plan = LogicalPlanDatasetResetToMetadata {
            dataset_id: created.dataset_handle.id.clone(),
        }
        .into_logical_plan();
        self.prepare::<CompactionDatasetTaskPlanner>(&logical_plan)
            .await
    }

    /// Prepares the task definition, returning the error the task fails with
    /// when planning fails
    async fn prepare<P: TaskDefinitionPlanner + 'static>(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, TaskError> {
        match self
            .catalog
            .get_one::<P>()
            .unwrap()
            .prepare_task_definition(TaskID::new(1), logical_plan)
            .await
        {
            Ok(task_definition) => Ok(task_definition),
            Err(PrepareTaskDefinitionError::Failed(task_error)) => Err(task_error),
            Err(PrepareTaskDefinitionError::Internal(e)) => {
                panic!("Unexpected internal error: {e}")
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
