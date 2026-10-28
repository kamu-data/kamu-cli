// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::{CompactionPlannerImpl, ResetPlannerImpl};
use kamu_adapter_task_dataset::{
    HardCompactDatasetTaskPlanner,
    LogicalPlanDatasetHardCompact,
    LogicalPlanDatasetReset,
    LogicalPlanDatasetResetToMetadata,
    ResetDatasetTaskPlanner,
    ResetToMetadataDatasetTaskPlanner,
    TaskDefinitionDatasetHardCompact,
};
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
async fn test_hard_compaction_plans_for_the_dataset() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let harness = CompactionAndResetTaskPlannersHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;

    let task_definition = harness.plan_hard_compaction(&foo).await.unwrap();

    let task_compact = task_definition
        .downcast::<TaskDefinitionDatasetHardCompact>()
        .unwrap();
    assert_eq!(foo.dataset_handle, *task_compact.target.get_handle());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_hard_compaction_of_deleted_dataset_fails_unrecoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let harness = CompactionAndResetTaskPlannersHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness.delete_dataset(&foo).await;

    let failure = harness.plan_hard_compaction(&foo).await.unwrap_err();

    assert_eq!(TaskError::empty_unrecoverable(), failure);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_hard_compaction_of_derived_dataset_fails_unrecoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let harness = CompactionAndResetTaskPlannersHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let failure = harness.plan_hard_compaction(&bar).await.unwrap_err();

    assert_eq!(TaskError::empty_unrecoverable(), failure);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_with_mismatching_old_head_fails_unrecoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let harness = CompactionAndResetTaskPlannersHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;

    let failure = harness
        .plan_reset(
            &foo,
            Some(odf::Multihash::from_digest_sha3_256(b"not-the-head")),
        )
        .await
        .unwrap_err();

    assert_eq!(TaskError::empty_unrecoverable(), failure);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_of_deleted_dataset_fails_unrecoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let harness = CompactionAndResetTaskPlannersHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness.delete_dataset(&foo).await;

    let failure = harness.plan_reset(&foo, None).await.unwrap_err();

    assert_eq!(TaskError::empty_unrecoverable(), failure);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_to_metadata_of_deleted_dataset_fails_unrecoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let harness = CompactionAndResetTaskPlannersHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness.delete_dataset(&foo).await;

    let failure = harness.plan_reset_to_metadata(&foo).await.unwrap_err();

    assert_eq!(TaskError::empty_unrecoverable(), failure);
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
        b.add::<HardCompactDatasetTaskPlanner>()
            .add::<ResetDatasetTaskPlanner>()
            .add::<ResetToMetadataDatasetTaskPlanner>()
            .add::<CompactionPlannerImpl>()
            .add::<ResetPlannerImpl>();

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        Self {
            base_use_case_harness,
            catalog: b.build(),
        }
    }

    /// Deletes the dataset's storage, as if it was deleted after the task was
    /// scheduled
    async fn delete_dataset(&self, created: &CreateDatasetResult) {
        self.dataset_storage_unit_writer()
            .delete_dataset(&created.dataset_handle.id)
            .await
            .unwrap();
    }

    async fn plan_hard_compaction(
        &self,
        created: &CreateDatasetResult,
    ) -> Result<TaskDefinition, TaskError> {
        let logical_plan = LogicalPlanDatasetHardCompact {
            dataset_id: created.dataset_handle.id.clone(),
            max_slice_size: None,
            max_slice_records: None,
        }
        .into_logical_plan();
        self.prepare::<HardCompactDatasetTaskPlanner>(&logical_plan)
            .await
    }

    /// Plans a reset of `created` to its `Seed`, expecting `HEAD` at
    /// `old_head_hash` if given
    async fn plan_reset(
        &self,
        created: &CreateDatasetResult,
        old_head_hash: Option<odf::Multihash>,
    ) -> Result<TaskDefinition, TaskError> {
        let logical_plan = LogicalPlanDatasetReset {
            dataset_id: created.dataset_handle.id.clone(),
            new_head_hash: None,
            old_head_hash,
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
        self.prepare::<ResetToMetadataDatasetTaskPlanner>(&logical_plan)
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
