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
    CompactionDatasetTaskPlanner,
    DatasetResetOldHeadMismatch,
    LogicalPlanDatasetHardCompact,
    LogicalPlanDatasetReset,
    LogicalPlanDatasetResetToMetadata,
    ResetDatasetTaskPlanner,
    TaskDefinitionDatasetHardCompact,
    TaskDefinitionDatasetResetToMetadata,
    TaskErrorDatasetReset,
};
use kamu_core::CompactionDataSliceBatch;
use kamu_datasets::CreateDatasetResult;
use kamu_task_system::{
    LogicalPlan,
    PrepareTaskDefinitionError,
    TaskDefinition,
    TaskDefinitionPlanner,
    TaskError,
    TaskID,
};
use odf::metadata::testing::MetadataFactory;
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

    let not_the_head = odf::Multihash::from_digest_sha3_256(b"not-the-head");

    let failure = harness
        .plan_reset(&foo, Some(not_the_head.clone()))
        .await
        .unwrap_err();

    assert_eq!(
        TaskErrorDatasetReset::OldHeadMismatch(DatasetResetOldHeadMismatch {
            expected_head: not_the_head,
            actual_head: foo.head,
        })
        .into_task_error(),
        failure
    );
    assert!(!failure.recoverable);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_hard_compaction_merges_slices_up_to_the_limits() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let harness = CompactionAndResetTaskPlannersHarness::new();
    let foo = harness
        .create_root_dataset_with_two_slices(&alias_foo)
        .await;

    for (case, max_slice_size, max_slice_records, expected_merged_batches) in [
        ("unlimited", None, None, 1),
        (
            "max_slice_records",
            None,
            Some(CompactionAndResetTaskPlannersHarness::SLICE_RECORDS),
            0,
        ),
        (
            "max_slice_size",
            Some(CompactionAndResetTaskPlannersHarness::SLICE_SIZE),
            None,
            0,
        ),
    ] {
        assert_eq!(
            expected_merged_batches,
            harness
                .plan_hard_compaction_merged_batches(&foo, max_slice_size, max_slice_records)
                .await,
            "{case}",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_to_metadata_of_derived_dataset_plans_for_the_dataset() {
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
    assert_eq!(bar.dataset_handle, *task_reset.target.get_handle());
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
            .add::<ResetDatasetTaskPlanner>()
            .add::<CompactionPlannerImpl>()
            .add::<ResetPlannerImpl>();

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        Self {
            base_use_case_harness,
            catalog: b.build(),
        }
    }

    /// Records in each slice of [`Self::create_root_dataset_with_two_slices`]
    const SLICE_RECORDS: u64 = 10;
    /// Bytes in each slice of [`Self::create_root_dataset_with_two_slices`]
    const SLICE_SIZE: u64 = 100;

    async fn plan(
        &self,
        planned_task: PlannedTask,
        created: &CreateDatasetResult,
    ) -> Result<TaskDefinition, TaskError> {
        match planned_task {
            PlannedTask::HardCompaction => self.plan_hard_compaction(created).await,
            PlannedTask::Reset => self.plan_reset(created, None).await,
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

    /// How many batches of several slices the planned hard compaction merges
    async fn plan_hard_compaction_merged_batches(
        &self,
        created: &CreateDatasetResult,
        max_slice_size: Option<u64>,
        max_slice_records: Option<u64>,
    ) -> usize {
        let task_compact = self
            .plan_hard_compaction_with(created, max_slice_size, max_slice_records)
            .await
            .unwrap()
            .downcast::<TaskDefinitionDatasetHardCompact>()
            .unwrap();

        task_compact
            .compaction_plan
            .data_slice_batches
            .iter()
            .filter(|batch| match batch {
                CompactionDataSliceBatch::CompactedBatch(_) => true,
                CompactionDataSliceBatch::SingleBlock(_) => false,
            })
            .count()
    }

    /// A root dataset with two data slices of [`Self::SLICE_RECORDS`] records
    /// and [`Self::SLICE_SIZE`] bytes each, which a hard compaction without
    /// limits merges into one
    async fn create_root_dataset_with_two_slices(
        &self,
        alias: &odf::DatasetAlias,
    ) -> CreateDatasetResult {
        let created = self
            .create_root_dataset_with_events(
                alias,
                vec![MetadataFactory::set_data_schema().build().into()],
            )
            .await;

        let chain = created.dataset.as_metadata_chain();
        for start in [0, Self::SLICE_RECORDS] {
            let head = self.get_head(&created).await;
            let head_block = chain.get_block(&head).await.unwrap();
            chain
                .append(
                    MetadataFactory::metadata_block(
                        MetadataFactory::add_data()
                            .new_offset_interval(start, start + Self::SLICE_RECORDS - 1)
                            .new_data_size(Self::SLICE_SIZE)
                            .build(),
                    )
                    .prev(&head, head_block.sequence_number)
                    .system_time(self.system_time_source().now())
                    .build(),
                    odf::dataset::AppendOpts::default(),
                )
                .await
                .unwrap();
        }

        created
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
