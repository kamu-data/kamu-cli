// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::TransformRequestPlannerImpl;
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu_adapter_task_dataset::{
    DatasetHeadMovedError,
    DatasetUpdateJob,
    InputDatasetCompactedError,
    TaskDefinitionDatasetUpdate,
    TaskErrorDatasetCommit,
    TaskErrorDatasetUpdate,
    TaskResultDatasetUpdate,
    UpdateDatasetTaskRunner,
};
use kamu_core::testing::ReportedHeads;
use kamu_core::*;
use kamu_datasets::{CreateDatasetResult, DatasetHeadCommitter, ResolvedDataset};
use kamu_datasets_services::testing::RacingDatasetHeadCommitter;
use kamu_task_system::{TaskDefinition, TaskOutcome, TaskResult, TaskRunner};
use pretty_assertions::{assert_eq, assert_matches};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_head_moved_concurrently_fails_recoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let reported_heads = ReportedHeads::default();

    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
        mock_polling_ingest_service: MockPollingIngestService::new()
            .make_expect_ingest_updated(alias_foo.clone(), reported_heads.clone()),
        ..UpdateDatasetTaskRunnerMocks::default()
    });
    let foo = harness.create_root_dataset(&alias_foo).await;
    let ingested_head = harness.append_unreferenced_block(&foo, "ingested").await;
    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
    reported_heads.set(&foo.head, &ingested_head);
    harness.move_head(&foo, &foo.head, &concurrent_head).await;

    let outcome = harness.run_ingest_update(&foo).await;

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
async fn test_transform_head_moved_concurrently_fails_recoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let reported_heads = ReportedHeads::default();

    let harness = UpdateDatasetTaskRunnerHarness::new_transforming(&alias_bar, &reported_heads);
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    let transformed_head = harness.append_unreferenced_block(&bar, "transformed").await;
    let concurrent_head = harness.append_unreferenced_block(&bar, "concurrent").await;
    reported_heads.set(&bar.head, &transformed_head);
    harness.move_head_before_next_commit(concurrent_head.clone());

    let outcome = harness.run_transform_update(&bar).await;

    assert_eq!(
        TaskOutcome::Failed(
            TaskErrorDatasetCommit::HeadMoved(DatasetHeadMovedError {
                dataset_id: bar.dataset_handle.id.clone(),
                expected_head: Some(bar.head.clone()),
                actual_head: Some(concurrent_head.clone()),
            })
            .into_task_error()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&bar).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_input_not_pulled_succeeds_as_up_to_date() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let foo_handle = odf::DatasetHandle::new(
        odf::DatasetID::new_seeded_ed25519(b"foo"),
        alias_foo.clone(),
        odf::DatasetKind::Root,
    );

    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
        mock_transform_elaboration_service: MockTransformElaborationService::new()
            .make_expect_elaborate_transform_error(alias_bar.clone(), move || {
                InputNotPulledError {
                    dataset_handle: foo_handle.clone(),
                }
                .into()
            }),
        ..UpdateDatasetTaskRunnerMocks::default()
    });
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let outcome = harness.run_transform_update(&bar).await;

    assert_eq!(TaskOutcome::Success(TaskResult::empty()), outcome);
    assert_eq!(harness.get_head(&bar).await, bar.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_diverged_input_fails_as_input_compacted() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
        mock_transform_elaboration_service: MockTransformElaborationService::new()
            .make_expect_elaborate_transform_diverged(alias_bar.clone()),
        ..UpdateDatasetTaskRunnerMocks::default()
    });
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let outcome = harness.run_transform_update(&bar).await;

    assert_eq!(
        TaskOutcome::Failed(
            TaskErrorDatasetUpdate::InputDatasetCompacted(InputDatasetCompactedError {
                dataset_id: odf::DatasetID::new_seeded_ed25519(b"input"),
            })
            .into_task_error()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&bar).await, bar.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_success_carries_increment_from_executor() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let reported_heads = ReportedHeads::default();

    let harness = UpdateDatasetTaskRunnerHarness::new_transforming(&alias_bar, &reported_heads);
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    let transformed_head = harness.append_unreferenced_block(&bar, "transformed").await;
    reported_heads.set(&bar.head, &transformed_head);

    let outcome = harness.run_transform_update(&bar).await;

    assert_eq!(
        TaskOutcome::Success(
            TaskResultDatasetUpdate {
                pull_result: PullResult::Updated {
                    old_head: Some(bar.head.clone()),
                    new_head: transformed_head.clone(),
                    has_more: false,
                },
                data_increment: Some(MockTransformExecutor::REPORTED_INCREMENT),
            }
            .into_task_result()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&bar).await, transformed_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_plans_at_run_time_head() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let reported_heads = ReportedHeads::default();

    let harness = UpdateDatasetTaskRunnerHarness::new_transforming(&alias_bar, &reported_heads);
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    // Another writer commits after the flow queued the task
    let concurrent_head = harness.append_unreferenced_block(&bar, "concurrent").await;
    harness.move_head(&bar, &bar.head, &concurrent_head).await;
    let transformed_head = harness.append_unreferenced_block(&bar, "transformed").await;
    reported_heads.set(&concurrent_head, &transformed_head);

    let outcome = harness.run_transform_update(&bar).await;

    assert_matches!(outcome, TaskOutcome::Success(_));
    assert_eq!(harness.get_head(&bar).await, transformed_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct UpdateDatasetTaskRunnerHarness {
    base_use_case_harness: BaseUseCaseHarness,
    runner: Arc<dyn TaskRunner>,
    racing_committer: Arc<RacingDatasetHeadCommitter>,
}

impl UpdateDatasetTaskRunnerHarness {
    fn new(mocks: UpdateDatasetTaskRunnerMocks) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new());

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<UpdateDatasetTaskRunner>()
            .add_value(RacingDatasetHeadCommitter::default())
            .bind::<dyn DatasetHeadCommitter, RacingDatasetHeadCommitter>()
            .add::<TransformRequestPlannerImpl>()
            .add_value(mocks.mock_polling_ingest_service)
            .bind::<dyn PollingIngestService, MockPollingIngestService>()
            .add_value(mocks.mock_transform_elaboration_service)
            .bind::<dyn TransformElaborationService, MockTransformElaborationService>()
            .add_value(mocks.mock_transform_executor)
            .bind::<dyn TransformExecutor, MockTransformExecutor>()
            .add_value(MockSyncService::new())
            .bind::<dyn SyncService, MockSyncService>();

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        Self {
            base_use_case_harness,
            runner: catalog.get_one::<UpdateDatasetTaskRunner>().unwrap(),
            racing_committer: catalog.get_one().unwrap(),
        }
    }

    /// Elaborates and transforms `alias` once, expecting the transform to be
    /// elaborated at the old head set in `heads` and reporting the new one
    fn new_transforming(alias: &odf::DatasetAlias, heads: &ReportedHeads) -> Self {
        Self::new(UpdateDatasetTaskRunnerMocks {
            mock_transform_elaboration_service: MockTransformElaborationService::new()
                .make_expect_elaborate_transform(alias.clone()),
            mock_transform_executor: MockTransformExecutor::new()
                .make_expect_transform_updated_at_plan_head(alias.clone(), heads.clone()),
            ..UpdateDatasetTaskRunnerMocks::default()
        })
    }

    fn move_head_before_next_commit(&self, concurrent_head: odf::Multihash) {
        self.racing_committer
            .move_head_before_next_commit(concurrent_head);
    }

    async fn run_ingest_update(&self, created: &CreateDatasetResult) -> TaskOutcome {
        let metadata_state = DataWriterMetadataState::build(
            ResolvedDataset::from_created(created),
            &odf::BlockRef::Head,
            None,
            None,
        )
        .await
        .unwrap();

        self.run_update(DatasetUpdateJob::Ingest(PullIngestItem {
            depth: 0,
            target: ResolvedDataset::from_created(created),
            metadata_state: Box::new(metadata_state),
            maybe_original_request: None,
        }))
        .await
    }

    async fn run_transform_update(&self, created: &CreateDatasetResult) -> TaskOutcome {
        self.run_update(DatasetUpdateJob::Transform(created.dataset_handle.clone()))
            .await
    }

    async fn run_update(&self, job: DatasetUpdateJob) -> TaskOutcome {
        self.runner
            .run_task(TaskDefinition::new(TaskDefinitionDatasetUpdate {
                pull_options: PullOptions::default(),
                job,
            }))
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct UpdateDatasetTaskRunnerMocks {
    mock_polling_ingest_service: MockPollingIngestService,
    mock_transform_elaboration_service: MockTransformElaborationService,
    mock_transform_executor: MockTransformExecutor,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
