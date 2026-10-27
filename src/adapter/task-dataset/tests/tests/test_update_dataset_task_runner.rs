// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::datatypes::Schema;
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::{TransformExecutorImpl, TransformPlannerImpl};
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
use kamu_core::engine::TransformResponseExt;
use kamu_core::testing::ReportedHeads;
use kamu_core::*;
use kamu_datasets::{CreateDatasetResult, DatasetHeadCommitter, ResolvedDataset};
use kamu_datasets_services::testing::RacingDatasetHeadCommitter;
use kamu_task_system::{TaskDefinition, TaskError, TaskOutcome, TaskResult, TaskRunner};
use pretty_assertions::assert_eq;

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

    let harness =
        UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
            mock_transform_planner: MockTransformPlanner::new()
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
        mock_transform_planner: MockTransformPlanner::new()
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
async fn test_transform_incompatible_schema_fails_unrecoverably() {
    let outcome = UpdateDatasetTaskRunnerHarness::run_transform_failing_with(|| {
        IncompatibleSchemaError::new(
            "Schema of the new slice is not compatible",
            Arc::new(Schema::empty()),
            Arc::new(Schema::empty()),
        )
        .into()
    })
    .await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_unsupported_engine_fails_unrecoverably() {
    let outcome = UpdateDatasetTaskRunnerHarness::run_transform_failing_with(|| {
        EngineProvisioningError::from(UnsupportedEngineError {
            engine_id: "cobol".to_string(),
        })
        .into()
    })
    .await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_up_to_date_succeeds_empty() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
        mock_transform_planner: MockTransformPlanner::new()
            .make_expect_elaborate_transform_up_to_date(alias_bar.clone()),
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
async fn test_transform_not_defined_fails_unrecoverably() {
    let outcome = UpdateDatasetTaskRunnerHarness::run_transform_elaboration_failing_with(|_| {
        TransformNotDefinedError {}.into()
    })
    .await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_inputs_changed_fails_unrecoverably() {
    let outcome =
        UpdateDatasetTaskRunnerHarness::run_transform_elaboration_failing_with(|target| {
            TransformInputsChangedError {
                dataset_handle: target.clone(),
            }
            .into()
        })
        .await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_with_real_planner_and_executor_commits_and_reports_increment() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let watermark = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();

    let harness = UpdateDatasetTaskRunnerHarness::new_with_real_transform(watermark);
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness
        .append_schema_and_watermark(&foo, Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap())
        .await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let outcome = harness.run_transform_update(&bar).await;

    let new_head = harness.get_head(&bar).await;
    assert_ne!(new_head, bar.head);
    assert_eq!(
        TaskOutcome::Success(
            TaskResultDatasetUpdate {
                pull_result: PullResult::Updated {
                    old_head: Some(bar.head.clone()),
                    new_head,
                    has_more: false,
                },
                data_increment: Some(odf::dataset::MetadataChainIncrementInterval {
                    num_blocks: 1,
                    num_records: 0,
                    updated_watermark: Some(watermark),
                }),
            }
            .into_task_result()
        ),
        outcome,
    );
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
        Self::build(mocks.mock_polling_ingest_service, |b| {
            b.add_value(mocks.mock_transform_planner)
                .bind::<dyn TransformPlanner, MockTransformPlanner>()
                .add_value(mocks.mock_transform_executor)
                .bind::<dyn TransformExecutor, MockTransformExecutor>();
        })
    }

    /// Transforms with the real planner and executor, through an engine that
    /// only advances the watermark to `watermark`
    fn new_with_real_transform(watermark: DateTime<Utc>) -> Self {
        Self::build(MockPollingIngestService::new(), |b| {
            b.add::<TransformPlannerImpl>()
                .add::<TransformExecutorImpl>()
                .add_value(
                    MockEngineProvisioner::new().always_provision_engine_responding(move || {
                        TransformResponseExt {
                            new_offset_interval: None,
                            new_watermark: Some(watermark),
                            output_schema: None,
                            new_checkpoint: None,
                            new_data: None,
                            run_dir: None,
                        }
                    }),
                )
                .bind::<dyn EngineProvisioner, MockEngineProvisioner>();
        })
    }

    fn build(
        mock_polling_ingest_service: MockPollingIngestService,
        register_transform: impl FnOnce(&mut dill::CatalogBuilder),
    ) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new());

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<UpdateDatasetTaskRunner>()
            .add_value(RacingDatasetHeadCommitter::default())
            .bind::<dyn DatasetHeadCommitter, RacingDatasetHeadCommitter>()
            .add_value(mock_polling_ingest_service)
            .bind::<dyn PollingIngestService, MockPollingIngestService>()
            .add_value(MockSyncService::new())
            .bind::<dyn SyncService, MockSyncService>();
        register_transform(&mut b);

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        Self {
            base_use_case_harness,
            runner: catalog.get_one::<UpdateDatasetTaskRunner>().unwrap(),
            racing_committer: catalog.get_one().unwrap(),
        }
    }

    /// Elaborates and transforms `alias` once, from the old to the new head set
    /// in `heads`
    fn new_transforming(alias: &odf::DatasetAlias, heads: &ReportedHeads) -> Self {
        Self::new(UpdateDatasetTaskRunnerMocks {
            mock_transform_planner: MockTransformPlanner::new()
                .make_expect_elaborate_transform(alias.clone()),
            mock_transform_executor: MockTransformExecutor::new()
                .make_expect_transform_updated(alias.clone(), heads.clone()),
            ..UpdateDatasetTaskRunnerMocks::default()
        })
    }

    /// Runs the transform of a fresh derivative whose execution fails with
    /// `make_error()`
    async fn run_transform_failing_with(
        make_error: impl Fn() -> TransformExecuteError + Send + 'static,
    ) -> TaskOutcome {
        let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
        let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
        let harness = Self::new(UpdateDatasetTaskRunnerMocks {
            mock_transform_planner: MockTransformPlanner::new()
                .make_expect_elaborate_transform(alias_bar.clone()),
            mock_transform_executor: MockTransformExecutor::new()
                .make_expect_transform_error(alias_bar.clone(), make_error),
            ..UpdateDatasetTaskRunnerMocks::default()
        });
        let foo = harness.create_root_dataset(&alias_foo).await;
        let bar = harness
            .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
            .await;

        harness.run_transform_update(&bar).await
    }

    /// Runs the transform of a fresh derivative whose elaboration fails with
    /// `make_error(target)`
    async fn run_transform_elaboration_failing_with(
        make_error: impl Fn(&odf::DatasetHandle) -> TransformElaborateError + Send + Sync + 'static,
    ) -> TaskOutcome {
        let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
        let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
        let mut mock_transform_planner = MockTransformPlanner::new();
        mock_transform_planner
            .expect_elaborate_transform()
            .times(1)
            .returning(move |target| Err(make_error(target.get_handle())));
        let harness = Self::new(UpdateDatasetTaskRunnerMocks {
            mock_transform_planner,
            ..UpdateDatasetTaskRunnerMocks::default()
        });
        let foo = harness.create_root_dataset(&alias_foo).await;
        let bar = harness
            .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
            .await;

        harness.run_transform_update(&bar).await
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
    mock_transform_planner: MockTransformPlanner,
    mock_transform_executor: MockTransformExecutor,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
