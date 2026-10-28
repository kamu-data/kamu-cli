// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use container_runtime::{ContainerRuntime, ContainerRuntimeConfig};
use datafusion::arrow::datatypes::Schema;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::{
    DataFormatRegistryImpl,
    EngineConfigDatafusionEmbeddedIngest,
    EngineProvisionerNull,
    FetchService,
    LocalSyncCommitterImpl,
    ObjectStoreBuilderLocalFs,
    ObjectStoreRegistryImpl,
    PollingIngestPlannerImpl,
    PollingIngestServiceImpl,
    TransformExecutorImpl,
    TransformPlannerImpl,
};
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
use kamu_core::testing::{MockIngestIteration, ReportedHeads};
use kamu_core::*;
use kamu_datasets::{CreateDatasetResult, DatasetHeadCommitter, ResolvedDataset};
use kamu_datasets_services::DatasetKeyValueServiceSysEnv;
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
async fn test_ingest_reports_increment_from_its_result() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let reported_heads = ReportedHeads::default();

    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
        mock_polling_ingest_service: MockPollingIngestService::new().make_expect_ingest_iterations(
            &alias_foo,
            vec![MockIngestIteration::updated(&reported_heads, true, 7)],
        ),
        ..UpdateDatasetTaskRunnerMocks::default()
    });
    let foo = harness.create_root_dataset(&alias_foo).await;
    let ingested_head = harness.append_unreferenced_block(&foo, "ingested").await;
    reported_heads.set(&foo.head, &ingested_head);

    let outcome = harness.run_ingest_update(&foo).await;

    assert_eq!(
        TaskOutcome::Success(
            TaskResultDatasetUpdate::Updated {
                old_head: Some(foo.head.clone()),
                new_head: ingested_head.clone(),
                has_more: true,
                increment: odf::dataset::MetadataChainIncrementInterval {
                    num_blocks: 1,
                    num_records: 7,
                    updated_watermark: None,
                },
            }
            .into_task_result()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&foo).await, ingested_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_of_uncacheable_source_succeeds_up_to_date_without_increment() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
        mock_polling_ingest_service: MockPollingIngestService::new()
            .make_expect_ingest_up_to_date(&alias_foo, false, true),
        ..UpdateDatasetTaskRunnerMocks::default()
    });
    let foo = harness.create_root_dataset(&alias_foo).await;

    let outcome = harness.run_ingest_update(&foo).await;

    assert_eq!(
        TaskOutcome::Success(
            TaskResultDatasetUpdate::UpToDate(PullResultUpToDate::PollingIngest(
                PollingIngestResultUpToDate { uncacheable: true }
            ))
            .into_task_result()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_passes_fetch_uncacheable_to_the_ingest() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
        mock_polling_ingest_service: MockPollingIngestService::new()
            .make_expect_ingest_up_to_date(&alias_foo, true, false),
        ..UpdateDatasetTaskRunnerMocks::default()
    });
    let foo = harness.create_root_dataset(&alias_foo).await;

    let outcome = harness.run_ingest_update_fetching_uncacheable(&foo).await;

    assert_matches!(outcome, TaskOutcome::Success(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_without_polling_source_fails_unrecoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks::default());
    let foo = harness.create_root_dataset_without_source(&alias_foo).await;

    let outcome = harness.run_ingest_update(&foo).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_from_unreachable_source_fails_recoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
        mock_polling_ingest_service: MockPollingIngestService::new()
            .make_expect_ingest_iterations(&alias_foo, vec![MockIngestIteration::Unreachable]),
        ..UpdateDatasetTaskRunnerMocks::default()
    });
    let foo = harness.create_root_dataset(&alias_foo).await;

    let outcome = harness.run_ingest_update(&foo).await;

    assert_eq!(TaskOutcome::Failed(TaskError::empty_recoverable()), outcome);
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_with_real_planner_and_service_commits_and_reports_increment() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = UpdateDatasetTaskRunnerHarness::new_with_real_ingest();
    let src_path = harness.write_source_file("city,population\nA,1000\nB,2000\nC,3000\n");
    let foo = harness
        .create_root_dataset_polling_csv(&alias_foo, &src_path)
        .await;

    let outcome = harness.run_ingest_update(&foo).await;

    let new_head = harness.get_head(&foo).await;
    assert_ne!(new_head, foo.head);
    assert_matches!(
        harness.task_result_update(outcome),
        TaskResultDatasetUpdate::Updated {
            old_head: Some(old_head),
            new_head: reported_head,
            has_more: false,
            increment: odf::dataset::MetadataChainIncrementInterval {
                num_blocks: 2,
                num_records: 3,
                ..
            },
        } if old_head == foo.head && reported_head == new_head
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_with_real_service_of_unreadable_data_fails_unrecoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = UpdateDatasetTaskRunnerHarness::new_with_real_ingest();
    let src_path = harness.write_source_file("city,population\nA,not-a-number\n");
    let foo = harness
        .create_root_dataset_polling_csv(&alias_foo, &src_path)
        .await;

    let outcome = harness.run_ingest_update(&foo).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
    assert_eq!(harness.get_head(&foo).await, foo.head);
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
            TaskResultDatasetUpdate::Updated {
                old_head: Some(bar.head.clone()),
                new_head: transformed_head.clone(),
                has_more: false,
                increment: MockTransformExecutor::REPORTED_INCREMENT,
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
            TaskResultDatasetUpdate::Updated {
                old_head: Some(bar.head.clone()),
                new_head,
                has_more: false,
                increment: odf::dataset::MetadataChainIncrementInterval {
                    num_blocks: 1,
                    num_records: 0,
                    updated_watermark: Some(watermark),
                },
            }
            .into_task_result()
        ),
        outcome,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_commits_and_reports_increment() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let synced_heads = ReportedHeads::default();

    let harness = UpdateDatasetTaskRunnerHarness::new_syncing(&alias_foo, Some(&synced_heads));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let synced_head = harness.append_unreferenced_block(&foo, "synced").await;
    synced_heads.set(&foo.head, &synced_head);

    let outcome = harness.run_sync_update(&foo).await;

    assert_eq!(
        TaskOutcome::Success(
            TaskResultDatasetUpdate::Updated {
                old_head: Some(foo.head.clone()),
                new_head: synced_head.clone(),
                has_more: false,
                increment: MockSyncService::REPORTED_INCREMENT,
            }
            .into_task_result()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&foo).await, synced_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_expects_head_read_when_it_runs() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let synced_heads = ReportedHeads::default();

    let harness = UpdateDatasetTaskRunnerHarness::new_syncing(&alias_foo, Some(&synced_heads));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let head_at_run = harness
        .append_unreferenced_block(&foo, "after planning")
        .await;
    harness.move_head(&foo, &foo.head, &head_at_run).await;
    let synced_head = harness.append_unreferenced_block(&foo, "synced").await;
    synced_heads.set(&head_at_run, &synced_head);

    let outcome = harness.run_sync_update(&foo).await;

    assert_matches!(
        harness.task_result_update(outcome),
        TaskResultDatasetUpdate::Updated { old_head: Some(old_head), new_head, .. }
            if old_head == head_at_run && new_head == synced_head
    );
    assert_eq!(harness.get_head(&foo).await, synced_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_into_missing_dataset_is_an_internal_error() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks::default());
    let foo = harness.create_root_dataset(&alias_foo).await;

    assert_matches!(
        harness.try_run_sync_update_into_new(&foo, &alias_bar).await,
        Err(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_up_to_date() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = UpdateDatasetTaskRunnerHarness::new_syncing(&alias_foo, None);
    let foo = harness.create_root_dataset(&alias_foo).await;

    let outcome = harness.run_sync_update(&foo).await;

    assert_eq!(
        TaskOutcome::Success(
            TaskResultDatasetUpdate::UpToDate(PullResultUpToDate::Sync).into_task_result()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_head_moved_concurrently_fails_recoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let synced_heads = ReportedHeads::default();

    let harness = UpdateDatasetTaskRunnerHarness::new_syncing(&alias_foo, Some(&synced_heads));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let synced_head = harness.append_unreferenced_block(&foo, "synced").await;
    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
    synced_heads.set(&foo.head, &synced_head);
    harness.move_head_before_next_commit(concurrent_head.clone());

    let outcome = harness.run_sync_update(&foo).await;

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
    assert!(outcome.is_recoverable_failure());
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from_diverged_source_fails_unrecoverably() {
    let outcome = UpdateDatasetTaskRunnerHarness::run_sync_failing_with(|| {
        SyncError::DatasetsDiverged(DatasetsDivergedError {
            src_head: odf::Multihash::from_digest_sha3_256(b"src"),
            dst_head: odf::Multihash::from_digest_sha3_256(b"dst"),
            detail: None,
        })
    })
    .await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_from_unreachable_source_fails_recoverably() {
    let outcome = UpdateDatasetTaskRunnerHarness::run_sync_failing_with(|| {
        SyncError::Internal("connection refused".int_err())
    })
    .await;

    assert_eq!(TaskOutcome::Failed(TaskError::empty_recoverable()), outcome);
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
        Self::build(|b, _| {
            b.add_value(mocks.mock_sync_service)
                .bind::<dyn SyncService, MockSyncService>()
                .add_value(mocks.mock_polling_ingest_service)
                .bind::<dyn PollingIngestService, MockPollingIngestService>()
                .add_value(mocks.mock_transform_planner)
                .bind::<dyn TransformPlanner, MockTransformPlanner>()
                .add_value(mocks.mock_transform_executor)
                .bind::<dyn TransformExecutor, MockTransformExecutor>();
        })
    }

    /// Transforms with the real planner and executor, through an engine that
    /// only advances the watermark to `watermark`
    fn new_with_real_transform(watermark: DateTime<Utc>) -> Self {
        Self::build(|b, _| {
            b.add_value(MockSyncService::new())
                .bind::<dyn SyncService, MockSyncService>()
                .add_value(MockPollingIngestService::new())
                .bind::<dyn PollingIngestService, MockPollingIngestService>()
                .add::<TransformPlannerImpl>()
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

    /// Ingests with the real planner and service, and the default transform
    /// mocks
    fn new_with_real_ingest() -> Self {
        Self::build(|b, temp_dir| {
            let cache_dir = temp_dir.join("cache");
            std::fs::create_dir(&cache_dir).unwrap();

            b.add_value(MockSyncService::new())
                .bind::<dyn SyncService, MockSyncService>()
                .add::<PollingIngestServiceImpl>()
                .add::<FetchService>()
                .add_value(CacheDir::new(cache_dir))
                .add_value(ContainerRuntimeConfig::default())
                .add::<ContainerRuntime>()
                .add::<DatasetKeyValueServiceSysEnv>()
                .add::<ObjectStoreRegistryImpl>()
                .add::<ObjectStoreBuilderLocalFs>()
                .add::<DataFormatRegistryImpl>()
                .add_value(EngineConfigDatafusionEmbeddedIngest::default())
                .add::<EngineProvisionerNull>()
                .add_value(MockTransformPlanner::new())
                .bind::<dyn TransformPlanner, MockTransformPlanner>()
                .add_value(MockTransformExecutor::new())
                .bind::<dyn TransformExecutor, MockTransformExecutor>();
        })
    }

    /// `register_operations` registers the ingest and transform services; it
    /// gets the harness's temporary directory
    fn build(register_operations: impl FnOnce(&mut dill::CatalogBuilder, &Path)) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new());

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<UpdateDatasetTaskRunner>()
            .add::<PollingIngestPlannerImpl>()
            .add_value(RacingDatasetHeadCommitter::default())
            .bind::<dyn DatasetHeadCommitter, RacingDatasetHeadCommitter>()
            .add::<LocalSyncCommitterImpl>();
        register_operations(&mut b, base_use_case_harness.temp_dir_path());

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        Self {
            base_use_case_harness,
            runner: catalog.get_one::<UpdateDatasetTaskRunner>().unwrap(),
            racing_committer: catalog.get_one().unwrap(),
        }
    }

    /// Syncs `alias` once from its remote: up to date without `heads`,
    /// otherwise transferred up to their new head
    fn new_syncing(alias: &odf::DatasetAlias, heads: Option<&ReportedHeads>) -> Self {
        Self::new(UpdateDatasetTaskRunnerMocks {
            mock_sync_service: MockSyncService::new()
                .make_expect_transfer_from_remote_into_existing_local(
                    alias.clone(),
                    Self::sync_remote_ref(alias),
                    heads.cloned(),
                ),
            ..UpdateDatasetTaskRunnerMocks::default()
        })
    }

    fn sync_remote_ref(alias: &odf::DatasetAlias) -> odf::DatasetRefRemote {
        format!("odf+http://example.com/{}", alias.dataset_name)
            .parse()
            .unwrap()
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

    /// Runs the sync of a fresh dataset whose transfer fails with
    /// `make_error()`
    async fn run_sync_failing_with(
        make_error: impl Fn() -> SyncError + Send + 'static,
    ) -> TaskOutcome {
        let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
        let harness = Self::new(UpdateDatasetTaskRunnerMocks {
            mock_sync_service: MockSyncService::new()
                .make_expect_transfer_error(alias_foo.clone(), make_error),
            ..UpdateDatasetTaskRunnerMocks::default()
        });
        let foo = harness.create_root_dataset(&alias_foo).await;

        harness.run_sync_update(&foo).await
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

    /// Writes the file a polling source reads, in the harness's directory
    fn write_source_file(&self, contents: &str) -> PathBuf {
        let path = self.temp_dir_path().join("data.csv");
        std::fs::write(&path, contents).unwrap();
        path
    }

    fn task_result_update(&self, outcome: TaskOutcome) -> TaskResultDatasetUpdate {
        let TaskOutcome::Success(task_result) = outcome else {
            panic!("Expected a success, got {outcome:?}");
        };
        TaskResultDatasetUpdate::from_task_result(&task_result).unwrap()
    }

    async fn run_ingest_update(&self, created: &CreateDatasetResult) -> TaskOutcome {
        self.run_update(DatasetUpdateJob::Ingest(created.dataset_handle.clone()))
            .await
    }

    async fn run_ingest_update_fetching_uncacheable(
        &self,
        created: &CreateDatasetResult,
    ) -> TaskOutcome {
        self.run_update_with(
            DatasetUpdateJob::Ingest(created.dataset_handle.clone()),
            PullOptions {
                ingest_options: PollingIngestOptions {
                    fetch_uncacheable: true,
                    ..PollingIngestOptions::default()
                },
                ..PullOptions::default()
            },
        )
        .await
    }

    async fn run_transform_update(&self, created: &CreateDatasetResult) -> TaskOutcome {
        self.run_update(DatasetUpdateJob::Transform(created.dataset_handle.clone()))
            .await
    }

    /// Syncs `created` from its remote, planned when `created` was at its
    /// creation head; the mocked transfer ignores the source
    async fn run_sync_update(&self, created: &CreateDatasetResult) -> TaskOutcome {
        let dataset = ResolvedDataset::from_created(created);
        self.run_update(Self::sync_job(
            created,
            PullLocalTarget::existing(created.dataset_handle.clone()),
            LocalSyncTarget::Existing {
                dataset,
                expected_head: Some(created.head.clone()),
            },
        ))
        .await
    }

    /// Syncs into a dataset `alias` that does not exist yet, using `source`
    /// as the remote's storage
    async fn try_run_sync_update_into_new(
        &self,
        source: &CreateDatasetResult,
        alias: &odf::DatasetAlias,
    ) -> Result<TaskOutcome, InternalError> {
        self.try_run_update(
            Self::sync_job(
                source,
                PullLocalTarget::to_create(alias.clone()),
                LocalSyncTarget::New(alias.clone()),
            ),
            PullOptions::default(),
        )
        .await
    }

    fn sync_job(
        source: &CreateDatasetResult,
        local_target: PullLocalTarget,
        dst: LocalSyncTarget,
    ) -> DatasetUpdateJob {
        let remote_ref = Self::sync_remote_ref(local_target.alias());
        let odf::DatasetRefRemote::Url(url) = &remote_ref else {
            unreachable!()
        };

        DatasetUpdateJob::Sync(PullSyncItem {
            depth: 0,
            local_target,
            remote_ref: remote_ref.clone(),
            maybe_original_request: None,
            sync_request: Box::new(LocalSyncRequest {
                src: SyncRef::Remote(SyncRefRemote {
                    url: url.clone(),
                    dataset: source.dataset.clone(),
                    original_remote_ref: remote_ref.clone(),
                }),
                dst,
            }),
        })
    }

    async fn run_update(&self, job: DatasetUpdateJob) -> TaskOutcome {
        self.run_update_with(job, PullOptions::default()).await
    }

    async fn run_update_with(
        &self,
        job: DatasetUpdateJob,
        pull_options: PullOptions,
    ) -> TaskOutcome {
        self.try_run_update(job, pull_options).await.unwrap()
    }

    async fn try_run_update(
        &self,
        job: DatasetUpdateJob,
        pull_options: PullOptions,
    ) -> Result<TaskOutcome, InternalError> {
        self.runner
            .run_task(TaskDefinition::new(TaskDefinitionDatasetUpdate {
                pull_options,
                job,
            }))
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct UpdateDatasetTaskRunnerMocks {
    mock_sync_service: MockSyncService,
    mock_polling_ingest_service: MockPollingIngestService,
    mock_transform_planner: MockTransformPlanner,
    mock_transform_executor: MockTransformExecutor,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
