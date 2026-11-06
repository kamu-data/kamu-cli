// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use container_runtime::{ContainerRuntime, ContainerRuntimeConfig};
use datafusion::arrow::datatypes::Schema;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu::testing::{
    BaseUseCaseHarness,
    BaseUseCaseHarnessOptions,
    DatasetTestHelper,
    DummySmartTransferProtocolClient,
    RacingDatasetUpdateSteps,
};
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::utils::simple_transfer_protocol::SimpleTransferProtocol;
use kamu::{
    DataFormatRegistryImpl,
    EngineConfigDatafusionEmbeddedIngest,
    EngineProvisionerNull,
    FetchService,
    IpfsServiceImpl,
    LocalSyncCommitterImpl,
    ObjectStoreBuilderLocalFs,
    ObjectStoreRegistryImpl,
    PollingIngestPlannerImpl,
    PollingIngestServiceImpl,
    RemoteDatasetUrlResolverImpl,
    RemoteReposDir,
    RemoteRepositoryRegistryImpl,
    SyncRequestBuilder,
    SyncServiceImpl,
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
    UpdateTargetMissingError,
};
use kamu_core::engine::TransformResponseExt;
use kamu_core::testing::{MockIngestIteration, ReportedHeads};
use kamu_core::*;
use kamu_datasets::{
    CreateDatasetResult,
    DatasetEnvVar,
    DatasetEnvVarResolver,
    DatasetEnvVarValue,
    DatasetHeadCommitter,
    GetDatasetEnvVarError,
    SecretsEncryptionConfig,
};
use kamu_datasets_services::DatasetKeyValueServiceSysEnv;
use kamu_datasets_services::testing::RacingDatasetHeadCommitter;
use kamu_task_system::{TaskDefinition, TaskError, TaskOutcome, TaskRunner};
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::assert_eq;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_head_moved_concurrently_fails_recoverably() {
    let (harness, foo, _) = UpdateDatasetTaskRunnerHarness::new_ingesting_new_root(
        &UpdateDatasetTaskRunnerHarness::alias("foo"),
    )
    .await;
    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
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
    let alias_foo = UpdateDatasetTaskRunnerHarness::alias("foo");
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
    let alias_foo = UpdateDatasetTaskRunnerHarness::alias("foo");

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
    let alias_foo = UpdateDatasetTaskRunnerHarness::alias("foo");

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
    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks::default());
    let foo = harness
        .create_root_dataset_without_source(&UpdateDatasetTaskRunnerHarness::alias("foo"))
        .await;

    let outcome = harness.run_ingest_update(&foo).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_from_unreachable_source_fails_recoverably() {
    let alias_foo = UpdateDatasetTaskRunnerHarness::alias("foo");

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
    let harness = UpdateDatasetTaskRunnerHarness::new_with_real_ingest();
    let src_path = harness.write_source_file("city,population\nA,1000\nB,2000\nC,3000\n");
    let foo = harness
        .create_root_dataset_polling_csv(&UpdateDatasetTaskRunnerHarness::alias("foo"), &src_path)
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
    let harness = UpdateDatasetTaskRunnerHarness::new_with_real_ingest();
    let src_path = harness.write_source_file("city,population\nA,not-a-number\n");
    let foo = harness
        .create_root_dataset_polling_csv(&UpdateDatasetTaskRunnerHarness::alias("foo"), &src_path)
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
async fn test_transform_input_not_pulled_succeeds_as_up_to_date() {
    let alias_bar = UpdateDatasetTaskRunnerHarness::alias("bar");
    let input_handle = odf::DatasetHandle::new(
        odf::DatasetID::new_seeded_ed25519(b"foo"),
        UpdateDatasetTaskRunnerHarness::alias("foo"),
        odf::DatasetKind::Root,
    );

    let harness =
        UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
            mock_transform_planner: MockTransformPlanner::new()
                .make_expect_elaborate_transform_error(alias_bar.clone(), move || {
                    InputNotPulledError {
                        dataset_handle: input_handle.clone(),
                    }
                    .into()
                }),
            ..UpdateDatasetTaskRunnerMocks::default()
        });
    let bar = harness.create_derivative(&alias_bar).await;

    let outcome = harness.run_transform_update(&bar).await;

    assert_eq!(
        TaskOutcome::Success(
            TaskResultDatasetUpdate::UpToDate(PullResultUpToDate::Transform).into_task_result()
        ),
        outcome
    );
    assert_eq!(harness.get_head(&bar).await, bar.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_diverged_input_fails_as_input_compacted() {
    let alias_bar = UpdateDatasetTaskRunnerHarness::alias("bar");

    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
        mock_transform_planner: MockTransformPlanner::new()
            .make_expect_elaborate_transform_diverged(alias_bar.clone()),
        ..UpdateDatasetTaskRunnerMocks::default()
    });
    let bar = harness.create_derivative(&alias_bar).await;

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
    let alias_bar = UpdateDatasetTaskRunnerHarness::alias("bar");
    let reported_heads = ReportedHeads::default();

    let harness = UpdateDatasetTaskRunnerHarness::new_transforming(&alias_bar, &reported_heads);
    let bar = harness.create_derivative(&alias_bar).await;
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
async fn test_transform_execution_failure_fails_as_recoverable_as_its_error() {
    /// Case name, the failure, and whether the task may be retried
    type Case = (&'static str, fn() -> TransformExecuteError, bool);
    let cases: [Case; 2] = [
        (
            "engine_crash",
            || TransformExecuteError::Internal("Engine container exited unexpectedly".int_err()),
            true,
        ),
        (
            "incompatible_schema",
            || {
                IncompatibleSchemaError::new(
                    "Schema of the new slice is not compatible",
                    Arc::new(Schema::empty()),
                    Arc::new(Schema::empty()),
                )
                .into()
            },
            false,
        ),
    ];

    for (case, make_error, recoverable) in cases {
        let outcome = UpdateDatasetTaskRunnerHarness::run_transform_failing_with(make_error).await;

        assert_eq!(
            TaskOutcome::Failed(UpdateDatasetTaskRunnerHarness::empty_task_error(
                recoverable
            )),
            outcome,
            "{case}",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_up_to_date_reports_up_to_date() {
    let alias_bar = UpdateDatasetTaskRunnerHarness::alias("bar");

    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
        mock_transform_planner: MockTransformPlanner::new()
            .make_expect_elaborate_transform_up_to_date(alias_bar.clone()),
        ..UpdateDatasetTaskRunnerMocks::default()
    });
    let bar = harness.create_derivative(&alias_bar).await;

    let outcome = harness.run_transform_update(&bar).await;

    assert_eq!(
        TaskOutcome::Success(
            TaskResultDatasetUpdate::UpToDate(PullResultUpToDate::Transform).into_task_result()
        ),
        outcome
    );
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
async fn test_transform_with_real_planner_and_executor_commits_and_reports_increment() {
    let watermark = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();

    let harness = UpdateDatasetTaskRunnerHarness::new_with_real_transform(watermark);
    let foo = harness
        .create_root_dataset(&UpdateDatasetTaskRunnerHarness::alias("foo"))
        .await;
    harness
        .append_schema_and_watermark(&foo, Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap())
        .await;
    let bar = harness
        .create_derived_dataset(
            &UpdateDatasetTaskRunnerHarness::alias("bar"),
            vec![foo.dataset_handle.as_local_ref()],
        )
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
    let (harness, foo, synced_head) = UpdateDatasetTaskRunnerHarness::new_syncing_new_root(
        &UpdateDatasetTaskRunnerHarness::alias("foo"),
    )
    .await;

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
    let alias_foo = UpdateDatasetTaskRunnerHarness::alias("foo");
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

#[test]
fn test_sync_into_missing_dataset_is_not_an_update_task_job() {
    let alias_bar = UpdateDatasetTaskRunnerHarness::alias("bar");

    assert_matches!(
        UpdateDatasetTaskRunnerHarness::sync_job_into_new(&alias_bar),
        Err(UpdateTargetMissingError { alias }) if alias == alias_bar
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_up_to_date() {
    let alias_foo = UpdateDatasetTaskRunnerHarness::alias("foo");

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
async fn test_sync_with_real_service_commits_and_reports_increment() {
    let harness = UpdateDatasetTaskRunnerHarness::new_with_real_sync();
    let foo = harness
        .create_root_dataset_with_schema(&UpdateDatasetTaskRunnerHarness::alias("foo"))
        .await;
    harness.push_to_repo(&foo).await;
    let repo_head = harness.append_data_in_repo(&foo).await;

    let outcome = harness.run_sync_update_from_repo(&foo).await;

    assert_eq!(
        TaskOutcome::Success(
            TaskResultDatasetUpdate::Updated {
                old_head: Some(foo.head.clone()),
                new_head: repo_head.clone(),
                has_more: false,
                increment: odf::dataset::MetadataChainIncrementInterval {
                    num_blocks: 1,
                    num_records: 10,
                    updated_watermark: None,
                },
            }
            .into_task_result()
        ),
        outcome,
    );
    assert_eq!(harness.get_head(&foo).await, repo_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_with_real_service_from_repo_in_sync_is_up_to_date() {
    let harness = UpdateDatasetTaskRunnerHarness::new_with_real_sync();
    let foo = harness
        .create_root_dataset_with_schema(&UpdateDatasetTaskRunnerHarness::alias("foo"))
        .await;
    harness.push_to_repo(&foo).await;

    let outcome = harness.run_sync_update_from_repo(&foo).await;

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
    let (harness, foo, _) = UpdateDatasetTaskRunnerHarness::new_syncing_new_root(
        &UpdateDatasetTaskRunnerHarness::alias("foo"),
    )
    .await;
    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
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
async fn test_sync_commit_not_allowed_fails_unrecoverably() {
    let (harness, foo, _) = UpdateDatasetTaskRunnerHarness::new_syncing_new_root(
        &UpdateDatasetTaskRunnerHarness::alias("foo"),
    )
    .await;
    harness.deny_next_commit();

    let outcome = harness.run_sync_update(&foo).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_commit_not_allowed_fails_unrecoverably() {
    let (harness, foo, _) = UpdateDatasetTaskRunnerHarness::new_ingesting_new_root(
        &UpdateDatasetTaskRunnerHarness::alias("foo"),
    )
    .await;
    harness.deny_next_commit();

    let outcome = harness.run_ingest_update(&foo).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_head_moved_without_heads_fails_recoverably() {
    let outcome = UpdateDatasetTaskRunnerHarness::run_sync_failing_with(|| {
        SyncError::HeadMoved(SyncHeadMovedError::unreported())
    })
    .await;

    let TaskOutcome::Failed(task_error) = &outcome else {
        panic!("The sync must fail, got {outcome:?}");
    };
    assert_matches!(
        TaskErrorDatasetCommit::from_task_error(task_error).unwrap(),
        TaskErrorDatasetCommit::HeadMoved(DatasetHeadMovedError {
            expected_head: None,
            actual_head: None,
            ..
        })
    );
    assert!(outcome.is_recoverable_failure());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_transfer_failure_fails_as_recoverable_as_its_error() {
    /// Case name, the failure, and whether the task may be retried
    type Case = (&'static str, fn() -> SyncError, bool);
    let cases: [Case; 2] = [
        (
            "diverged_source",
            || {
                SyncError::DatasetsDiverged(DatasetsDivergedError {
                    src_head: odf::Multihash::from_digest_sha3_256(b"src"),
                    dst_head: odf::Multihash::from_digest_sha3_256(b"dst"),
                    detail: None,
                })
            },
            false,
        ),
        (
            "unreachable_source",
            || SyncError::Internal("connection refused".int_err()),
            true,
        ),
    ];

    for (case, make_error, recoverable) in cases {
        let outcome = UpdateDatasetTaskRunnerHarness::run_sync_failing_with(make_error).await;

        assert_eq!(
            TaskOutcome::Failed(UpdateDatasetTaskRunnerHarness::empty_task_error(
                recoverable
            )),
            outcome,
            "{case}",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_of_deleted_dataset_fails_unrecoverably() {
    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks::default());
    let foo = harness
        .create_root_dataset(&UpdateDatasetTaskRunnerHarness::alias("foo"))
        .await;
    harness.delete_dataset(&foo).await;

    let outcome = harness.run_ingest_update(&foo).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_of_deleted_dataset_fails_unrecoverably() {
    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks::default());
    let bar = harness
        .create_derivative(&UpdateDatasetTaskRunnerHarness::alias("bar"))
        .await;
    harness.delete_dataset(&bar).await;

    let outcome = harness.run_transform_update(&bar).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_into_deleted_dataset_fails_unrecoverably() {
    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks::default());
    let foo = harness
        .create_root_dataset(&UpdateDatasetTaskRunnerHarness::alias("foo"))
        .await;
    harness.delete_dataset(&foo).await;

    let outcome = harness.run_sync_update(&foo).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_passes_resolved_env_vars_to_the_ingest() {
    let alias_foo = UpdateDatasetTaskRunnerHarness::alias("foo");

    let harness = UpdateDatasetTaskRunnerHarness::new_ingesting_expecting_env_var(
        &alias_foo,
        ConstDatasetEnvVarResolver::KEY,
    );
    let foo = harness.create_root_dataset(&alias_foo).await;

    let outcome = harness.run_ingest_update(&foo).await;

    assert_matches!(outcome, TaskOutcome::Success(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_planning_failure_fails_recoverably() {
    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
        env_var_resolution: Some(EnvVarResolution::Failing),
        ..UpdateDatasetTaskRunnerMocks::default()
    });
    let foo = harness
        .create_root_dataset(&UpdateDatasetTaskRunnerHarness::alias("foo"))
        .await;

    let outcome = harness.run_ingest_update(&foo).await;

    assert_eq!(TaskOutcome::Failed(TaskError::empty_recoverable()), outcome);
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_of_dataset_deleted_before_commit_fails_unrecoverably() {
    let (harness, foo, _) = UpdateDatasetTaskRunnerHarness::new_ingesting_new_root(
        &UpdateDatasetTaskRunnerHarness::alias("foo"),
    )
    .await;
    harness.delete_dataset_before_next_commit();

    let outcome = harness.run_ingest_update(&foo).await;

    assert_eq!(
        TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        outcome
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct UpdateDatasetTaskRunnerHarness {
    base_use_case_harness: BaseUseCaseHarness,
    catalog: dill::Catalog,
    runner: Arc<dyn TaskRunner>,
    racing_committer: Arc<RacingDatasetHeadCommitter>,
    racing_update_steps: Arc<RacingDatasetUpdateSteps>,
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

            match mocks.env_var_resolution {
                None => {}
                Some(EnvVarResolution::Const) => {
                    b.add_value(SecretsEncryptionConfig::sample())
                        .add::<ConstDatasetEnvVarResolver>();
                }
                Some(EnvVarResolution::Failing) => {
                    b.add_value(SecretsEncryptionConfig::sample())
                        .add::<FailingDatasetEnvVarResolver>();
                }
            }
        })
    }

    /// Expects one up-to-date ingest of `alias` that receives `env_var_key`,
    /// resolved by [`ConstDatasetEnvVarResolver`]
    fn new_ingesting_expecting_env_var(
        alias: &odf::DatasetAlias,
        env_var_key: &'static str,
    ) -> Self {
        let mut mock_polling_ingest_service = MockPollingIngestService::new();
        let alias = alias.clone();
        mock_polling_ingest_service
            .expect_ingest()
            .withf(move |target, _, _, env_vars, _, _| {
                target.get_alias() == &alias && env_vars.as_map().contains_key(env_var_key)
            })
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(PollingIngestResult::UpToDate { uncacheable: false }));

        Self::new(UpdateDatasetTaskRunnerMocks {
            mock_polling_ingest_service,
            env_var_resolution: Some(EnvVarResolution::Const),
            ..UpdateDatasetTaskRunnerMocks::default()
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

    /// Syncs with the real service from a `file://` repository in the
    /// harness's directory, with the default ingest and transform mocks
    fn new_with_real_sync() -> Self {
        Self::build(|b, temp_dir| {
            std::fs::create_dir(Self::repo_dir(temp_dir)).unwrap();

            b.add::<SyncServiceImpl>()
                .add::<SyncRequestBuilder>()
                .add::<SimpleTransferProtocol>()
                .add::<DummySmartTransferProtocolClient>()
                .add::<IpfsServiceImpl>()
                .add_value(IpfsClient::default())
                .add_value(odf::dataset::IpfsGateway::default())
                .add::<odf::dataset::DatasetFactoryImpl>()
                .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
                .add::<RemoteDatasetUrlResolverImpl>()
                .add::<RemoteRepositoryRegistryImpl>()
                .add_value(RemoteReposDir::new_with_creation(temp_dir.join("repos")))
                .add_value(MockPollingIngestService::new())
                .bind::<dyn PollingIngestService, MockPollingIngestService>()
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
        RacingDatasetUpdateSteps::register(&mut b);
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
            racing_update_steps: catalog.get_one().unwrap(),
            catalog,
        }
    }

    fn repo_dir(temp_dir: &Path) -> PathBuf {
        temp_dir.join("file_repo")
    }

    /// The copy of `alias` in the harness's `file://` repository
    fn repo_dataset_url(&self, alias: &odf::DatasetAlias) -> Url {
        Url::from_directory_path(Self::repo_dir(self.temp_dir_path()))
            .unwrap()
            .join(&format!("{}/", alias.dataset_name))
            .unwrap()
    }

    async fn create_root_dataset_with_schema(
        &self,
        alias: &odf::DatasetAlias,
    ) -> CreateDatasetResult {
        self.create_root_dataset_with_events(
            alias,
            vec![MetadataFactory::set_data_schema().build().into()],
        )
        .await
    }

    async fn push_to_repo(&self, created: &CreateDatasetResult) {
        let request = self
            .catalog
            .get_one::<SyncRequestBuilder>()
            .unwrap()
            .build_sync_request(
                created.dataset_handle.as_local_ref(),
                odf::DatasetRefRemote::from(&self.repo_dataset_url(&created.dataset_handle.alias)),
                true,
            )
            .await
            .unwrap();

        self.catalog
            .get_one::<dyn SyncService>()
            .unwrap()
            .sync(request, SyncOptions::default(), None)
            .await
            .unwrap();
    }

    /// Puts the repository copy one data block ahead and returns its head
    async fn append_data_in_repo(&self, created: &CreateDatasetResult) -> odf::Multihash {
        let repo_dataset = self
            .catalog
            .get_one::<dyn odf::dataset::DatasetFactory>()
            .unwrap()
            .get_dataset(&self.repo_dataset_url(&created.dataset_handle.alias), false)
            .await
            .unwrap();

        DatasetTestHelper::append_random_data_to(repo_dataset.as_ref(), 32).await
    }

    /// Expects one sync of `alias` from its remote: up to date without
    /// `heads`, otherwise updated to their new head
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
        let alias_foo = Self::alias("foo");
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
        let alias_bar = Self::alias("bar");
        let harness = Self::new(UpdateDatasetTaskRunnerMocks {
            mock_transform_planner: MockTransformPlanner::new()
                .make_expect_elaborate_transform(alias_bar.clone()),
            mock_transform_executor: MockTransformExecutor::new()
                .make_expect_transform_error(alias_bar.clone(), make_error),
            ..UpdateDatasetTaskRunnerMocks::default()
        });
        let bar = harness.create_derivative(&alias_bar).await;

        harness.run_transform_update(&bar).await
    }

    /// Runs the transform of a fresh derivative whose elaboration fails with
    /// `make_error(target)`
    async fn run_transform_elaboration_failing_with(
        make_error: impl Fn(&odf::DatasetHandle) -> TransformElaborateError + Send + Sync + 'static,
    ) -> TaskOutcome {
        let alias_bar = Self::alias("bar");
        let mut mock_transform_planner = MockTransformPlanner::new();
        mock_transform_planner
            .expect_elaborate_transform()
            .times(1)
            .returning(move |target| Err(make_error(target.get_handle())));
        let harness = Self::new(UpdateDatasetTaskRunnerMocks {
            mock_transform_planner,
            ..UpdateDatasetTaskRunnerMocks::default()
        });
        let bar = harness.create_derivative(&alias_bar).await;

        harness.run_transform_update(&bar).await
    }

    fn alias(name: &str) -> odf::DatasetAlias {
        odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(name))
    }

    /// The error an update task fails with when its operation fails with an
    /// error of this recoverability
    fn empty_task_error(recoverable: bool) -> TaskError {
        if recoverable {
            TaskError::empty_recoverable()
        } else {
            TaskError::empty_unrecoverable()
        }
    }

    /// Creates root `alias` and expects one ingest of it reporting a new head,
    /// appended unreferenced; returns the dataset and that head
    async fn new_ingesting_new_root(
        alias: &odf::DatasetAlias,
    ) -> (Self, CreateDatasetResult, odf::Multihash) {
        let reported_heads = ReportedHeads::default();
        let harness = Self::new(UpdateDatasetTaskRunnerMocks {
            mock_polling_ingest_service: MockPollingIngestService::new()
                .make_expect_ingest_updated(alias.clone(), reported_heads.clone()),
            ..UpdateDatasetTaskRunnerMocks::default()
        });
        let created = harness.create_root_dataset(alias).await;
        let ingested_head = harness
            .append_unreferenced_block(&created, "ingested")
            .await;
        reported_heads.set(&created.head, &ingested_head);
        (harness, created, ingested_head)
    }

    /// Creates root `alias` and expects one sync of it from its remote
    /// reporting a new head, appended unreferenced; returns the dataset and
    /// that head
    async fn new_syncing_new_root(
        alias: &odf::DatasetAlias,
    ) -> (Self, CreateDatasetResult, odf::Multihash) {
        let synced_heads = ReportedHeads::default();
        let harness = Self::new_syncing(alias, Some(&synced_heads));
        let created = harness.create_root_dataset(alias).await;
        let synced_head = harness.append_unreferenced_block(&created, "synced").await;
        synced_heads.set(&created.head, &synced_head);
        (harness, created, synced_head)
    }

    /// Creates derivative `alias` of a new root dataset `input`
    async fn create_derivative(&self, alias: &odf::DatasetAlias) -> CreateDatasetResult {
        let input = self.create_root_dataset(&Self::alias("input")).await;
        self.create_derived_dataset(alias, vec![input.dataset_handle.as_local_ref()])
            .await
    }

    fn move_head_before_next_commit(&self, concurrent_head: odf::Multihash) {
        self.racing_committer
            .move_head_before_next_commit(&self.catalog, concurrent_head);
    }

    fn deny_next_commit(&self) {
        self.racing_committer.deny_next_commit();
    }

    fn delete_dataset_before_next_commit(&self) {
        self.racing_update_steps.delete_dataset_before_next_commit();
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
            PollingIngestOptions {
                fetch_uncacheable: true,
                ..PollingIngestOptions::default()
            },
        )
        .await
    }

    async fn run_transform_update(&self, created: &CreateDatasetResult) -> TaskOutcome {
        self.run_update(DatasetUpdateJob::Transform(created.dataset_handle.clone()))
            .await
    }

    /// Syncs `created` from its remote; the mocked transfer ignores the
    /// source
    async fn run_sync_update(&self, created: &CreateDatasetResult) -> TaskOutcome {
        self.run_update(DatasetUpdateJob::Sync {
            target: created.dataset_handle.clone(),
            remote_ref: Self::sync_remote_ref(&created.dataset_handle.alias),
        })
        .await
    }

    /// Syncs `created` from its copy in the harness's repository
    async fn run_sync_update_from_repo(&self, created: &CreateDatasetResult) -> TaskOutcome {
        self.run_update(DatasetUpdateJob::Sync {
            target: created.dataset_handle.clone(),
            remote_ref: odf::DatasetRefRemote::from(
                &self.repo_dataset_url(&created.dataset_handle.alias),
            ),
        })
        .await
    }

    /// Converts a planned sync whose target `alias` does not exist yet into an
    /// update task job
    fn sync_job_into_new(
        alias: &odf::DatasetAlias,
    ) -> Result<DatasetUpdateJob, UpdateTargetMissingError> {
        DatasetUpdateJob::try_from(PullPlanIterationJob::Sync(PullSyncItem {
            local_target: PullLocalTarget::to_create(alias.clone()),
            remote_ref: Self::sync_remote_ref(alias),
            maybe_original_request: None,
        }))
    }

    async fn run_update(&self, job: DatasetUpdateJob) -> TaskOutcome {
        self.run_update_with(job, PollingIngestOptions::default())
            .await
    }

    async fn run_update_with(
        &self,
        job: DatasetUpdateJob,
        ingest_options: PollingIngestOptions,
    ) -> TaskOutcome {
        self.runner
            .run_task(TaskDefinition::new(TaskDefinitionDatasetUpdate {
                ingest_options,
                sync_options: SyncOptions::default(),
                job,
            }))
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct UpdateDatasetTaskRunnerMocks {
    mock_sync_service: MockSyncService,
    mock_polling_ingest_service: MockPollingIngestService,
    mock_transform_planner: MockTransformPlanner,
    mock_transform_executor: MockTransformExecutor,
    env_var_resolution: Option<EnvVarResolution>,
}

/// The dataset env-var resolver registered with secrets enabled, if any
enum EnvVarResolution {
    Const,
    Failing,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves one plain variable for every dataset
#[dill::component]
#[dill::interface(dyn DatasetEnvVarResolver)]
struct ConstDatasetEnvVarResolver {}

impl ConstDatasetEnvVarResolver {
    const KEY: &'static str = "API_KEY";

    fn env_var(dataset_id: &odf::DatasetID) -> DatasetEnvVar {
        DatasetEnvVar::new(
            Self::KEY,
            Utc::now(),
            &DatasetEnvVarValue::Regular("value".to_string()),
            dataset_id,
            "",
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
impl DatasetEnvVarResolver for ConstDatasetEnvVarResolver {
    async fn resolve_effective_env_vars(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashMap<String, DatasetEnvVar>, InternalError> {
        Ok(HashMap::from([(
            Self::KEY.to_string(),
            Self::env_var(dataset_id),
        )]))
    }

    async fn get_env_var_by_entry_key(
        &self,
        dataset_id: &odf::DatasetID,
        _entry_key: &str,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        Ok(Self::env_var(dataset_id))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Fails to resolve, as when the dataset's secrets cannot be decrypted
#[dill::component]
#[dill::interface(dyn DatasetEnvVarResolver)]
struct FailingDatasetEnvVarResolver {}

#[async_trait::async_trait]
impl DatasetEnvVarResolver for FailingDatasetEnvVarResolver {
    async fn resolve_effective_env_vars(
        &self,
        _dataset_id: &odf::DatasetID,
    ) -> Result<HashMap<String, DatasetEnvVar>, InternalError> {
        Err("Cannot decrypt the dataset's secrets".int_err())
    }

    async fn get_env_var_by_entry_key(
        &self,
        _dataset_id: &odf::DatasetID,
        _entry_key: &str,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        Err("Cannot decrypt the dataset's secrets".int_err().into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
