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
use kamu::testing::{BaseRepoHarness, BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::{
    DataFormatRegistryImpl,
    EngineConfigDatafusionEmbeddedIngest,
    EngineProvisionerNull,
    FetchService,
    IngestEnvVarsResolver,
    ObjectStoreBuilderLocalFs,
    ObjectStoreRegistryImpl,
    PollingIngestJobImpl,
    PollingIngestPlannerImpl,
    PollingIngestServiceImpl,
    SyncFromJobImpl,
    TransformExecutorImpl,
    TransformJobImpl,
    TransformPlannerImpl,
};
use kamu_adapter_task_dataset::{
    DatasetRefMovedError,
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
    DatasetReferenceUpdater,
    GetDatasetEnvVarError,
    SecretsEncryptionConfig,
};
use kamu_datasets_services::DatasetKeyValueServiceSysEnv;
use kamu_datasets_services::testing::RacingDatasetReferenceUpdater;
use kamu_task_system::{TaskDefinition, TaskError, TaskOutcome, TaskRunner};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_reports_increment_from_its_result() {
    let alias_foo = BaseRepoHarness::alias("foo");
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
    let alias_foo = BaseRepoHarness::alias("foo");

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
    let alias_foo = BaseRepoHarness::alias("foo");

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
        .create_root_dataset_without_source(&BaseRepoHarness::alias("foo"))
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
    let alias_foo = BaseRepoHarness::alias("foo");

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
async fn test_ingest_with_real_service_of_unreadable_data_fails_unrecoverably() {
    let harness = UpdateDatasetTaskRunnerHarness::new_with_real_ingest();
    let src_path = harness.write_source_file("city,population\nA,not-a-number\n");
    let foo = harness
        .create_root_dataset_polling_csv(&BaseRepoHarness::alias("foo"), &src_path)
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
async fn test_transform_with_nothing_to_do_succeeds_as_up_to_date() {
    /// Case name and the planner's expectations for the derivative
    type Case = (&'static str, fn(odf::DatasetAlias) -> MockTransformPlanner);
    let cases: [Case; 2] = [
        ("input_not_pulled", |alias| {
            MockTransformPlanner::new().make_expect_elaborate_transform_error(alias, || {
                InputNotPulledError {
                    dataset_handle: odf::DatasetHandle::new(
                        odf::DatasetID::new_seeded_ed25519(b"foo"),
                        BaseRepoHarness::alias("foo"),
                        odf::DatasetKind::Root,
                    ),
                }
                .into()
            })
        }),
        ("up_to_date", |alias| {
            MockTransformPlanner::new().make_expect_elaborate_transform_up_to_date(alias)
        }),
    ];

    for (case, make_mock_transform_planner) in cases {
        let alias_bar = BaseRepoHarness::alias("bar");
        let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
            mock_transform_planner: make_mock_transform_planner(alias_bar.clone()),
            ..UpdateDatasetTaskRunnerMocks::default()
        });
        let bar = harness.create_derivative(&alias_bar).await;

        let outcome = harness.run_transform_update(&bar).await;

        assert_eq!(
            TaskOutcome::Success(
                TaskResultDatasetUpdate::UpToDate(PullResultUpToDate::Transform).into_task_result()
            ),
            outcome,
            "{case}",
        );
        assert_eq!(harness.get_head(&bar).await, bar.head, "{case}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_diverged_input_fails_as_input_compacted() {
    let alias_bar = BaseRepoHarness::alias("bar");

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
    let alias_bar = BaseRepoHarness::alias("bar");
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
async fn test_update_ref_moved_concurrently_fails_recoverably() {
    for kind in [UpdateKind::Ingest, UpdateKind::Transform, UpdateKind::Sync] {
        let (harness, target, _) =
            UpdateDatasetTaskRunnerHarness::new_updating_new_target(kind).await;
        let concurrent_head = harness
            .append_unreferenced_block(&target, "concurrent")
            .await;
        harness.move_reference_before_next_update(concurrent_head.clone());

        let outcome = harness.run_update_of(kind, &target).await;

        assert_eq!(
            TaskOutcome::Failed(
                TaskErrorDatasetCommit::RefMoved(DatasetRefMovedError {
                    dataset_id: target.dataset_handle.id.clone(),
                    block_ref: odf::BlockRef::Head,
                    expected_block_hash: Some(target.head.clone()),
                    actual_block_hash: Some(concurrent_head.clone()),
                })
                .into_task_error()
            ),
            outcome,
            "{kind:?}",
        );
        assert!(outcome.is_recoverable_failure(), "{kind:?}");
        assert_eq!(harness.get_head(&target).await, concurrent_head, "{kind:?}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_commit_not_allowed_fails_unrecoverably() {
    for kind in [UpdateKind::Ingest, UpdateKind::Transform, UpdateKind::Sync] {
        let (harness, target, _) =
            UpdateDatasetTaskRunnerHarness::new_updating_new_target(kind).await;
        harness.deny_next_update();

        let outcome = harness.run_update_of(kind, &target).await;

        assert_eq!(
            TaskOutcome::Failed(TaskError::empty_unrecoverable()),
            outcome,
            "{kind:?}",
        );
        assert_eq!(harness.get_head(&target).await, target.head, "{kind:?}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_transform_of_dataset_deleted_during_execution_fails_unrecoverably() {
    let (harness, bar) =
        UpdateDatasetTaskRunnerHarness::new_transforming_derivative_deleted_meanwhile(
            &BaseRepoHarness::alias("bar"),
        )
        .await;

    let outcome = harness.run_transform_update(&bar).await;

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
        .create_root_dataset(&BaseRepoHarness::alias("foo"))
        .await;
    harness
        .append_schema_and_watermark(&foo, Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap())
        .await;
    let bar = harness
        .create_derived_dataset(
            &BaseRepoHarness::alias("bar"),
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
    let (harness, foo, synced_head) =
        UpdateDatasetTaskRunnerHarness::new_syncing_new_root(&BaseRepoHarness::alias("foo")).await;

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
    let alias_foo = BaseRepoHarness::alias("foo");
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
    let alias_bar = BaseRepoHarness::alias("bar");

    assert_matches!(
        UpdateDatasetTaskRunnerHarness::sync_job_into_new(&alias_bar),
        Err(UpdateTargetMissingError { alias }) if alias == alias_bar
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sync_up_to_date() {
    let alias_foo = BaseRepoHarness::alias("foo");

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
async fn test_sync_ref_moved_without_block_hashes_fails_recoverably() {
    let outcome = UpdateDatasetTaskRunnerHarness::run_sync_failing_with(|| {
        SyncError::RefMoved(SyncRefMovedError::unreported(odf::BlockRef::Head))
    })
    .await;

    let TaskOutcome::Failed(task_error) = &outcome else {
        panic!("The sync must fail, got {outcome:?}");
    };
    assert_matches!(
        TaskErrorDatasetCommit::from_task_error(task_error).unwrap(),
        TaskErrorDatasetCommit::RefMoved(DatasetRefMovedError {
            block_ref: odf::BlockRef::Head,
            expected_block_hash: None,
            actual_block_hash: None,
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
async fn test_update_of_deleted_dataset_fails_unrecoverably() {
    for kind in [UpdateKind::Ingest, UpdateKind::Transform, UpdateKind::Sync] {
        let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks::default());
        let target = harness.create_target(kind).await;
        harness.delete_dataset(&target).await;

        let outcome = harness.run_update_of(kind, &target).await;

        assert_eq!(
            TaskOutcome::Failed(TaskError::empty_unrecoverable()),
            outcome,
            "{kind:?}",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_ingest_planning_failure_fails_recoverably() {
    let harness = UpdateDatasetTaskRunnerHarness::new(UpdateDatasetTaskRunnerMocks {
        env_var_resolution: Some(EnvVarResolution::Failing),
        ..UpdateDatasetTaskRunnerMocks::default()
    });
    let foo = harness
        .create_root_dataset(&BaseRepoHarness::alias("foo"))
        .await;

    let outcome = harness.run_ingest_update(&foo).await;

    assert_eq!(TaskOutcome::Failed(TaskError::empty_recoverable()), outcome);
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct UpdateDatasetTaskRunnerHarness {
    base_use_case_harness: BaseUseCaseHarness,
    catalog: dill::Catalog,
    runner: Arc<dyn TaskRunner>,
    racing_reference_updater: Arc<RacingDatasetReferenceUpdater>,
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
                Some(EnvVarResolution::Failing) => {
                    b.add_value(SecretsEncryptionConfig::sample())
                        .add::<FailingDatasetEnvVarResolver>();
                }
            }
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

    /// `register_operations` registers the sync, ingest and transform
    /// services; it gets the harness's temporary directory. Tasks run as the
    /// node, which may run every operation
    fn build(register_operations: impl FnOnce(&mut dill::CatalogBuilder, &Path)) -> Self {
        let base_use_case_harness =
            BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new().as_task_node());

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<UpdateDatasetTaskRunner>()
            .add::<PollingIngestJobImpl>()
            .add::<TransformJobImpl>()
            .add::<SyncFromJobImpl>()
            .add::<PollingIngestPlannerImpl>()
            .add::<IngestEnvVarsResolver>()
            .add_value(RacingDatasetReferenceUpdater::default())
            .bind::<dyn DatasetReferenceUpdater, RacingDatasetReferenceUpdater>();
        register_operations(&mut b, base_use_case_harness.temp_dir_path());

        let catalog = b.build();

        Self {
            base_use_case_harness,
            runner: catalog.get_one::<UpdateDatasetTaskRunner>().unwrap(),
            racing_reference_updater: catalog.get_one().unwrap(),
            catalog,
        }
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

    /// Creates derivative `alias` and expects one transform of it reporting a
    /// new head, appended unreferenced; returns the dataset and that head
    async fn new_transforming_new_derivative(
        alias: &odf::DatasetAlias,
    ) -> (Self, CreateDatasetResult, odf::Multihash) {
        let reported_heads = ReportedHeads::default();
        let harness = Self::new_transforming(alias, &reported_heads);
        let created = harness.create_derivative(alias).await;
        let transformed_head = harness
            .append_unreferenced_block(&created, "transformed")
            .await;
        reported_heads.set(&created.head, &transformed_head);
        (harness, created, transformed_head)
    }

    /// Creates derivative `alias` and expects one transform of it reporting a
    /// new head, during which another writer deletes the dataset
    async fn new_transforming_derivative_deleted_meanwhile(
        alias: &odf::DatasetAlias,
    ) -> (Self, CreateDatasetResult) {
        let reported_heads = ReportedHeads::default();
        let harness = Self::new(UpdateDatasetTaskRunnerMocks {
            mock_transform_planner: MockTransformPlanner::new()
                .make_expect_elaborate_transform(alias.clone()),
            mock_transform_executor: Self::transform_deleting_target(alias, &reported_heads),
            ..UpdateDatasetTaskRunnerMocks::default()
        });
        let created = harness.create_derivative(alias).await;
        let transformed_head = harness
            .append_unreferenced_block(&created, "transformed")
            .await;
        reported_heads.set(&created.head, &transformed_head);
        (harness, created)
    }

    /// Expects one transform of `alias` that deletes the dataset's storage
    /// before it reports `heads`, as another writer deleting the dataset
    /// would
    fn transform_deleting_target(
        alias: &odf::DatasetAlias,
        heads: &ReportedHeads,
    ) -> MockTransformExecutor {
        let alias = alias.clone();
        let heads = heads.clone();
        let mut mock_transform_executor = MockTransformExecutor::new();
        mock_transform_executor
            .expect_execute_transform()
            .withf(move |target, _, _, _| target.get_alias() == &alias)
            .times(1)
            .returning(move |target, _, _, _| {
                std::fs::remove_dir_all(target.get_storage_internal_url().to_file_path().unwrap())
                    .unwrap();
                let (old_head, new_head) = heads.get();
                (
                    target,
                    Ok(TransformResult::Updated {
                        old_head,
                        new_head,
                        new_offset_interval: None,
                        increment: MockTransformExecutor::REPORTED_INCREMENT,
                    }),
                )
            });
        mock_transform_executor
    }

    /// Runs the sync of a fresh dataset whose transfer fails with
    /// `make_error()`
    async fn run_sync_failing_with(
        make_error: impl Fn() -> SyncError + Send + 'static,
    ) -> TaskOutcome {
        let alias_foo = BaseRepoHarness::alias("foo");
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
        let alias_bar = BaseRepoHarness::alias("bar");
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
        let alias_bar = BaseRepoHarness::alias("bar");
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

    /// Creates a dataset that an update of `kind` applies to, as
    /// [`Self::create_target`] does, and expects one update of it reporting a
    /// new head, appended unreferenced; returns the dataset and that head
    async fn new_updating_new_target(
        kind: UpdateKind,
    ) -> (Self, CreateDatasetResult, odf::Multihash) {
        match kind {
            UpdateKind::Ingest => {
                Self::new_ingesting_new_root(&BaseRepoHarness::alias("foo")).await
            }
            UpdateKind::Transform => {
                Self::new_transforming_new_derivative(&BaseRepoHarness::alias("bar")).await
            }
            UpdateKind::Sync => Self::new_syncing_new_root(&BaseRepoHarness::alias("foo")).await,
        }
    }

    /// Creates derivative `alias` of a new root dataset `input`
    async fn create_derivative(&self, alias: &odf::DatasetAlias) -> CreateDatasetResult {
        let input = self
            .create_root_dataset(&BaseRepoHarness::alias("input"))
            .await;
        self.create_derived_dataset(alias, vec![input.dataset_handle.as_local_ref()])
            .await
    }

    fn move_reference_before_next_update(&self, concurrent_head: odf::Multihash) {
        self.racing_reference_updater
            .move_reference_before_next_update(&self.catalog, concurrent_head);
    }

    fn deny_next_update(&self) {
        self.racing_reference_updater.deny_next_update();
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

    /// Creates a dataset that an update of `kind` applies to: root `foo`, or
    /// derivative `bar` for a transform
    async fn create_target(&self, kind: UpdateKind) -> CreateDatasetResult {
        match kind {
            UpdateKind::Ingest | UpdateKind::Sync => {
                self.create_root_dataset(&BaseRepoHarness::alias("foo"))
                    .await
            }
            UpdateKind::Transform => self.create_derivative(&BaseRepoHarness::alias("bar")).await,
        }
    }

    async fn run_update_of(&self, kind: UpdateKind, created: &CreateDatasetResult) -> TaskOutcome {
        match kind {
            UpdateKind::Ingest => self.run_ingest_update(created).await,
            UpdateKind::Transform => self.run_transform_update(created).await,
            UpdateKind::Sync => self.run_sync_update(created).await,
        }
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
    Failing,
}

/// The operation an update task runs
#[derive(Debug, Clone, Copy)]
enum UpdateKind {
    Ingest,
    Transform,
    Sync,
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
