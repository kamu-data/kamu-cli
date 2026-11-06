// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use container_runtime::{ContainerRuntime, ContainerRuntimeConfig};
use internal_error::{ErrorIntoInternal, InternalError};
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::*;
use kamu_core::engine::TransformResponseExt;
use kamu_core::testing::{MockIngestIteration, ReportedHeads};
use kamu_core::*;
use kamu_datasets::{
    ClassifyByAllowanceDatasetPermissionError,
    ClassifyByAllowanceResponse,
    CreateDatasetResult,
    DatasetEnvVar,
    DatasetEnvVarResolver,
    DatasetEnvVarValue,
    DatasetHeadCommitter,
    DatasetPermission,
    DatasetPermissionError,
    GetDatasetEnvVarError,
    MockDatasetPermissionChecker,
    ResolvedDataset,
    SecretsEncryptionConfig,
};
use kamu_datasets_services::{
    DatasetHeadCommitterImpl,
    DatasetKeyValueServiceSysEnv,
    DependencyGraphIndexer,
    DependencyGraphServiceImpl,
};
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::dataset::{DatasetFactoryImpl, IpfsGateway};
use odf::metadata::AsTypedBlock as _;
use odf::metadata::testing::MetadataFactory;
use tempfile::TempDir;
use url::Url;

use crate::utils::RecordingListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_up_to_date_keeps_pull_aliases() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let mocks = PullUseCaseHarnessMocks::default()
        .with_permission_checker_mock(
            MockDatasetPermissionChecker::new().make_expect_classify_datasets_by_allowance(
                DatasetPermission::Write,
                1,
                HashSet::from_iter([alias_foo.clone()]),
            ),
        )
        .with_polling_ingest_mock(
            MockPollingIngestService::new().make_expect_ingest(alias_foo.clone()),
        );

    let harness = PullUseCaseHarness::new(mocks);
    let foo = harness.create_root_dataset(&alias_foo).await;

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));

    let pull_request = PullRequest::Local(alias_foo.as_local_ref());

    let pull_response = harness
        .pull(pull_request.clone(), PullOptions::default())
        .await;

    assert_matches!(
        pull_response,
        PullResponse {
            maybe_original_request: Some(a_pull_request),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(_)),
        } if a_pull_request == pull_request &&
        a_local_ref == foo.dataset_handle.as_local_ref()
    );

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_up_to_date_keeps_pull_aliases() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let mocks = PullUseCaseHarnessMocks::default()
        .with_permission_checker_mock(
            MockDatasetPermissionChecker::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    1,
                    HashSet::from_iter([alias_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Read,
                    1,
                    HashSet::from_iter([alias_foo.clone()]),
                ),
        )
        .with_transform_elaboration_mock(
            MockTransformPlanner::new().make_expect_elaborate_transform(alias_bar.clone()),
        )
        .with_transform_execution_mock(
            MockTransformExecutor::new().make_expect_transform(alias_bar.clone()),
        );

    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let aliases = harness.get_remote_aliases(&bar).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));

    let pull_request = PullRequest::Local(alias_bar.as_local_ref());

    let pull_response = harness
        .pull(pull_request.clone(), PullOptions::default())
        .await;

    assert_matches!(
        pull_response,
        PullResponse {
            maybe_original_request: Some(a_pull_request),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(_)),
        } if a_pull_request == pull_request &&
        a_local_ref == bar.dataset_handle.as_local_ref()
    );

    let aliases = harness.get_remote_aliases(&bar).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_rechecks_inputs_changed_after_planning() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let baz_handle = odf::DatasetHandle::new(
        odf::DatasetID::new_seeded_ed25519(b"baz"),
        alias_baz.clone(),
        odf::DatasetKind::Root,
    );

    // The pull authorizes `foo`, but elaboration finds a transform reading `baz`.
    // The recheck authorizes the transform again, its target included, and
    // reports the unreadable `baz` as not found.
    let mocks = PullUseCaseHarnessMocks::default()
        .with_permission_checker_mock(
            MockDatasetPermissionChecker::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    2,
                    HashSet::from_iter([alias_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Read,
                    2,
                    HashSet::from_iter([alias_foo.clone()]),
                ),
        )
        .with_transform_elaboration_mock(
            MockTransformPlanner::new().make_expect_elaborate_transform_reading(
                alias_bar.clone(),
                vec![baz_handle.clone()],
            ),
        );

    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let (pull_response, events) = harness.pull_local_recording(&alias_bar).await;

    assert_matches!(
        pull_response,
        PullResponse {
            maybe_local_ref: Some(a_local_ref),
            result: Err(PullError::NotFound(e)),
            ..
        } if a_local_ref == bar.dataset_handle.as_local_ref()
            && e.dataset_ref == baz_handle.as_local_ref()
    );
    assert_eq!(events, vec!["error"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_with_real_planner_and_executor_commits_head() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let watermark = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();

    let mocks = PullUseCaseHarnessMocks::default()
        .with_permission_checker_mock(
            MockDatasetPermissionChecker::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    1,
                    HashSet::from_iter([alias_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Read,
                    1,
                    HashSet::from_iter([alias_foo.clone()]),
                ),
        )
        .with_real_transform(watermark);
    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;
    harness
        .append_schema_and_watermark(&foo, Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap())
        .await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let (pull_response, events) = harness.pull_local_recording(&alias_bar).await;

    // The executor reports only `begin`: the outcome is reported once, after the commit
    assert_eq!(events, vec!["begin", "success"]);
    let new_head = harness.get_head(&bar).await;
    assert_ne!(new_head, bar.head);
    assert_matches!(
        pull_response.result,
        Ok(PullResult::Updated {
            old_head: Some(old_head),
            new_head: reported_head,
            has_more: false,
        }) if old_head == bar.head && reported_head == new_head
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_commits_head() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let reported_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_updated(
        &alias_foo,
        &reported_heads,
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let ingested_head = harness.append_unreferenced_block(&foo, "ingested").await;
    reported_heads.set(&foo.head, &ingested_head);

    let pull_response = harness.pull_local(&alias_foo).await;

    assert_matches!(
        pull_response.result,
        Ok(PullResult::Updated { old_head: Some(old_head), new_head, .. })
        if old_head == foo.head && new_head == ingested_head
    );
    assert_eq!(harness.get_head(&foo).await, ingested_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_head_moved_concurrently() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let reported_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_updated(
        &alias_foo,
        &reported_heads,
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let ingested_head = harness.append_unreferenced_block(&foo, "ingested").await;
    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
    reported_heads.set(&foo.head, &ingested_head);
    harness.move_head(&foo, &foo.head, &concurrent_head).await;

    let pull_response = harness.pull_local(&alias_foo).await;

    assert_matches!(
        pull_response.result,
        Err(PullError::HeadMoved(e))
        if e.expected == Some(foo.head.clone()) && e.actual == Some(concurrent_head.clone())
    );
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_reports_success_after_commit() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let reported_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_updated(
        &alias_foo,
        &reported_heads,
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let ingested_head = harness.append_unreferenced_block(&foo, "ingested").await;
    reported_heads.set(&foo.head, &ingested_head);

    let (pull_response, events) = harness.pull_local_recording(&alias_foo).await;

    assert_matches!(pull_response.result, Ok(PullResult::Updated { .. }));
    assert_eq!(events, vec!["success"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_head_moved_reports_error_not_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let reported_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_updated(
        &alias_foo,
        &reported_heads,
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let ingested_head = harness.append_unreferenced_block(&foo, "ingested").await;
    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
    reported_heads.set(&foo.head, &ingested_head);
    harness.move_head(&foo, &foo.head, &concurrent_head).await;

    let (pull_response, events) = harness.pull_local_recording(&alias_foo).await;

    assert_matches!(pull_response.result, Err(PullError::HeadMoved(_)));
    assert_eq!(events, vec!["error"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_exhausts_source() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let heads = [(); 3].map(|()| ReportedHeads::default());

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_iterations(
        &alias_foo,
        vec![
            MockIngestIteration::updated(&heads[0], true, 2),
            MockIngestIteration::updated(&heads[1], true, 3),
            MockIngestIteration::updated(&heads[2], false, 4),
        ],
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let last_head = harness.chain_reported_heads(&foo, &heads).await;

    let pull_response = harness.pull_local_exhausting_sources(&alias_foo).await;

    assert_matches!(
        pull_response.result,
        Ok(PullResult::Updated { old_head: Some(old_head), new_head, has_more: false })
        if old_head == foo.head && new_head == last_head
    );
    assert_eq!(harness.get_head(&foo).await, last_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_without_exhausting_runs_one_iteration() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let heads = [ReportedHeads::default()];

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_iterations(
        &alias_foo,
        vec![MockIngestIteration::updated(&heads[0], true, 2)],
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let ingested_head = harness.chain_reported_heads(&foo, &heads).await;

    let pull_response = harness.pull_local(&alias_foo).await;

    assert_matches!(
        pull_response.result,
        Ok(PullResult::Updated { old_head: Some(old_head), new_head, has_more: true })
        if old_head == foo.head && new_head == ingested_head
    );
    assert_eq!(harness.get_head(&foo).await, ingested_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_failure_after_committed_iterations_reports_progress() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let heads = [(); 2].map(|()| ReportedHeads::default());

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_iterations(
        &alias_foo,
        vec![
            MockIngestIteration::updated(&heads[0], true, 2),
            MockIngestIteration::updated(&heads[1], true, 3),
            MockIngestIteration::Unreachable,
        ],
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let committed_head = harness.chain_reported_heads(&foo, &heads).await;

    let pull_response = harness.pull_local_exhausting_sources(&alias_foo).await;

    let Err(PullError::IngestStopped(stopped)) = pull_response.result else {
        panic!(
            "Expected the ingest to stop, got {:?}",
            pull_response.result
        );
    };
    assert_matches!(
        *stopped,
        IngestStoppedError {
            old_head,
            new_head,
            increment: odf::dataset::MetadataChainIncrementInterval {
                num_blocks: 2,
                num_records: 5,
                ..
            },
            source: PullError::PollingIngestError(PollingIngestError::Unreachable { .. }),
        }
        if old_head == foo.head && new_head == committed_head
    );
    assert_eq!(harness.get_head(&foo).await, committed_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_first_iteration_failure_is_not_progress() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_iterations(
        &alias_foo,
        vec![MockIngestIteration::Unreachable],
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;

    let pull_response = harness.pull_local_exhausting_sources(&alias_foo).await;

    assert_matches!(
        pull_response.result,
        Err(PullError::PollingIngestError(
            PollingIngestError::Unreachable { .. }
        ))
    );
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[tokio::test]
async fn test_pull_ingest_unparsable_source_fails_unrecoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::default()
            .with_permission_checker_mock(
                MockDatasetPermissionChecker::new().make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    1,
                    HashSet::from_iter([alias_foo.clone()]),
                ),
            )
            .with_real_ingest(),
    );
    let foo = harness
        .create_root_dataset_polling_csv_with(&alias_foo, "city,population\nA,many\n")
        .await;

    let (pull_response, events) = harness.pull_local_recording(&alias_foo).await;

    let Err(PullError::PollingIngestError(e)) = &pull_response.result else {
        panic!("Expected an ingest error, got {:?}", pull_response.result);
    };
    assert_matches!(e, PollingIngestError::ExecutionError(_));
    assert!(!e.is_recoverable());
    assert_eq!(events, vec!["begin", "error"]);
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_without_polling_source_is_up_to_date() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_iterations(
        &alias_foo,
        vec![],
    ));
    let foo = harness.create_root_dataset_without_source(&alias_foo).await;

    let (pull_response, events) = harness.pull_local_recording(&alias_foo).await;

    assert_matches!(
        pull_response.result,
        Ok(PullResult::UpToDate(PullResultUpToDate::PollingIngest(_)))
    );
    assert_eq!(events, vec!["no_polling_source"]);
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_head_moved_after_committed_iterations_reports_progress() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let heads = [(); 2].map(|()| ReportedHeads::default());

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_iterations(
        &alias_foo,
        vec![
            MockIngestIteration::updated(&heads[0], true, 2),
            MockIngestIteration::updated(&heads[1], false, 3),
        ],
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let last_head = harness.chain_reported_heads(&foo, &heads).await;
    let (committed_head, _) = heads[1].get();
    // The second iteration expects the head the first one started from
    heads[1].set(&foo.head, &last_head);

    let pull_response = harness.pull_local_exhausting_sources(&alias_foo).await;

    let Err(PullError::IngestStopped(stopped)) = pull_response.result else {
        panic!(
            "Expected the ingest to stop, got {:?}",
            pull_response.result
        );
    };
    assert_matches!(
        *stopped,
        IngestStoppedError {
            old_head,
            new_head,
            increment: odf::dataset::MetadataChainIncrementInterval { num_records: 2, .. },
            source: PullError::HeadMoved(_),
        }
        if old_head == foo.head && new_head == committed_head
    );
    assert_eq!(harness.get_head(&foo).await, committed_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_more_data_then_nothing_new_is_updated() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let heads = [ReportedHeads::default()];

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_iterations(
        &alias_foo,
        vec![
            MockIngestIteration::updated(&heads[0], true, 2),
            MockIngestIteration::UpToDate,
        ],
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let last_head = harness.chain_reported_heads(&foo, &heads).await;

    let pull_response = harness.pull_local_exhausting_sources(&alias_foo).await;

    assert_matches!(
        pull_response.result,
        Ok(PullResult::Updated { old_head: Some(old_head), new_head, has_more: false })
        if old_head == foo.head && new_head == last_head
    );
    assert_eq!(harness.get_head(&foo).await, last_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_passes_resolved_env_vars_to_the_ingest() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_expecting_env_var(
        &alias_foo,
        ConstDatasetEnvVarResolver::KEY,
    ));
    harness.create_root_dataset(&alias_foo).await;

    let pull_response = harness.pull_local(&alias_foo).await;

    assert_matches!(
        pull_response.result,
        Ok(PullResult::UpToDate(PullResultUpToDate::PollingIngest(_)))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_planning_failure_notifies_listener() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::for_ingest_iterations(&alias_foo, vec![])
            .with_failing_env_var_resolution(),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;

    let (pull_response, events) = harness.pull_local_recording(&alias_foo).await;

    assert_matches!(pull_response.result, Err(PullError::Internal(_)));
    assert_eq!(events, vec!["error"]);
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_commits_head() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let reported_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_transform_updated(
        &alias_foo,
        &alias_bar,
        &reported_heads,
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    let transformed_head = harness.append_unreferenced_block(&bar, "transformed").await;
    reported_heads.set(&bar.head, &transformed_head);

    let pull_response = harness.pull_local(&alias_bar).await;

    assert_matches!(
        pull_response.result,
        Ok(PullResult::Updated { old_head: Some(old_head), new_head, .. })
        if old_head == bar.head && new_head == transformed_head
    );
    assert_eq!(harness.get_head(&bar).await, transformed_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_head_moved_concurrently() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let reported_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_transform_updated(
        &alias_foo,
        &alias_bar,
        &reported_heads,
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    let transformed_head = harness.append_unreferenced_block(&bar, "transformed").await;
    let concurrent_head = harness.append_unreferenced_block(&bar, "concurrent").await;
    reported_heads.set(&bar.head, &transformed_head);
    harness.move_head(&bar, &bar.head, &concurrent_head).await;

    let pull_response = harness.pull_local(&alias_bar).await;

    assert_matches!(
        pull_response.result,
        Err(PullError::HeadMoved(e))
        if e.expected == Some(bar.head.clone()) && e.actual == Some(concurrent_head.clone())
    );
    assert_eq!(harness.get_head(&bar).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_reports_success_after_commit() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let reported_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_transform_updated(
        &alias_foo,
        &alias_bar,
        &reported_heads,
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    let transformed_head = harness.append_unreferenced_block(&bar, "transformed").await;
    reported_heads.set(&bar.head, &transformed_head);

    let (pull_response, events) = harness.pull_local_recording(&alias_bar).await;

    assert_matches!(pull_response.result, Ok(PullResult::Updated { .. }));
    assert_eq!(events, vec!["success"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_head_moved_reports_error_not_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let reported_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_transform_updated(
        &alias_foo,
        &alias_bar,
        &reported_heads,
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    let transformed_head = harness.append_unreferenced_block(&bar, "transformed").await;
    let concurrent_head = harness.append_unreferenced_block(&bar, "concurrent").await;
    reported_heads.set(&bar.head, &transformed_head);
    harness.move_head(&bar, &bar.head, &concurrent_head).await;

    let (pull_response, events) = harness.pull_local_recording(&alias_bar).await;

    assert_matches!(pull_response.result, Err(PullError::HeadMoved(_)));
    assert_eq!(events, vec!["error"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_failing_after_reset_reports_reset_heads() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let reset_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::for_transform_resetting(&alias_foo, &alias_bar, true)
            .with_transform_elaboration_mock(
                MockTransformPlanner::new()
                    .make_expect_elaborate_transform_diverged(alias_bar.clone())
                    .make_expect_elaborate_transform(alias_bar.clone()),
            )
            .with_compaction_executor_mock(
                MockCompactionExecutor::new()
                    .make_expect_compaction_success(alias_bar.clone(), reset_heads.clone()),
            )
            .with_transform_execution_mock(
                MockTransformExecutor::new().make_expect_transform_error(alias_bar.clone(), || {
                    TransformExecuteError::Internal("Engine crashed".int_err())
                }),
            ),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    let reset_head = harness.append_unreferenced_block(&bar, "reset").await;
    reset_heads.set(&bar.head, &reset_head);

    let pull_response = harness
        .pull_local_resetting_on_diverged_input(&alias_bar)
        .await;

    assert_matches!(
        pull_response.result,
        Err(PullError::TransformStopped(e))
        if e.old_head == bar.head
            && e.new_head == reset_head
            && matches!(
                e.source,
                PullError::TransformError(TransformError::Execute(TransformExecuteError::Internal(_)))
            )
    );
    assert_eq!(harness.get_head(&bar).await, reset_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_diverged_input_fails_without_reset() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::for_transform(&alias_foo, &alias_bar)
            .with_transform_elaboration_mock(
                MockTransformPlanner::new()
                    .make_expect_elaborate_transform_diverged(alias_bar.clone()),
            ),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let (pull_response, events) = harness.pull_local_recording(&alias_bar).await;

    assert_matches!(
        pull_response.result,
        Err(PullError::TransformError(TransformError::Elaborate(
            TransformElaborateError::InvalidInputInterval(_)
        )))
    );
    assert_eq!(events, vec!["error"]);
    assert_eq!(harness.get_head(&bar).await, bar.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_diverged_input_without_maintain_right_is_not_reset() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::for_transform_resetting(&alias_foo, &alias_bar, false)
            .with_transform_elaboration_mock(
                MockTransformPlanner::new()
                    .make_expect_elaborate_transform_diverged(alias_bar.clone()),
            ),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let pull_response = harness
        .pull_local_resetting_on_diverged_input(&alias_bar)
        .await;

    assert_matches!(pull_response.result, Err(PullError::Access(_)));
    assert_eq!(harness.get_head(&bar).await, bar.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_diverged_input_resets_and_retransforms() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let reset_heads = ReportedHeads::default();
    let transform_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::for_transform_resetting(&alias_foo, &alias_bar, true)
            .with_transform_elaboration_mock(
                MockTransformPlanner::new()
                    .make_expect_elaborate_transform_diverged(alias_bar.clone())
                    .make_expect_elaborate_transform(alias_bar.clone()),
            )
            .with_compaction_executor_mock(
                MockCompactionExecutor::new()
                    .make_expect_compaction_success(alias_bar.clone(), reset_heads.clone()),
            )
            .with_transform_execution_mock(
                MockTransformExecutor::new()
                    .make_expect_transform_updated(alias_bar.clone(), transform_heads.clone()),
            ),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    let reset_head = harness.append_unreferenced_block(&bar, "reset").await;
    let transformed_head = harness.append_unreferenced_block(&bar, "transformed").await;
    reset_heads.set(&bar.head, &reset_head);
    transform_heads.set(&reset_head, &transformed_head);

    let pull_response = harness
        .pull_local_resetting_on_diverged_input(&alias_bar)
        .await;

    assert_matches!(
        pull_response.result,
        Ok(PullResult::Updated { old_head: Some(old_head), new_head, .. })
        if old_head == bar.head && new_head == transformed_head
    );
    assert_eq!(harness.get_head(&bar).await, transformed_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_reset_then_up_to_date_reports_updated() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let reset_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::for_transform_resetting(&alias_foo, &alias_bar, true)
            .with_transform_elaboration_mock(
                MockTransformPlanner::new()
                    .make_expect_elaborate_transform_diverged(alias_bar.clone())
                    .make_expect_elaborate_transform_up_to_date(alias_bar.clone()),
            )
            .with_compaction_executor_mock(
                MockCompactionExecutor::new()
                    .make_expect_compaction_success(alias_bar.clone(), reset_heads.clone()),
            ),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    let reset_head = harness.append_unreferenced_block(&bar, "reset").await;
    reset_heads.set(&bar.head, &reset_head);

    let pull_response = harness
        .pull_local_resetting_on_diverged_input(&alias_bar)
        .await;

    assert_matches!(
        pull_response.result,
        Ok(PullResult::Updated { old_head: Some(old_head), new_head, .. })
        if old_head == bar.head && new_head == reset_head
    );
    assert_eq!(harness.get_head(&bar).await, reset_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_input_not_pulled() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let foo_handle = odf::DatasetHandle::new(
        odf::DatasetID::new_seeded_ed25519(b"foo"),
        alias_foo.clone(),
        odf::DatasetKind::Root,
    );

    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::for_transform(&alias_foo, &alias_bar)
            .with_transform_elaboration_mock(
                MockTransformPlanner::new().make_expect_elaborate_transform_error(
                    alias_bar.clone(),
                    move || {
                        InputNotPulledError {
                            dataset_handle: foo_handle.clone(),
                        }
                        .into()
                    },
                ),
            ),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let pull_response = harness.pull_local(&alias_bar).await;

    assert_matches!(
        pull_response.result,
        Err(PullError::TransformError(TransformError::Elaborate(
            TransformElaborateError::InputNotPulled(e)
        )))
        if e.dataset_handle.alias == alias_foo
    );
    assert_eq!(harness.get_head(&bar).await, bar.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_sync_into_existing_commits_from_its_head_and_keeps_pull_aliases() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let remote_ref = PullUseCaseHarness::remote_ref_of(&alias_foo);

    let mocks =
        PullUseCaseHarnessMocks::default().for_sync_into_existing_updated(&alias_foo, &remote_ref);
    let harness = PullUseCaseHarness::new(mocks);
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness
        .copy_dataset_to_remote_repo(&foo.dataset_handle.id, &alias_foo.dataset_name)
        .await;

    let pull_request = PullRequest::Remote(PullRequestRemote {
        remote_ref: remote_ref.clone(),
        maybe_local_alias: Some(alias_foo.clone()),
    });

    let pull_response = harness
        .pull(pull_request.clone(), PullOptions::default())
        .await;

    assert_matches!(
        pull_response,
        PullResponse {
            maybe_original_request: Some(a_pull_request),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: Some(a_remote_ref),
            result: Ok(PullResult::Updated { old_head: Some(old_head), new_head, .. }),
        } if a_pull_request == pull_request &&
        a_local_ref == foo.dataset_handle.as_local_ref() &&
        a_remote_ref == remote_ref &&
        old_head == foo.head &&
        new_head == PullUseCaseHarnessMocks::synced_head(),
    );

    // Only a pull that creates its dataset stores the pull alias
    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_sync_head_moved_is_a_conflict() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let remote_ref = PullUseCaseHarness::remote_ref_of(&alias_foo);

    let mocks = PullUseCaseHarnessMocks::default().for_sync_into_existing(
        &alias_foo,
        &remote_ref,
        Err(SyncError::HeadMoved(
            odf::dataset::RefCASError {
                reference: odf::BlockRef::Head,
                expected: Some(odf::Multihash::from_digest_sha3_256(b"planned")),
                actual: Some(odf::Multihash::from_digest_sha3_256(b"concurrent")),
            }
            .into(),
        )),
    );

    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;

    harness
        .copy_dataset_to_remote_repo(&foo.dataset_handle.id, &alias_foo.dataset_name)
        .await;

    let pull_response = harness
        .pull(
            PullRequest::Remote(PullRequestRemote {
                remote_ref,
                maybe_local_alias: Some(alias_foo),
            }),
            PullOptions::default(),
        )
        .await;

    assert_matches!(pull_response.result, Err(PullError::HeadMoved(_)));

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_sync_head_moved_without_heads_stays_a_sync_error() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let remote_ref = PullUseCaseHarness::remote_ref_of(&alias_foo);

    let mocks = PullUseCaseHarnessMocks::default().for_sync_into_existing(
        &alias_foo,
        &remote_ref,
        Err(SyncError::HeadMoved(SyncHeadMovedError::unreported())),
    );
    let harness = PullUseCaseHarness::new(mocks);
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness
        .copy_dataset_to_remote_repo(&foo.dataset_handle.id, &alias_foo.dataset_name)
        .await;

    let pull_response = harness
        .pull(
            PullRequest::Remote(PullRequestRemote {
                remote_ref,
                maybe_local_alias: Some(alias_foo),
            }),
            PullOptions::default(),
        )
        .await;

    assert_matches!(
        pull_response.result,
        Err(PullError::SyncError(SyncError::HeadMoved(
            SyncHeadMovedError { heads: None }
        )))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_sync_transfer_failure_is_reported_and_commits_nothing() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let remote_ref = PullUseCaseHarness::remote_ref_of(&alias_foo);

    let mocks = PullUseCaseHarnessMocks::default()
        .with_permission_checker_mock(
            MockDatasetPermissionChecker::new().make_expect_classify_datasets_by_allowance(
                DatasetPermission::Write,
                1,
                HashSet::from_iter([alias_foo.clone()]),
            ),
        )
        .with_sync_mock(MockSyncService::new().make_expect_transfer_error(
            alias_foo.clone(),
            || {
                DatasetsDivergedError {
                    src_head: odf::Multihash::from_digest_sha3_256(b"remote"),
                    dst_head: odf::Multihash::from_digest_sha3_256(b"local"),
                    detail: None,
                }
                .into()
            },
        ));

    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;

    harness
        .copy_dataset_to_remote_repo(&foo.dataset_handle.id, &alias_foo.dataset_name)
        .await;

    let pull_response = harness
        .pull(
            PullRequest::Remote(PullRequestRemote {
                remote_ref,
                maybe_local_alias: Some(alias_foo),
            }),
            PullOptions::default(),
        )
        .await;

    assert_matches!(
        pull_response.result,
        Err(PullError::SyncError(SyncError::DatasetsDiverged(_)))
    );
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_multi_recursive() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let alias_foo_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo-bar"));

    let mocks = PullUseCaseHarnessMocks::default()
        .with_permission_checker_mock(
            MockDatasetPermissionChecker::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    1,
                    HashSet::from_iter([alias_foo.clone(), alias_bar.clone(), alias_baz.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    1,
                    HashSet::from_iter([alias_foo_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Read,
                    1,
                    HashSet::from_iter([alias_foo.clone(), alias_bar.clone()]),
                ),
        )
        .with_polling_ingest_mock(
            MockPollingIngestService::new()
                .make_expect_ingest(alias_foo.clone())
                .make_expect_ingest(alias_bar.clone())
                .make_expect_ingest(alias_baz.clone()),
        )
        .with_transform_elaboration_mock(
            MockTransformPlanner::new().make_expect_elaborate_transform(alias_foo_bar.clone()),
        )
        .with_transform_execution_mock(
            MockTransformExecutor::new().make_expect_transform(alias_foo_bar.clone()),
        );
    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let baz = harness.create_root_dataset(&alias_baz).await;

    let foo_bar = harness
        .create_derived_dataset(
            &alias_foo_bar,
            vec![
                foo.dataset_handle.as_local_ref(),
                bar.dataset_handle.as_local_ref(),
            ],
        )
        .await;

    harness.index_dataset_dependencies().await;

    let pull_responses = harness
        .use_case
        .execute_multi(
            vec![
                PullRequest::Local(baz.dataset_handle.as_local_ref()),
                PullRequest::Local(foo_bar.dataset_handle.as_local_ref()),
            ],
            PullOptions {
                recursive: true,
                ..PullOptions::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(4, pull_responses.len());

    let responses_by_name: HashMap<_, _> = pull_responses
        .into_iter()
        .map(|response| {
            assert!(response.maybe_local_ref.is_some());
            (
                response
                    .maybe_local_ref
                    .as_ref()
                    .unwrap()
                    .dataset_name()
                    .unwrap()
                    .clone(),
                response,
            )
        })
        .collect();

    assert_matches!(
        responses_by_name.get(&alias_foo.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: None,  // triggered via recursion
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::PollingIngest(
                PollingIngestResultUpToDate { uncacheable: false }
            )))
        } if *a_local_ref == foo.dataset_handle.as_local_ref()
    );

    assert_matches!(
        responses_by_name.get(&alias_bar.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: None, // triggered via recursion
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::PollingIngest(
                PollingIngestResultUpToDate { uncacheable: false }
            )))
        } if *a_local_ref == bar.dataset_handle.as_local_ref()
    );

    assert_matches!(
        responses_by_name.get(&alias_baz.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::PollingIngest(
                PollingIngestResultUpToDate { uncacheable: false }
            )))
        } if *a_local_ref == baz.dataset_handle.as_local_ref()
    );

    assert_matches!(
        responses_by_name.get(&alias_foo_bar.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::Transform))
        } if *a_local_ref == foo_bar.dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_all_owned() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let alias_foo_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo-bar"));
    let alias_foo_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo-baz"));

    let baz_ref_remote = odf::DatasetRefRemote::Alias(odf::DatasetAliasRemote {
        repo_name: odf::RepoName::new_unchecked(REMOTE_REPO_NAME_STR),
        account_name: None,
        dataset_name: alias_baz.dataset_name.clone(),
    });

    let mocks = PullUseCaseHarnessMocks::default()
        .with_permission_checker_mock(
            MockDatasetPermissionChecker::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    1,
                    HashSet::from_iter([alias_foo.clone(), alias_bar.clone(), alias_baz.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    1,
                    HashSet::from_iter([alias_foo_bar.clone(), alias_foo_baz.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Read,
                    1,
                    HashSet::from_iter([alias_foo.clone(), alias_bar.clone(), alias_baz.clone()]),
                ),
        )
        .with_polling_ingest_mock(
            MockPollingIngestService::new()
                .make_expect_ingest(alias_foo.clone())
                .make_expect_ingest(alias_bar.clone()),
        )
        .with_sync_mock(
            MockSyncService::new().make_expect_transfer_from_remote_into_existing_local(
                alias_baz.clone(),
                baz_ref_remote.clone(),
                None,
            ),
        )
        .with_transform_elaboration_mock(
            MockTransformPlanner::new()
                .make_expect_elaborate_transform(alias_foo_bar.clone())
                .make_expect_elaborate_transform(alias_foo_baz.clone()),
        )
        .with_transform_execution_mock(
            MockTransformExecutor::new()
                .make_expect_transform(alias_foo_bar.clone())
                .make_expect_transform(alias_foo_baz.clone()),
        );
    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let baz = harness.create_root_dataset(&alias_baz).await;

    harness
        .copy_dataset_to_remote_repo(&baz.dataset_handle.id, &alias_baz.dataset_name)
        .await;
    harness
        .get_remote_aliases(&baz)
        .await
        .add(&baz_ref_remote, RemoteAliasKind::Pull)
        .await
        .unwrap();

    let foo_bar = harness
        .create_derived_dataset(
            &alias_foo_bar,
            vec![
                foo.dataset_handle.as_local_ref(),
                bar.dataset_handle.as_local_ref(),
            ],
        )
        .await;
    let foo_baz = harness
        .create_derived_dataset(
            &alias_foo_baz,
            vec![
                foo.dataset_handle.as_local_ref(),
                baz.dataset_handle.as_local_ref(),
            ],
        )
        .await;

    harness.index_dataset_dependencies().await;

    let pull_responses = harness
        .use_case
        .execute_all_owned(PullOptions::default(), None)
        .await
        .unwrap();
    assert_eq!(5, pull_responses.len());

    let responses_by_name: HashMap<_, _> = pull_responses
        .into_iter()
        .map(|response| {
            assert!(response.maybe_local_ref.is_some());
            (
                response
                    .maybe_local_ref
                    .as_ref()
                    .unwrap()
                    .dataset_name()
                    .unwrap()
                    .clone(),
                response,
            )
        })
        .collect();

    assert_matches!(
        responses_by_name.get(&alias_foo.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::PollingIngest(
                PollingIngestResultUpToDate { uncacheable: false }
            )))
        } if *a_local_ref == foo.dataset_handle.as_local_ref()
    );

    assert_matches!(
        responses_by_name.get(&alias_bar.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::PollingIngest(
                PollingIngestResultUpToDate { uncacheable: false }
            )))
        } if *a_local_ref == bar.dataset_handle.as_local_ref()
    );

    assert_matches!(
        responses_by_name.get(&alias_baz.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: Some(a_remote_ref),
            result: Ok(PullResult::UpToDate(PullResultUpToDate::Sync))
        } if *a_local_ref == baz.dataset_handle.as_local_ref() && *a_remote_ref == baz_ref_remote
    );

    assert_matches!(
        responses_by_name.get(&alias_foo_bar.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::Transform))
        } if *a_local_ref == foo_bar.dataset_handle.as_local_ref()
    );

    assert_matches!(
        responses_by_name.get(&alias_foo_baz.dataset_name).unwrap(),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Ok(PullResult::UpToDate(PullResultUpToDate::Transform))
        } if *a_local_ref == foo_baz.dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_authorization_denial_still_pulls_allowed_datasets() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let mocks = PullUseCaseHarnessMocks::default()
        .with_permission_checker_mock(
            MockDatasetPermissionChecker::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    1,
                    HashSet::from_iter([alias_foo.clone(), alias_baz.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Read,
                    1,
                    HashSet::from_iter([alias_bar.clone()]),
                ),
        )
        .with_polling_ingest_mock(
            MockPollingIngestService::new()
                .make_expect_ingest(alias_foo.clone())
                .make_expect_ingest(alias_baz.clone()),
        );
    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let baz = harness.create_root_dataset(&alias_baz).await;

    harness.index_dataset_dependencies().await;

    let pull_responses = harness
        .use_case
        .execute_multi(
            vec![
                PullRequest::local(alias_foo.as_local_ref()),
                PullRequest::local(alias_bar.as_local_ref()),
                PullRequest::local(alias_baz.as_local_ref()),
            ],
            PullOptions::default(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(3, pull_responses.len());

    let response_for = |handle: &odf::DatasetHandle| {
        pull_responses
            .iter()
            .find(|r| r.maybe_local_ref == Some(handle.as_local_ref()))
            .unwrap()
    };

    assert_matches!(
        response_for(&bar.dataset_handle),
        PullResponse {
            result: Err(PullError::Access(_)),
            ..
        }
    );
    for allowed in [&foo, &baz] {
        assert_matches!(
            response_for(&allowed.dataset_handle),
            PullResponse { result: Ok(_), .. }
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_recursive_stops_after_depth_with_failed_job() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    // No transform of `bar` is expected
    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_iterations(
        &alias_foo,
        vec![MockIngestIteration::Unreachable],
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    harness.index_dataset_dependencies().await;

    let pull_responses = harness.pull_recursive(&alias_bar).await;

    assert_matches!(
        pull_responses.as_slice(),
        [PullResponse {
            maybe_local_ref: Some(a_local_ref),
            result: Err(PullError::PollingIngestError(PollingIngestError::Unreachable { .. })),
            ..
        }] if *a_local_ref == foo.dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_recursive_stops_after_depth_with_denied_job() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    // Neither an ingest of `foo` nor a transform of `bar` is expected
    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::default().with_permission_checker_mock(
            MockDatasetPermissionChecker::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    1,
                    HashSet::new(),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Read,
                    1,
                    HashSet::from_iter([alias_foo.clone(), alias_bar.clone()]),
                ),
        ),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    harness.index_dataset_dependencies().await;

    let pull_responses = harness.pull_recursive(&alias_bar).await;

    assert_matches!(
        pull_responses.as_slice(),
        [PullResponse {
            maybe_local_ref: Some(a_local_ref),
            result: Err(PullError::Access(_)),
            ..
        }] if *a_local_ref == foo.dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_planning_error_runs_no_job() {
    let alias_a = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("a"));
    let alias_d = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("d"));

    // Neither an authorization nor an ingest of `a` is expected
    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::default());
    harness.create_root_dataset(&alias_a).await;
    let d = harness.create_derivative_without_transform(&alias_d).await;
    harness.index_dataset_dependencies().await;

    let pull_responses = harness.pull_multi(&[&alias_d, &alias_a]).await;

    assert_matches!(
        pull_responses.as_slice(),
        [PullResponse {
            maybe_local_ref: Some(a_local_ref),
            result: Err(PullError::TransformError(TransformError::Plan(
                TransformPlanError::TransformNotDefined(_)
            ))),
            ..
        }] if *a_local_ref == d.dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_sync_into_new_dataset_skips_authorization() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let remote_ref = PullUseCaseHarness::remote_ref_of(&alias_foo);

    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::default()
            .with_permission_checker_denying_everything()
            .for_sync_into_new(&alias_foo, &remote_ref),
    );

    let pull_response = harness
        .pull(
            PullRequest::Remote(PullRequestRemote {
                remote_ref,
                maybe_local_alias: Some(alias_foo),
            }),
            // The commit is mocked and creates no dataset to store the alias of
            PullOptions {
                add_aliases: false,
                ..PullOptions::default()
            },
        )
        .await;

    assert_matches!(
        pull_response.result,
        Ok(PullResult::Updated { old_head: None, new_head, .. })
        if new_head == PullUseCaseHarnessMocks::synced_head()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_reset_with_nothing_to_compact_keeps_diverged_error() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::for_transform_resetting(&alias_foo, &alias_bar, true)
            .with_transform_elaboration_mock(
                MockTransformPlanner::new()
                    .make_expect_elaborate_transform_diverged(alias_bar.clone()),
            )
            .with_compaction_executor_mock(
                MockCompactionExecutor::new()
                    .make_expect_compaction_nothing_to_do(alias_bar.clone()),
            ),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let pull_response = harness
        .pull_local_resetting_on_diverged_input(&alias_bar)
        .await;

    assert_matches!(
        pull_response.result,
        Err(PullError::TransformError(TransformError::Elaborate(
            TransformElaborateError::InvalidInputInterval(_)
        )))
    );
    assert_eq!(harness.get_head(&bar).await, bar.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_reset_head_moved_concurrently() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let reset_heads = ReportedHeads::default();

    // The transform is not elaborated again after the lost reset
    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::for_transform_resetting(&alias_foo, &alias_bar, true)
            .with_transform_elaboration_mock(
                MockTransformPlanner::new()
                    .make_expect_elaborate_transform_diverged(alias_bar.clone()),
            )
            .with_compaction_executor_mock(
                MockCompactionExecutor::new()
                    .make_expect_compaction_success(alias_bar.clone(), reset_heads.clone()),
            ),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    let reset_head = harness.append_unreferenced_block(&bar, "reset").await;
    let concurrent_head = harness.append_unreferenced_block(&bar, "concurrent").await;
    reset_heads.set(&bar.head, &reset_head);
    harness.move_head(&bar, &bar.head, &concurrent_head).await;

    let pull_response = harness
        .pull_local_resetting_on_diverged_input(&alias_bar)
        .await;

    assert_matches!(
        pull_response.result,
        Err(PullError::HeadMoved(e))
        if e.expected == Some(bar.head.clone()) && e.actual == Some(concurrent_head.clone())
    );
    assert_eq!(harness.get_head(&bar).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_into_dataset_deleted_before_commit_is_not_found() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let reported_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_deleting_target(
        &alias_foo,
        &reported_heads,
    ));
    let foo = harness.create_root_dataset(&alias_foo).await;
    let ingested_head = harness.append_unreferenced_block(&foo, "ingested").await;
    reported_heads.set(&foo.head, &ingested_head);

    let (pull_response, events) = harness.pull_local_recording(&alias_foo).await;

    assert_matches!(pull_response.result, Err(PullError::NotFound(_)));
    assert_eq!(events, vec!["error"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_commit_not_allowed_is_access_error() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let reported_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::for_ingest_updated(&alias_foo, &reported_heads)
            .with_head_commits_denied(),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let ingested_head = harness.append_unreferenced_block(&foo, "ingested").await;
    reported_heads.set(&foo.head, &ingested_head);

    let (pull_response, events) = harness.pull_local_recording(&alias_foo).await;

    assert_matches!(pull_response.result, Err(PullError::Access(_)));
    assert_eq!(events, vec!["error"]);
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_sync_into_dataset_deleted_during_sync_is_not_found() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let remote_ref = PullUseCaseHarness::remote_ref_of(&alias_foo);

    let harness =
        PullUseCaseHarness::new(PullUseCaseHarnessMocks::default().for_sync_into_existing(
            &alias_foo,
            &remote_ref,
            Err(DatasetAnyRefUnresolvedError::new(alias_foo.as_local_ref()).into()),
        ));
    harness.create_root_dataset(&alias_foo).await;

    let (pull_response, events) = harness.pull_remote_recording(&alias_foo, &remote_ref).await;

    assert_matches!(pull_response.result, Err(PullError::NotFound(_)));
    assert_eq!(events, vec!["error"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_sync_reports_success_after_commit() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let remote_ref = PullUseCaseHarness::remote_ref_of(&alias_foo);

    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::default().for_sync_into_existing_updated(&alias_foo, &remote_ref),
    );
    harness.create_root_dataset(&alias_foo).await;

    let (pull_response, events) = harness.pull_remote_recording(&alias_foo, &remote_ref).await;

    assert_matches!(pull_response.result, Ok(PullResult::Updated { .. }));
    assert_eq!(events, vec!["success"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_sync_failed_commit_reports_error_not_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let remote_ref = PullUseCaseHarness::remote_ref_of(&alias_foo);

    let harness =
        PullUseCaseHarness::new(PullUseCaseHarnessMocks::default().for_sync_into_existing(
            &alias_foo,
            &remote_ref,
            Err(SyncError::HeadMoved(SyncHeadMovedError::unreported())),
        ));
    harness.create_root_dataset(&alias_foo).await;

    let (pull_response, events) = harness.pull_remote_recording(&alias_foo, &remote_ref).await;

    assert_matches!(pull_response.result, Err(PullError::SyncError(_)));
    assert_eq!(events, vec!["error"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_with_allowed_input_added_after_planning_commits() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));
    let reported_heads = ReportedHeads::default();

    let baz_handle = odf::DatasetHandle::new(
        odf::DatasetID::new_seeded_ed25519(b"baz"),
        alias_baz.clone(),
        odf::DatasetKind::Root,
    );

    // The recheck of the transform reading `baz` grants it
    let mocks = PullUseCaseHarnessMocks::default()
        .with_permission_checker_mock(
            MockDatasetPermissionChecker::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    2,
                    HashSet::from_iter([alias_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Read,
                    2,
                    HashSet::from_iter([alias_foo.clone(), alias_baz]),
                ),
        )
        .with_transform_elaboration_mock(
            MockTransformPlanner::new()
                .make_expect_elaborate_transform_reading(alias_bar.clone(), vec![baz_handle]),
        )
        .with_transform_execution_mock(
            MockTransformExecutor::new()
                .make_expect_transform_updated(alias_bar.clone(), reported_heads.clone()),
        );

    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    let transformed_head = harness.append_unreferenced_block(&bar, "transformed").await;
    reported_heads.set(&bar.head, &transformed_head);

    let pull_response = harness.pull_local(&alias_bar).await;

    assert_matches!(
        pull_response.result,
        Ok(PullResult::Updated { old_head: Some(old_head), new_head, .. })
        if old_head == bar.head && new_head == transformed_head
    );
    assert_eq!(harness.get_head(&bar).await, transformed_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static REMOTE_REPO_NAME_STR: &str = "remote";

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct PullUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn PullDatasetUseCase>,
    dependency_graph_indexer: Arc<DependencyGraphIndexer>,
    remote_aliases_registry: Arc<dyn RemoteAliasesRegistry>,
    remote_tmp_dir: TempDir,
}

impl PullUseCaseHarness {
    fn new(mocks: PullUseCaseHarnessMocks) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_permission_checker(Some(mocks.mock_dataset_permission_checker)),
        );

        let repos_dir = base_use_case_harness.temp_dir_path().join("repos");
        std::fs::create_dir(&repos_dir).unwrap();

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        kamu_datasets_inmem::register_dependencies(&mut b);
        b.add::<PullDatasetUseCaseImpl>()
            .add::<DatasetUpdateStepsImpl>()
            .add::<PullRequestPlannerImpl>()
            .add::<DatasetUpdateKindResolverImpl>()
            .add::<DatasetPullSourceResolverImpl>()
            .add::<PollingIngestPlannerImpl>()
            .add::<CompactionPlannerImpl>()
            .add_value(mocks.mock_compaction_executor)
            .bind::<dyn CompactionExecutor, MockCompactionExecutor>()
            .add_value(mocks.mock_sync_service)
            .bind::<dyn SyncService, MockSyncService>()
            .add_value(mocks.mock_local_sync_committer)
            .bind::<dyn LocalSyncCommitter, MockLocalSyncCommitter>()
            .add::<SyncRequestBuilder>()
            .add::<DatasetFactoryImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add::<RemoteDatasetUrlResolverImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add::<RemoteRepositoryRegistryImpl>()
            .add_value(RemoteReposDir::new(repos_dir))
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add_value(IpfsGateway::default())
            .add::<DependencyGraphServiceImpl>()
            .add::<DependencyGraphIndexer>();

        if mocks.real_ingest {
            let cache_dir = base_use_case_harness.temp_dir_path().join("cache");
            std::fs::create_dir(&cache_dir).unwrap();

            b.add_value(CacheDir::new(cache_dir))
                .add_value(ContainerRuntimeConfig::default())
                .add::<ContainerRuntime>()
                .add::<ObjectStoreRegistryImpl>()
                .add::<ObjectStoreBuilderLocalFs>()
                .add_value(EngineProvisionerLocalConfig::default())
                .add::<EngineProvisionerLocal>()
                .add::<DataFormatRegistryImpl>()
                .add::<FetchService>()
                .add_value(EngineConfigDatafusionEmbeddedIngest::default())
                .add::<PollingIngestServiceImpl>()
                .add::<DatasetKeyValueServiceSysEnv>();
        } else {
            b.add_value(mocks.mock_polling_ingest_service)
                .bind::<dyn PollingIngestService, MockPollingIngestService>();
        }

        if let Some(watermark) = mocks.real_transform_watermark {
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
        } else {
            b.add_value(TransformPlannerWithMockedElaboration {
                real: TransformPlannerImpl::new(
                    base_use_case_harness.catalog().get_one().unwrap(),
                    base_use_case_harness.catalog().get_one().unwrap(),
                ),
                mock: mocks.mock_transform_elaboration,
            })
            .bind::<dyn TransformPlanner, TransformPlannerWithMockedElaboration>()
            .add_value(mocks.mock_transform_execution_service)
            .bind::<dyn TransformExecutor, MockTransformExecutor>();
        }

        if mocks.head_commits_denied {
            b.add::<ReadOnlyDatasetHeadCommitter>();
        } else {
            b.add::<DatasetHeadCommitterImpl>();
        }

        match mocks.env_var_resolution {
            None => {}
            Some(EnvVarResolution::Failing) => {
                b.add_value(SecretsEncryptionConfig::sample())
                    .add::<FailingDatasetEnvVarResolver>();
            }
            Some(EnvVarResolution::Const) => {
                b.add_value(SecretsEncryptionConfig::sample())
                    .add::<ConstDatasetEnvVarResolver>();
            }
        }

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        let remote_tmp_dir = tempfile::tempdir().unwrap();
        let remote_repo_url = Url::from_directory_path(remote_tmp_dir.path()).unwrap();

        let remote_repo_name = odf::RepoName::new_unchecked(REMOTE_REPO_NAME_STR);
        let remote_repo_registry = catalog.get_one::<dyn RemoteRepositoryRegistry>().unwrap();
        remote_repo_registry
            .add_repository(&remote_repo_name, remote_repo_url)
            .unwrap();

        Self {
            base_use_case_harness,
            use_case: catalog.get_one().unwrap(),
            dependency_graph_indexer: catalog.get_one().unwrap(),
            remote_aliases_registry: catalog.get_one().unwrap(),
            remote_tmp_dir,
        }
    }

    /// A root dataset polling a CSV file in the harness directory that holds
    /// `contents`
    async fn create_root_dataset_polling_csv_with(
        &self,
        alias: &odf::DatasetAlias,
        contents: &str,
    ) -> CreateDatasetResult {
        let src_path = self.temp_dir_path().join("data.csv");
        std::fs::write(&src_path, contents).unwrap();
        self.create_root_dataset_polling_csv(alias, &src_path).await
    }

    async fn pull_local(&self, alias: &odf::DatasetAlias) -> PullResponse {
        self.pull_local_with(alias, PullOptions::default()).await
    }

    async fn pull_local_exhausting_sources(&self, alias: &odf::DatasetAlias) -> PullResponse {
        self.pull_local_with(
            alias,
            PullOptions {
                exhaust_sources: true,
                ..PullOptions::default()
            },
        )
        .await
    }

    /// Chains one unreferenced block per entry from `created.head`, sets each
    /// entry to report the step from the previous block onto its own, and
    /// returns the last block
    async fn chain_reported_heads(
        &self,
        created: &CreateDatasetResult,
        heads: &[ReportedHeads],
    ) -> odf::Multihash {
        let mut prev = created.head.clone();
        for (i, reported) in heads.iter().enumerate() {
            let next = self
                .append_unreferenced_block_after(created, &prev, &format!("iteration {i}"))
                .await;
            reported.set(&prev, &next);
            prev = next;
        }
        prev
    }

    async fn pull_local_resetting_on_diverged_input(
        &self,
        alias: &odf::DatasetAlias,
    ) -> PullResponse {
        self.pull_local_with(
            alias,
            PullOptions {
                transform_options: TransformOptions {
                    reset_derivatives_on_diverged_input: true,
                },
                ..PullOptions::default()
            },
        )
        .await
    }

    async fn pull_local_with(
        &self,
        alias: &odf::DatasetAlias,
        options: PullOptions,
    ) -> PullResponse {
        self.pull(PullRequest::Local(alias.as_local_ref()), options)
            .await
    }

    /// The dataset of the same name in the remote repository
    fn remote_ref_of(alias: &odf::DatasetAlias) -> odf::DatasetRefRemote {
        odf::DatasetRefRemote::Alias(odf::DatasetAliasRemote {
            repo_name: odf::RepoName::new_unchecked(REMOTE_REPO_NAME_STR),
            account_name: None,
            dataset_name: alias.dataset_name.clone(),
        })
    }

    async fn pull(&self, request: PullRequest, options: PullOptions) -> PullResponse {
        self.use_case.execute(request, options, None).await.unwrap()
    }

    /// Pulls with a listener and returns the ingest and transform events it
    /// received
    async fn pull_local_recording(
        &self,
        alias: &odf::DatasetAlias,
    ) -> (PullResponse, Vec<&'static str>) {
        self.pull_recording(PullRequest::Local(alias.as_local_ref()))
            .await
    }

    /// Pulls `alias` from `remote_ref` with a listener and returns the sync
    /// events it received
    async fn pull_remote_recording(
        &self,
        alias: &odf::DatasetAlias,
        remote_ref: &odf::DatasetRefRemote,
    ) -> (PullResponse, Vec<&'static str>) {
        self.pull_recording(PullRequest::Remote(PullRequestRemote {
            remote_ref: remote_ref.clone(),
            maybe_local_alias: Some(alias.clone()),
        }))
        .await
    }

    async fn pull_recording(&self, request: PullRequest) -> (PullResponse, Vec<&'static str>) {
        let listener = Arc::new(RecordingListener::default());
        let pull_response = self
            .use_case
            .execute(request, PullOptions::default(), Some(listener.clone()))
            .await
            .unwrap();
        (pull_response, listener.events())
    }

    /// Pulls `alias` with everything it depends on
    async fn pull_recursive(&self, alias: &odf::DatasetAlias) -> Vec<PullResponse> {
        self.use_case
            .execute_multi(
                vec![PullRequest::Local(alias.as_local_ref())],
                PullOptions {
                    recursive: true,
                    ..PullOptions::default()
                },
                None,
            )
            .await
            .unwrap()
    }

    async fn pull_multi(&self, aliases: &[&odf::DatasetAlias]) -> Vec<PullResponse> {
        self.use_case
            .execute_multi(
                aliases
                    .iter()
                    .map(|alias| PullRequest::Local(alias.as_local_ref()))
                    .collect(),
                PullOptions::default(),
                None,
            )
            .await
            .unwrap()
    }

    /// A derivative with no `SetTransform`, which a pull cannot plan
    async fn create_derivative_without_transform(
        &self,
        alias: &odf::DatasetAlias,
    ) -> CreateDatasetResult {
        let store_result = create_test_dataset_from_snapshot(
            self.dataset_registry(),
            self.dataset_storage_unit_writer(),
            MetadataFactory::dataset_snapshot()
                .name(alias.clone())
                .kind(odf::DatasetKind::Derivative)
                .build(),
            odf::DatasetID::new_generated_ed25519().1,
            Utc::now(),
        )
        .await
        .unwrap();

        CreateDatasetResult::from_stored(store_result, alias.clone())
    }

    async fn get_remote_aliases(&self, created: &CreateDatasetResult) -> Box<dyn RemoteAliases> {
        self.remote_aliases_registry
            .get_remote_aliases(&created.dataset_handle)
            .await
            .unwrap()
    }

    async fn copy_dataset_to_remote_repo(
        &self,
        dataset_id: &odf::DatasetID,
        dataset_name: &odf::DatasetName,
    ) {
        let src_path = self
            .base_use_case_harness
            .temp_dir_path()
            .join("datasets")
            .join(dataset_id.as_multibase().to_stack_string());

        let dst_path = self.remote_tmp_dir.path().join(dataset_name);

        tokio::fs::create_dir_all(&dst_path).await.unwrap();
        file_utils::copy_dir_contents_recursively(&src_path, &dst_path).unwrap();
    }

    async fn index_dataset_dependencies(&self) {
        use init_on_startup::InitOnStartup;
        self.dependency_graph_indexer
            .run_initialization()
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Plans for real, so that pull planning sees the inputs it authorizes, while
/// elaboration follows the test's expectations
struct TransformPlannerWithMockedElaboration {
    real: TransformPlannerImpl,
    mock: MockTransformPlanner,
}

#[async_trait::async_trait]
impl TransformPlanner for TransformPlannerWithMockedElaboration {
    async fn describe_transform(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformDescription, TransformPlanError> {
        self.real.describe_transform(target).await
    }

    async fn elaborate_transform(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformElaboration, TransformElaborateError> {
        self.mock.elaborate_transform(target).await
    }

    async fn rebuild_transform(
        &self,
        target: ResolvedDataset,
        stored: &TransformPlanPayload,
    ) -> Result<TransformExecutionPlan, TransformRebuildError> {
        self.mock.rebuild_transform(target, stored).await
    }

    async fn evaluate_transform_status(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformStatus, TransformStatusError> {
        self.real.evaluate_transform_status(target).await
    }

    async fn build_transform_verification_plan(
        &self,
        target: ResolvedDataset,
        block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
    ) -> Result<VerifyTransformOperation, VerifyTransformPlanError> {
        self.real
            .build_transform_verification_plan(target, block_range)
            .await
    }

    async fn get_verification_inputs(
        &self,
        target: ResolvedDataset,
        block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
    ) -> Result<Vec<odf::DatasetHandle>, VerifyTransformPlanError> {
        self.real.get_verification_inputs(target, block_range).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PullUseCaseHarnessMocks {
    mock_dataset_permission_checker: MockDatasetPermissionChecker,
    mock_polling_ingest_service: MockPollingIngestService,
    mock_transform_elaboration: MockTransformPlanner,
    mock_transform_execution_service: MockTransformExecutor,
    mock_compaction_executor: MockCompactionExecutor,
    mock_sync_service: MockSyncService,
    mock_local_sync_committer: MockLocalSyncCommitter,
    /// Ingests with the real polling ingest service instead of the mock
    real_ingest: bool,
    /// Transforms with the real planner and executor, through an engine that
    /// only advances the watermark to this value
    real_transform_watermark: Option<DateTime<Utc>>,
    /// Enables dataset environment variables with this resolver
    env_var_resolution: Option<EnvVarResolution>,
    /// Registers [`ReadOnlyDatasetHeadCommitter`]
    head_commits_denied: bool,
}

impl PullUseCaseHarnessMocks {
    fn with_failing_env_var_resolution(self) -> Self {
        Self {
            env_var_resolution: Some(EnvVarResolution::Failing),
            ..self
        }
    }

    /// Expects one up-to-date ingest of `foo` that receives `env_var_key`,
    /// resolved by [`ConstDatasetEnvVarResolver`]
    fn for_ingest_expecting_env_var(
        alias_foo: &odf::DatasetAlias,
        env_var_key: &'static str,
    ) -> Self {
        let mut mock_polling_ingest_service = MockPollingIngestService::new();
        let alias = alias_foo.clone();
        mock_polling_ingest_service
            .expect_ingest()
            .withf(move |target, _, _, env_vars, _, _| {
                target.get_alias() == &alias && env_vars.as_map().contains_key(env_var_key)
            })
            .times(1)
            .returning(|_, _, _, _, _, _| Ok(PollingIngestResult::UpToDate { uncacheable: false }));

        Self {
            env_var_resolution: Some(EnvVarResolution::Const),
            ..Self::for_ingest_iterations(alias_foo, vec![])
        }
        .with_polling_ingest_mock(mock_polling_ingest_service)
    }

    fn with_permission_checker_mock(
        self,
        mock_dataset_permission_checker: MockDatasetPermissionChecker,
    ) -> Self {
        Self {
            mock_dataset_permission_checker,
            ..self
        }
    }

    fn with_polling_ingest_mock(
        self,
        mock_polling_ingest_service: MockPollingIngestService,
    ) -> Self {
        Self {
            mock_polling_ingest_service,
            ..self
        }
    }

    fn with_transform_elaboration_mock(
        self,
        mock_transform_elaboration: MockTransformPlanner,
    ) -> Self {
        Self {
            mock_transform_elaboration,
            ..self
        }
    }

    fn with_real_ingest(self) -> Self {
        Self {
            real_ingest: true,
            ..self
        }
    }

    fn with_real_transform(self, watermark: DateTime<Utc>) -> Self {
        Self {
            real_transform_watermark: Some(watermark),
            ..self
        }
    }

    fn with_transform_execution_mock(
        self,
        mock_transform_execution_service: MockTransformExecutor,
    ) -> Self {
        Self {
            mock_transform_execution_service,
            ..self
        }
    }

    fn with_compaction_executor_mock(
        self,
        mock_compaction_executor: MockCompactionExecutor,
    ) -> Self {
        Self {
            mock_compaction_executor,
            ..self
        }
    }

    /// Expects one ingest of `foo` that reports `reported_heads`.
    fn for_ingest_iterations(
        alias_foo: &odf::DatasetAlias,
        iterations: Vec<MockIngestIteration>,
    ) -> Self {
        Self::default()
            .with_permission_checker_mock(
                MockDatasetPermissionChecker::new().make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    1,
                    HashSet::from_iter([alias_foo.clone()]),
                ),
            )
            .with_polling_ingest_mock(
                MockPollingIngestService::new()
                    .make_expect_ingest_iterations(alias_foo, iterations),
            )
    }

    fn for_ingest_updated(alias_foo: &odf::DatasetAlias, reported_heads: &ReportedHeads) -> Self {
        Self::default()
            .with_permission_checker_mock(
                MockDatasetPermissionChecker::new().make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    1,
                    HashSet::from_iter([alias_foo.clone()]),
                ),
            )
            .with_polling_ingest_mock(
                MockPollingIngestService::new()
                    .make_expect_ingest_updated(alias_foo.clone(), reported_heads.clone()),
            )
    }

    /// Expects one transform of `bar` from `foo` that reports
    /// `reported_heads`.
    fn for_transform_updated(
        alias_foo: &odf::DatasetAlias,
        alias_bar: &odf::DatasetAlias,
        reported_heads: &ReportedHeads,
    ) -> Self {
        Self::for_transform(alias_foo, alias_bar)
            .with_transform_elaboration_mock(
                MockTransformPlanner::new().make_expect_elaborate_transform(alias_bar.clone()),
            )
            .with_transform_execution_mock(
                MockTransformExecutor::new()
                    .make_expect_transform_updated(alias_bar.clone(), reported_heads.clone()),
            )
    }

    /// Expects one pull of `bar` derived from `foo`, authorized
    fn for_transform(alias_foo: &odf::DatasetAlias, alias_bar: &odf::DatasetAlias) -> Self {
        Self::default().with_permission_checker_mock(
            MockDatasetPermissionChecker::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    1,
                    HashSet::from_iter([alias_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Read,
                    1,
                    HashSet::from_iter([alias_foo.clone()]),
                ),
        )
    }

    /// Like [`Self::for_transform`], for a pull that resets `bar` to metadata,
    /// with the right to maintain `bar` granted or not
    fn for_transform_resetting(
        alias_foo: &odf::DatasetAlias,
        alias_bar: &odf::DatasetAlias,
        maintain_granted: bool,
    ) -> Self {
        let maintainable = if maintain_granted {
            HashSet::from_iter([alias_bar.clone()])
        } else {
            HashSet::new()
        };
        // A denied Maintain makes the guard check that `bar` is readable
        let read_checks_of_bar = usize::from(!maintain_granted);
        Self::default().with_permission_checker_mock(
            MockDatasetPermissionChecker::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Write,
                    1,
                    HashSet::from_iter([alias_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Read,
                    1,
                    HashSet::from_iter([alias_foo.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Maintain,
                    1,
                    maintainable,
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetPermission::Read,
                    read_checks_of_bar,
                    HashSet::from_iter([alias_bar.clone()]),
                ),
        )
    }

    /// Every permission check fails, as for an account without any rights
    fn with_permission_checker_denying_everything(self) -> Self {
        let mut checker = MockDatasetPermissionChecker::new();
        checker
            .expect_classify_dataset_handles_by_allowance()
            .returning(|_, handles, permission| {
                Ok(ClassifyByAllowanceResponse {
                    authorized_handles: Vec::new(),
                    unauthorized_handles_with_errors: handles
                        .into_iter()
                        .map(|handle| {
                            let error =
                                ClassifyByAllowanceDatasetPermissionError::not_enough_permissions(
                                    handle.as_local_ref(),
                                    permission,
                                );
                            (handle, error)
                        })
                        .collect(),
                })
            });
        checker
            .expect_check_permission()
            .returning(|_, dataset_id, permission| {
                Err(DatasetPermissionError::not_enough_permissions(
                    dataset_id.as_local_ref(),
                    permission,
                ))
            });
        self.with_permission_checker_mock(checker)
    }

    /// Every `HEAD` move is refused, as by a read-only storage
    fn with_head_commits_denied(self) -> Self {
        Self {
            head_commits_denied: true,
            ..self
        }
    }

    /// Like [`Self::for_ingest_updated`], with an ingest that deletes the
    /// dataset's storage before it reports the update
    fn for_ingest_deleting_target(
        alias_foo: &odf::DatasetAlias,
        reported_heads: &ReportedHeads,
    ) -> Self {
        let mut mock_polling_ingest_service = MockPollingIngestService::new();
        let alias = alias_foo.clone();
        let heads = reported_heads.clone();
        mock_polling_ingest_service
            .expect_ingest()
            .withf(move |target, _, _, _, _, _| target.get_alias() == &alias)
            .times(1)
            .returning(move |target, plan, _, _, _, _| {
                std::fs::remove_dir_all(target.get_storage_internal_url().to_file_path().unwrap())
                    .unwrap();
                let (old_head, new_head) = heads.get();
                Ok(PollingIngestResult::Updated {
                    old_head,
                    new_head,
                    has_more: false,
                    uncacheable: false,
                    increment: odf::dataset::MetadataChainIncrementInterval {
                        num_blocks: 1,
                        num_records: 0,
                        updated_watermark: None,
                    },
                    new_source_state: None,
                    metadata_state: plan.into_metadata_state(),
                })
            });

        Self::for_ingest_iterations(alias_foo, vec![])
            .with_polling_ingest_mock(mock_polling_ingest_service)
    }

    /// Expects one pull of `remote_ref` into `alias_foo`, which does not exist,
    /// transferred and committed up to [`Self::synced_head`]
    fn for_sync_into_new(
        self,
        alias_foo: &odf::DatasetAlias,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Self {
        let mut mock_sync_service = MockSyncService::new();
        let alias = alias_foo.clone();
        let src = remote_ref.clone();
        mock_sync_service
            .expect_transfer_into_local()
            .withf(move |request, _, _| {
                request.src == src
                    && matches!(&request.dst, LocalSyncTarget::New(dst) if *dst == alias)
            })
            .times(1)
            .returning(|request, _, _| {
                let LocalSyncTarget::New(alias) = request.dst else {
                    unreachable!()
                };
                let seed = MetadataFactory::metadata_block(
                    MetadataFactory::seed(odf::DatasetKind::Root).build(),
                )
                .build();
                Ok(SyncTransfer::Transferred(TransferredSync {
                    target: TransferredSyncTarget::New {
                        alias,
                        seed_block_bytes: odf::storage::serialize_metadata_block(&seed).unwrap(),
                        seed_block: seed.into_typed().unwrap(),
                        visibility: odf::DatasetVisibility::Private,
                    },
                    new_head: Self::synced_head(),
                    num_blocks: 1,
                    increment: MockSyncService::REPORTED_INCREMENT,
                }))
            });

        Self {
            mock_local_sync_committer: MockLocalSyncCommitter::new().make_expect_commit(
                Self::synced_head(),
                Ok(SyncResult::Updated {
                    old_head: None,
                    new_head: Self::synced_head(),
                    num_blocks: 1,
                    increment: MockSyncService::REPORTED_INCREMENT,
                }),
            ),
            ..self
        }
        .with_sync_mock(mock_sync_service)
    }

    fn with_sync_mock(self, mock_sync_service: MockSyncService) -> Self {
        Self {
            mock_sync_service,
            ..self
        }
    }

    /// The head a mocked sync transfers up to
    fn synced_head() -> odf::Multihash {
        odf::Multihash::from_multibase(
            "f16205603b882241c71351baf996d6dba7e3ddbd571457e93c1cd282bdc61f9fed5f2",
        )
        .unwrap()
    }

    /// Expects one authorized pull of the existing `foo` from `remote_ref`,
    /// transferred up to [`Self::synced_head`] and committed with
    /// `commit_result`
    fn for_sync_into_existing(
        self,
        alias_foo: &odf::DatasetAlias,
        remote_ref: &odf::DatasetRefRemote,
        commit_result: Result<SyncResult, SyncError>,
    ) -> Self {
        self.for_sync_into_existing_committed_by(
            alias_foo,
            remote_ref,
            MockLocalSyncCommitter::new().make_expect_commit(Self::synced_head(), commit_result),
        )
    }

    /// Expects one authorized pull of the existing `foo` from `remote_ref`,
    /// committed from its expected head to [`Self::synced_head`]
    fn for_sync_into_existing_updated(
        self,
        alias_foo: &odf::DatasetAlias,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Self {
        self.for_sync_into_existing_committed_by(
            alias_foo,
            remote_ref,
            MockLocalSyncCommitter::new().make_expect_commit_into_existing(Self::synced_head()),
        )
    }

    fn for_sync_into_existing_committed_by(
        self,
        alias_foo: &odf::DatasetAlias,
        remote_ref: &odf::DatasetRefRemote,
        mock_local_sync_committer: MockLocalSyncCommitter,
    ) -> Self {
        let synced_heads = ReportedHeads::default();
        synced_heads.set(&Self::synced_head(), &Self::synced_head());

        Self {
            mock_local_sync_committer,
            ..self
        }
        .with_permission_checker_mock(
            MockDatasetPermissionChecker::new().make_expect_classify_datasets_by_allowance(
                DatasetPermission::Write,
                1,
                HashSet::from_iter([alias_foo.clone()]),
            ),
        )
        .with_sync_mock(
            MockSyncService::new().make_expect_transfer_from_remote_into_existing_local(
                alias_foo.clone(),
                remote_ref.clone(),
                Some(synced_heads),
            ),
        )
    }
}

impl Default for PullUseCaseHarnessMocks {
    fn default() -> Self {
        Self {
            mock_dataset_permission_checker: MockDatasetPermissionChecker::new(),
            mock_polling_ingest_service: MockPollingIngestService::new(),
            mock_transform_elaboration: MockTransformPlanner::new(),
            mock_transform_execution_service: MockTransformExecutor::new(),
            mock_compaction_executor: MockCompactionExecutor::new(),
            mock_sync_service: MockSyncService::new(),
            mock_local_sync_committer: MockLocalSyncCommitter::new(),
            real_ingest: false,
            real_transform_watermark: None,
            env_var_resolution: None,
            head_commits_denied: false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Refuses every `HEAD` move, as a read-only storage does
#[dill::component]
#[dill::interface(dyn DatasetHeadCommitter)]
struct ReadOnlyDatasetHeadCommitter {}

#[async_trait::async_trait]
impl DatasetHeadCommitter for ReadOnlyDatasetHeadCommitter {
    async fn commit_head(
        &self,
        _target: &ResolvedDataset,
        _expected_head: Option<&odf::Multihash>,
        _new_head: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        Err(odf::dataset::SetChainRefError::Access(
            odf::AccessError::ReadOnly(None),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Which [`DatasetEnvVarResolver`] the harness registers
enum EnvVarResolution {
    Failing,
    Const,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
