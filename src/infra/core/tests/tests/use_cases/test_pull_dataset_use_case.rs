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
use internal_error::{ErrorIntoInternal, InternalError};
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::*;
use kamu_core::engine::TransformResponseExt;
use kamu_core::testing::{MockIngestIteration, ReportedHeads};
use kamu_core::*;
use kamu_datasets::{
    CreateDatasetResult,
    DatasetAction,
    DatasetEnvVar,
    DatasetEnvVarResolver,
    DatasetEnvVarValue,
    GetDatasetEnvVarError,
    MockDatasetActionAuthorizer,
    ResolvedDataset,
    SecretsEncryptionConfig,
};
use kamu_datasets_inmem::InMemoryDatasetDependencyRepository;
use kamu_datasets_services::{
    DatasetHeadCommitterImpl,
    DependencyGraphIndexer,
    DependencyGraphServiceImpl,
};
use odf::dataset::{DatasetFactoryImpl, IpfsGateway};
use tempfile::TempDir;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_ingest_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let mocks = PullUseCaseHarnessMocks::default()
        .with_authorizer_mock(
            MockDatasetActionAuthorizer::new().make_expect_classify_datasets_by_allowance(
                DatasetAction::Write,
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
        .use_case
        .execute(pull_request.clone(), PullOptions::default(), None)
        .await
        .unwrap();

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
async fn test_pull_transform_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let mocks = PullUseCaseHarnessMocks::default()
        .with_authorizer_mock(
            MockDatasetActionAuthorizer::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
                    1,
                    HashSet::from_iter([alias_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Read,
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
        .use_case
        .execute(pull_request.clone(), PullOptions::default(), None)
        .await
        .unwrap();

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

    // The pull authorizes `foo`, but elaboration finds a transform reading `baz`
    let mocks = PullUseCaseHarnessMocks::default()
        .with_authorizer_mock(
            MockDatasetActionAuthorizer::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
                    1,
                    HashSet::from_iter([alias_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Read,
                    2,
                    HashSet::from_iter([alias_foo.clone()]),
                ),
        )
        .with_transform_elaboration_mock(
            MockTransformPlanner::new()
                .make_expect_elaborate_transform_reading(alias_bar.clone(), vec![baz_handle]),
        );

    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let pull_response = harness
        .use_case
        .execute(
            PullRequest::Local(alias_bar.as_local_ref()),
            PullOptions::default(),
            None,
        )
        .await
        .unwrap();

    assert_matches!(
        pull_response,
        PullResponse {
            maybe_local_ref: Some(a_local_ref),
            result: Err(PullError::Access(_)),
            ..
        } if a_local_ref == bar.dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_transform_with_real_planner_and_executor_commits_head() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let watermark = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();

    let mocks = PullUseCaseHarnessMocks::default()
        .with_authorizer_mock(
            MockDatasetActionAuthorizer::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
                    1,
                    HashSet::from_iter([alias_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Read,
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

    let pull_response = harness
        .use_case
        .execute(
            PullRequest::Local(alias_bar.as_local_ref()),
            PullOptions::default(),
            None,
        )
        .await
        .unwrap();

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
        Err(PullError::PollingIngestError(PollingIngestError::CommitError(
            odf::dataset::CommitError::MetadataAppendError(
                odf::dataset::AppendError::RefCASFailed(e)
            )
        )))
        if e.expected == Some(foo.head.clone()) && e.actual == Some(concurrent_head.clone())
    );
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
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
            source: PollingIngestError::Unreachable { .. },
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

#[tokio::test]
async fn test_pull_ingest_without_polling_source_is_up_to_date() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = PullUseCaseHarness::new(PullUseCaseHarnessMocks::for_ingest_iterations(
        &alias_foo,
        vec![],
    ));
    let foo = harness.create_root_dataset_without_source(&alias_foo).await;

    let (pull_response, ingest_events) = harness.pull_local_recording_ingest(&alias_foo).await;

    assert_matches!(
        pull_response.result,
        Ok(PullResult::UpToDate(PullResultUpToDate::PollingIngest(_)))
    );
    assert_eq!(ingest_events, vec!["begin", "no_polling_source"]);
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
            source: PollingIngestError::CommitError(_),
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

    let (pull_response, ingest_events) = harness.pull_local_recording_ingest(&alias_foo).await;

    assert_matches!(pull_response.result, Err(PullError::Internal(_)));
    assert_eq!(ingest_events, vec!["begin", "plan_error"]);
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
        Err(PullError::TransformError(TransformError::Execute(
            TransformExecuteError::CommitError(odf::dataset::CommitError::MetadataAppendError(
                odf::dataset::AppendError::RefCASFailed(e)
            ))
        )))
        if e.expected == Some(bar.head.clone()) && e.actual == Some(concurrent_head.clone())
    );
    assert_eq!(harness.get_head(&bar).await, concurrent_head);
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

    let pull_response = harness.pull_local(&alias_bar).await;

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
async fn test_pull_transform_diverged_input_resets_and_retransforms() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let reset_heads = ReportedHeads::default();
    let transform_heads = ReportedHeads::default();

    let harness = PullUseCaseHarness::new(
        PullUseCaseHarnessMocks::for_transform(&alias_foo, &alias_bar)
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
        PullUseCaseHarnessMocks::for_transform(&alias_foo, &alias_bar)
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
async fn test_pull_sync_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let remote_ref = odf::DatasetRefRemote::Alias(odf::DatasetAliasRemote {
        repo_name: odf::RepoName::new_unchecked(REMOTE_REPO_NAME_STR),
        account_name: None,
        dataset_name: alias_foo.dataset_name.clone(),
    });

    let mocks = PullUseCaseHarnessMocks::default().for_sync_into_existing(
        &alias_foo,
        &remote_ref,
        Ok(SyncResult::Updated {
            old_head: None,
            new_head: synced_head(),
            num_blocks: 0,
        }),
    );

    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;

    harness
        .copy_dataset_to_remote_repo(&foo.dataset_handle.id, &alias_foo.dataset_name)
        .await;

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));

    let pull_request = PullRequest::Remote(PullRequestRemote {
        remote_ref: remote_ref.clone(),
        maybe_local_alias: Some(alias_foo.clone()),
    });

    let pull_response = harness
        .use_case
        .execute(pull_request.clone(), PullOptions::default(), None)
        .await
        .unwrap();

    assert_matches!(
        pull_response,
        PullResponse {
            maybe_original_request: Some(a_pull_request),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: Some(a_remote_ref),
            result: Ok(PullResult::Updated { .. }),
        } if a_pull_request == pull_request &&
        a_local_ref == foo.dataset_handle.as_local_ref() &&
        a_remote_ref == remote_ref,
    );

    let aliases = harness.get_remote_aliases(&foo).await;
    let pull_aliases: Vec<_> = aliases.get_by_kind(RemoteAliasKind::Pull).collect();
    assert_eq!(
        pull_aliases,
        vec![&odf::DatasetRefRemote::Alias(odf::DatasetAliasRemote {
            repo_name: harness.remote_repo_name,
            account_name: None,
            dataset_name: alias_foo.dataset_name.clone()
        })]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_sync_success_without_saving_alias() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let remote_ref = odf::DatasetRefRemote::Alias(odf::DatasetAliasRemote {
        repo_name: odf::RepoName::new_unchecked(REMOTE_REPO_NAME_STR),
        account_name: None,
        dataset_name: alias_foo.dataset_name.clone(),
    });

    let mocks = PullUseCaseHarnessMocks::default().for_sync_into_existing(
        &alias_foo,
        &remote_ref,
        Ok(SyncResult::Updated {
            old_head: None,
            new_head: synced_head(),
            num_blocks: 0,
        }),
    );

    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;

    harness
        .copy_dataset_to_remote_repo(&foo.dataset_handle.id, &alias_foo.dataset_name)
        .await;

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));

    let pull_request = PullRequest::Remote(PullRequestRemote {
        remote_ref: remote_ref.clone(),
        maybe_local_alias: Some(alias_foo.clone()),
    });

    let pull_response = harness
        .use_case
        .execute(
            pull_request.clone(),
            PullOptions {
                add_aliases: false,
                ..PullOptions::default()
            },
            None,
        )
        .await
        .unwrap();

    assert_matches!(
        pull_response,
        PullResponse {
            maybe_original_request: Some(a_pull_request),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: Some(a_remote_ref),
            result: Ok(PullResult::Updated { .. }),
        } if a_pull_request == pull_request &&
        a_local_ref == foo.dataset_handle.as_local_ref() &&
        a_remote_ref == remote_ref,
    );

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_sync_head_moved_is_a_conflict() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let remote_ref = odf::DatasetRefRemote::Alias(odf::DatasetAliasRemote {
        repo_name: odf::RepoName::new_unchecked(REMOTE_REPO_NAME_STR),
        account_name: None,
        dataset_name: alias_foo.dataset_name.clone(),
    });

    let mocks = PullUseCaseHarnessMocks::default().for_sync_into_existing(
        &alias_foo,
        &remote_ref,
        Err(SyncError::HeadMoved(odf::dataset::RefCASError {
            reference: odf::BlockRef::Head,
            expected: None,
            actual: None,
        })),
    );

    let harness = PullUseCaseHarness::new(mocks);

    let foo = harness.create_root_dataset(&alias_foo).await;

    harness
        .copy_dataset_to_remote_repo(&foo.dataset_handle.id, &alias_foo.dataset_name)
        .await;

    let pull_response = harness
        .use_case
        .execute(
            PullRequest::Remote(PullRequestRemote {
                remote_ref,
                maybe_local_alias: Some(alias_foo),
            }),
            PullOptions::default(),
            None,
        )
        .await
        .unwrap();

    assert_matches!(
        pull_response.result,
        Err(PullError::SyncError(SyncError::HeadMoved(_)))
    );

    let aliases = harness.get_remote_aliases(&foo).await;
    assert!(aliases.is_empty(RemoteAliasKind::Pull));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_multi_recursive() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let alias_foo_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo-bar"));

    let mocks = PullUseCaseHarnessMocks::default()
        .with_authorizer_mock(
            MockDatasetActionAuthorizer::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
                    1,
                    HashSet::from_iter([alias_foo.clone(), alias_bar.clone(), alias_baz.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
                    1,
                    HashSet::from_iter([alias_foo_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Read,
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
        .with_authorizer_mock(
            MockDatasetActionAuthorizer::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
                    1,
                    HashSet::from_iter([alias_foo.clone(), alias_bar.clone(), alias_baz.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
                    1,
                    HashSet::from_iter([alias_foo_bar.clone(), alias_foo_baz.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Read,
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
async fn test_pull_authorization_issue() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let mocks = PullUseCaseHarnessMocks::default().with_authorizer_mock(
        MockDatasetActionAuthorizer::new().make_expect_classify_datasets_by_allowance(
            DatasetAction::Write,
            1,
            HashSet::from_iter([alias_foo.clone(), alias_baz.clone()]),
        ),
    );
    let harness = PullUseCaseHarness::new(mocks);

    let _foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let _baz = harness.create_root_dataset(&alias_baz).await;

    harness.index_dataset_dependencies().await;

    let mut pull_responses = harness
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
    assert_eq!(1, pull_responses.len());

    assert_matches!(
        pull_responses.remove(0),
        PullResponse {
            maybe_original_request: Some(PullRequest::Local(_)),
            maybe_local_ref: Some(a_local_ref),
            maybe_remote_ref: None,
            result: Err(PullError::Access(_))
        } if a_local_ref == bar.dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static REMOTE_REPO_NAME_STR: &str = "remote";

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct PullUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn PullDatasetUseCase>,
    dependency_graph_indexer: Arc<DependencyGraphIndexer>,
    remote_aliases_registry: Arc<dyn RemoteAliasesRegistry>,
    remote_repo_name: odf::RepoName,
    remote_tmp_dir: TempDir,
}

impl PullUseCaseHarness {
    fn new(mocks: PullUseCaseHarnessMocks) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_authorizer(Some(mocks.mock_dataset_action_authorizer)),
        );

        let repos_dir = base_use_case_harness.temp_dir_path().join("repos");
        std::fs::create_dir(&repos_dir).unwrap();

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<PullDatasetUseCaseImpl>()
            .add::<DatasetHeadCommitterImpl>()
            .add::<PullRequestPlannerImpl>()
            .add::<DatasetUpdateKindResolverImpl>()
            .add::<PollingIngestPlannerImpl>()
            .add_value(mocks.mock_polling_ingest_service)
            .bind::<dyn PollingIngestService, MockPollingIngestService>()
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
            .add::<RemoteAliasesRegistryImpl>()
            .add::<RemoteRepositoryRegistryImpl>()
            .add_value(RemoteReposDir::new(repos_dir))
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add_value(IpfsGateway::default())
            .add::<DependencyGraphServiceImpl>()
            .add::<InMemoryDatasetDependencyRepository>()
            .add::<DependencyGraphIndexer>();

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
            remote_repo_name,
            remote_tmp_dir,
        }
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

    /// Chains one unreferenced block per entry after the current `HEAD`, sets
    /// each entry to report the step onto its block, and returns the last one
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
        self.use_case
            .execute(PullRequest::Local(alias.as_local_ref()), options, None)
            .await
            .unwrap()
    }

    /// Pulls with a listener and returns the ingest events it received
    async fn pull_local_recording_ingest(
        &self,
        alias: &odf::DatasetAlias,
    ) -> (PullResponse, Vec<&'static str>) {
        let listener = Arc::new(RecordingIngestListener::default());
        let pull_response = self
            .use_case
            .execute(
                PullRequest::Local(alias.as_local_ref()),
                PullOptions::default(),
                Some(listener.clone()),
            )
            .await
            .unwrap();
        (pull_response, listener.events())
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PullUseCaseHarnessMocks {
    mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
    mock_polling_ingest_service: MockPollingIngestService,
    mock_transform_elaboration: MockTransformPlanner,
    mock_transform_execution_service: MockTransformExecutor,
    mock_compaction_executor: MockCompactionExecutor,
    mock_sync_service: MockSyncService,
    mock_local_sync_committer: MockLocalSyncCommitter,
    /// Transforms with the real planner and executor, through an engine that
    /// only advances the watermark to this value
    real_transform_watermark: Option<DateTime<Utc>>,
    /// Enables dataset environment variables with this resolver
    env_var_resolution: Option<EnvVarResolution>,
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
            .returning(|_, _, _, _, _, _| {
                Ok(PollingIngestResponse {
                    result: PollingIngestResult::UpToDate { uncacheable: false },
                    metadata_state: None,
                })
            });

        Self {
            env_var_resolution: Some(EnvVarResolution::Const),
            ..Self::for_ingest_iterations(alias_foo, vec![])
        }
        .with_polling_ingest_mock(mock_polling_ingest_service)
    }

    fn with_authorizer_mock(
        self,
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
    ) -> Self {
        Self {
            mock_dataset_action_authorizer,
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
            .with_authorizer_mock(
                MockDatasetActionAuthorizer::new().make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
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
            .with_authorizer_mock(
                MockDatasetActionAuthorizer::new().make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
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
        Self::default().with_authorizer_mock(
            MockDatasetActionAuthorizer::new()
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Write,
                    1,
                    HashSet::from_iter([alias_bar.clone()]),
                )
                .make_expect_classify_datasets_by_allowance(
                    DatasetAction::Read,
                    1,
                    HashSet::from_iter([alias_foo.clone()]),
                ),
        )
    }

    fn with_sync_mock(self, mock_sync_service: MockSyncService) -> Self {
        Self {
            mock_sync_service,
            ..self
        }
    }

    /// Expects one authorized pull of the existing `foo` from `remote_ref`,
    /// transferred up to [`synced_head`] and committed with `commit_result`
    fn for_sync_into_existing(
        self,
        alias_foo: &odf::DatasetAlias,
        remote_ref: &odf::DatasetRefRemote,
        commit_result: Result<SyncResult, SyncError>,
    ) -> Self {
        let synced_heads = ReportedHeads::default();
        synced_heads.set(&synced_head(), &synced_head());

        Self {
            mock_local_sync_committer: MockLocalSyncCommitter::new()
                .make_expect_commit(synced_head(), commit_result),
            ..self
        }
        .with_authorizer_mock(
            MockDatasetActionAuthorizer::new().make_expect_classify_datasets_by_allowance(
                DatasetAction::Write,
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

/// The head a mocked sync transfers up to
fn synced_head() -> odf::Multihash {
    odf::Multihash::from_multibase(
        "f16205603b882241c71351baf996d6dba7e3ddbd571457e93c1cd282bdc61f9fed5f2",
    )
    .unwrap()
}

impl Default for PullUseCaseHarnessMocks {
    fn default() -> Self {
        Self {
            mock_dataset_action_authorizer: MockDatasetActionAuthorizer::new(),
            mock_polling_ingest_service: MockPollingIngestService::new(),
            mock_transform_elaboration: MockTransformPlanner::new(),
            mock_transform_execution_service: MockTransformExecutor::new(),
            mock_compaction_executor: MockCompactionExecutor::new(),
            mock_sync_service: MockSyncService::new(),
            mock_local_sync_committer: MockLocalSyncCommitter::new(),
            real_transform_watermark: None,
            env_var_resolution: None,
        }
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

/// Records the ingest listener calls of a pull, in order
#[derive(Default)]
struct RecordingIngestListener {
    events: std::sync::Mutex<Vec<&'static str>>,
}

impl RecordingIngestListener {
    fn events(&self) -> Vec<&'static str> {
        self.events.lock().unwrap().clone()
    }

    fn record(&self, event: &'static str) {
        self.events.lock().unwrap().push(event);
    }
}

impl PullListener for RecordingIngestListener {
    fn get_ingest_listener(self: Arc<Self>) -> Option<Arc<dyn PollingIngestListener>> {
        Some(self)
    }

    fn get_transform_listener(self: Arc<Self>) -> Option<Arc<dyn TransformListener>> {
        None
    }

    fn get_sync_listener(self: Arc<Self>) -> Option<Arc<dyn SyncListener>> {
        None
    }
}

impl PollingIngestListener for RecordingIngestListener {
    fn begin(&self) {
        self.record("begin");
    }

    fn success(&self, _result: &PollingIngestResult) {
        self.record("success");
    }

    fn error(&self, _error: &PollingIngestError) {
        self.record("error");
    }

    fn no_polling_source(&self) {
        self.record("no_polling_source");
    }

    fn plan_error(&self, _error: &PollingIngestPlanError) {
        self.record("plan_error");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
