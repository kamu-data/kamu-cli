// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use bytes::Bytes;
use file_utils::MediaType;
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::*;
use kamu_accounts::{Account, AccountRepository, QuotaError};
use kamu_accounts_inmem::{InMemoryAccountQuotaEventStore, InMemoryAccountRepository};
use kamu_accounts_services::{AccountQuotaServiceImpl, AccountServiceImpl};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacServiceImpl,
};
use kamu_core::*;
use kamu_datasets::{
    CreateDatasetResult,
    DatasetHeadCommitter,
    DatasetPermission,
    MockDatasetPermissionChecker,
    ResolvedDataset,
};
use kamu_datasets_services::testing::RacingDatasetHeadCommitter;
use kamu_datasets_services::{
    AccountQuotaCheckerStorageImpl,
    DatasetStatisticsServiceImpl,
    QuotaDefaultsConfig,
};
use messaging_outbox::{DummyOutboxImpl, register_message_dispatcher};
use odf::metadata::testing::MetadataFactory;

use crate::utils::RecordingListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_ingest_data_source_not_found() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let create_dataset_result = harness.create_root_dataset(&alias_foo).await;

    let data_stream = std::io::Cursor::new("{}");

    assert_matches!(
        harness
            .ingest_data(
                ResolvedDataset::from_created(&create_dataset_result),
                DataSource::Stream(Box::new(data_stream)),
                PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: None,
                    expected_head: None,
                    skip_quota_check: false,
                }
            )
            .await,
        Err(PushIngestDataError::Planning(
            PushIngestPlanningError::SourceNotFound(_)
        ))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_ingest_data_denied_without_write() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::new().expect_denied_but_readable(
            DatasetPermission::Write,
            &dataset_id_foo,
            1,
        ),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    let head_before = harness.get_head(&foo).await;

    assert_matches!(
        harness
            .ingest_data(
                ResolvedDataset::from_created(&foo),
                DataSource::Stream(Box::new(std::io::Cursor::new("{}"))),
                PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: true,
                    media_type: None,
                    expected_head: None,
                    skip_quota_check: false,
                }
            )
            .await,
        Err(PushIngestDataError::Access(_))
    );
    assert_eq!(head_before, harness.get_head(&foo).await);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_from_json() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let create_dataset_result = harness
        .create_root_dataset_with_push_source(&alias_foo)
        .await;

    let data_stream = std::io::Cursor::new("{\"city\":\"foo\", \"population\":100}");

    assert_matches!(
        harness
            .ingest_data(
                ResolvedDataset::from_created(&create_dataset_result),
                DataSource::Stream(Box::new(data_stream)),
                PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: None,
                    expected_head: None,
                    skip_quota_check: false,
                }
            )
            .await,
        Ok(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_from_file() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let create_dataset_result = harness
        .create_root_dataset_with_push_source(&alias_foo)
        .await;

    let data_file_content = "{\"city\":\"foo\", \"population\":100}";

    let data_file_path = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(data_file_path.path(), data_file_content)
        .await
        .unwrap();

    let file = tokio::fs::File::open(data_file_path.path()).await.unwrap();
    let data_stream = Box::new(file);

    assert_matches!(
        harness
            .ingest_data(
                ResolvedDataset::from_created(&create_dataset_result),
                DataSource::Stream(data_stream),
                PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: true,
                    media_type: Some(MediaType::NDJSON.to_owned()),
                    expected_head: None,
                    skip_quota_check: false,
                }
            )
            .await,
        Ok(PushIngestResult::Updated { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_execute_multi() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let create_dataset_result = harness
        .create_root_dataset_with_push_source(&alias_foo)
        .await;

    let target_dataset = ResolvedDataset::from_created(&create_dataset_result);
    let initial_head = target_dataset
        .as_metadata_chain()
        .resolve_ref(&odf::BlockRef::Head)
        .await
        .unwrap();

    let data_sources = vec![
        DataSource::Buffer(Bytes::from_static(br#"{"city":"foo","population":100}"#)),
        DataSource::Buffer(Bytes::from_static(br#"{"city":"bar","population":200}"#)),
    ];

    let ingest_result = harness
        .ingest_multi_data(
            target_dataset.clone(),
            data_sources,
            PushIngestDataUseCaseOptions {
                source_name: None,
                source_event_time: None,
                is_ingest_from_upload: false,
                media_type: None,
                expected_head: Some(initial_head.clone()),
                skip_quota_check: false,
            },
        )
        .await
        .expect("multi ingest succeeds");

    match ingest_result {
        PushIngestResult::Updated {
            old_head,
            new_head,
            num_blocks,
            system_time: _,
        } => {
            assert_eq!(old_head, initial_head);
            assert_ne!(new_head, old_head);
            assert_eq!(num_blocks, 2);

            let current_head = target_dataset
                .as_metadata_chain()
                .resolve_ref(&odf::BlockRef::Head)
                .await
                .unwrap();
            assert_eq!(current_head, new_head);
        }
        PushIngestResult::UpToDate => panic!("multi ingest should produce new blocks"),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_upload_adds_push_source() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset_without_source(&alias_foo).await;
    let seed_head = harness.get_head(&foo).await;

    // The source block and the data block move `HEAD` together
    assert_matches!(
        harness.ingest_ndjson(&foo, None, true).await,
        Ok(PushIngestResult::Updated { old_head, num_blocks: 2, .. }) if old_head == seed_head
    );

    // The first ingest committed the source, so one not allowed to create it still finds it
    assert_matches!(
        harness.ingest_ndjson(&foo, None, false).await,
        Ok(PushIngestResult::Updated { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_upload_with_expected_head_adds_push_source() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset_without_source(&alias_foo).await;
    let seed_head = harness.get_head(&foo).await;

    assert_matches!(
        harness.ingest_ndjson(&foo, Some(seed_head.clone()), true).await,
        Ok(PushIngestResult::Updated { old_head, num_blocks: 2, .. }) if old_head == seed_head
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_empty_upload_reports_the_added_push_source() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset_without_source(&alias_foo).await;
    let seed_head = harness.get_head(&foo).await;

    let result = harness.ingest_empty_upload(&foo).await;

    let head = harness.get_head(&foo).await;
    assert_matches!(
        result,
        Ok(PushIngestResult::Updated { old_head, new_head, num_blocks: 1, .. })
            if old_head == seed_head && new_head == head
    );
    assert_matches!(
        harness.head_event(&foo).await,
        odf::MetadataEvent::AddPushSource(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_upload_with_old_event_time_adds_push_source() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset_without_source(&alias_foo).await;

    // The event time predates the dataset; the source block takes the server clock
    assert_matches!(
        harness
            .ingest_data(
                ResolvedDataset::from_created(&foo),
                PushIngestDataUseCaseHarness::ndjson_source(),
                PushIngestDataUseCaseOptions {
                    source_event_time: Some("2000-01-01T00:00:00Z".parse().unwrap()),
                    ..PushIngestDataUseCaseHarness::ndjson_options(None, true)
                },
            )
            .await,
        Ok(PushIngestResult::Updated { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_ingest_data_into_derivative_rejected() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_bar) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo, dataset_id_bar]),
    );

    harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![alias_foo.as_local_ref()])
        .await;

    for is_ingest_from_upload in [false, true] {
        assert_matches!(
            harness
                .ingest_ndjson(&bar, None, is_ingest_from_upload)
                .await,
            Err(PushIngestDataError::Planning(
                PushIngestPlanningError::InvalidDatasetKind(_)
            ))
        );
    }
    assert_eq!(harness.get_head(&bar).await, bar.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_head_moved_concurrently() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness
        .create_root_dataset_with_push_source(&alias_foo)
        .await;
    let planned_head = harness.get_head(&foo).await;

    let concurrent_head = harness.move_head_concurrently(&foo).await;

    assert_matches!(
        harness.ingest_ndjson(&foo, Some(planned_head.clone()), false).await,
        Err(PushIngestDataError::Execution(PushIngestError::CommitError(
            odf::dataset::CommitError::MetadataAppendError(
                odf::dataset::AppendError::RefCASFailed(odf::dataset::RefCASError {
                    expected,
                    actual,
                    ..
                })
            )
        ))) if expected == Some(planned_head) && actual == Some(concurrent_head.clone())
    );
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_ingest_data_upload_head_moved_before_push_source_commit() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset_without_source(&alias_foo).await;
    let seed_head = harness.get_head(&foo).await;

    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
    harness.move_head_before_next_commit(concurrent_head.clone());

    assert_matches!(
        harness.ingest_ndjson(&foo, None, true).await,
        Err(PushIngestDataError::Execution(PushIngestError::CommitError(
            odf::dataset::CommitError::MetadataAppendError(
                odf::dataset::AppendError::RefCASFailed(odf::dataset::RefCASError {
                    expected,
                    actual,
                    ..
                })
            )
        ))) if expected == Some(seed_head) && actual == Some(concurrent_head.clone())
    );
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_execute_multi_head_moved_concurrently() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness
        .create_root_dataset_with_push_source(&alias_foo)
        .await;
    let planned_head = harness.get_head(&foo).await;

    let concurrent_head = harness.move_head_concurrently(&foo).await;

    assert_matches!(
        harness
            .ingest_multi_data(
                ResolvedDataset::from_created(&foo),
                vec![
                    PushIngestDataUseCaseHarness::ndjson_source(),
                    PushIngestDataUseCaseHarness::ndjson_source(),
                ],
                PushIngestDataUseCaseHarness::ndjson_options(Some(planned_head), false),
            )
            .await,
        Err(PushIngestDataError::Execution(
            PushIngestError::CommitError(odf::dataset::CommitError::MetadataAppendError(
                odf::dataset::AppendError::RefCASFailed(_)
            ))
        ))
    );
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_named_source() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );
    let foo = harness
        .create_root_dataset_with_push_sources(&alias_foo, &["csv", "json"])
        .await;

    let ambiguous = harness.ingest_csv(&foo, None).await;
    let named = harness.ingest_csv(&foo, Some("csv")).await;
    let unknown = harness.ingest_csv(&foo, Some("xml")).await;

    assert_matches!(
        ambiguous,
        Err(PushIngestDataError::Planning(
            PushIngestPlanningError::SourceNotFound(_)
        ))
    );
    assert_matches!(named, Ok(PushIngestResult::Updated { .. }));
    assert_matches!(
        unknown,
        Err(PushIngestDataError::Planning(
            PushIngestPlanningError::SourceNotFound(_)
        ))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_empty_input_into_source_with_schema() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );
    let foo = harness
        .create_root_dataset_with_push_sources(&alias_foo, &["csv"])
        .await;

    let first = harness.ingest_empty(&foo).await;
    let second = harness.ingest_empty(&foo).await;

    // The first push only records the schema the source declares
    assert_matches!(first, Ok(PushIngestResult::Updated { .. }));
    assert_matches!(second, Ok(PushIngestResult::UpToDate));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_empty_input_into_source_without_schema() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );
    let foo = harness
        .create_root_dataset_with_push_source(&alias_foo)
        .await;

    let res = harness.ingest_empty(&foo).await;

    assert_matches!(res, Ok(PushIngestResult::UpToDate));
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_ingest_data_unsupported_media_type() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );
    let foo = harness
        .create_root_dataset_with_push_source(&alias_foo)
        .await;

    let res = harness
        .ingest_data(
            ResolvedDataset::from_created(&foo),
            PushIngestDataUseCaseHarness::ndjson_source(),
            PushIngestDataUseCaseOptions {
                media_type: Some(MediaType("application/x-unknown".to_string())),
                ..PushIngestDataUseCaseHarness::ndjson_options(None, false)
            },
        )
        .await;

    assert_matches!(
        res,
        Err(PushIngestDataError::Execution(
            PushIngestError::UnsupportedMediaType(_)
        ))
    );
    assert_eq!(harness.get_head(&foo).await, foo.head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_reports_success_after_commit() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness
        .create_root_dataset_with_push_source(&alias_foo)
        .await;

    let (result, events) = harness.ingest_ndjson_recording(&foo).await;

    assert_matches!(result, Ok(PushIngestResult::Updated { .. }));
    assert_eq!(events, vec!["begin", "success"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_head_moved_before_commit_reports_error_not_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetPermissionChecker::allowing(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness
        .create_root_dataset_with_push_source(&alias_foo)
        .await;
    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
    harness.move_head_before_next_commit(concurrent_head.clone());

    let (result, events) = harness.ingest_ndjson_recording(&foo).await;

    assert_matches!(
        result,
        Err(PushIngestDataError::Execution(
            PushIngestError::CommitError(_)
        ))
    );
    assert_eq!(events, vec!["begin", "error"]);
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_execute_multi_counts_staged_slices_towards_quota() {
    let harness = PushIngestDataUseCaseHarness::with_room_for_one_slice().await;
    let target = harness.create_quota_owner_dataset("foo").await;
    let initial_head = harness.get_head(&target).await;

    assert_matches!(
        harness.ingest_slices(&target, &["foo", "bar"]).await,
        Err(PushIngestDataError::Execution(
            PushIngestError::QuotaExceeded(QuotaError::Exceeded(_))
        ))
    );
    assert_eq!(harness.get_head(&target).await, initial_head);

    // Each slice fits on its own
    assert_matches!(
        harness.ingest_slices(&target, &["bar"]).await,
        Ok(PushIngestResult::Updated { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct PushIngestDataUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn PushIngestDataUseCase>,
    racing_committer: Arc<RacingDatasetHeadCommitter>,
    account_repo: Arc<dyn AccountRepository>,
}

impl PushIngestDataUseCaseHarness {
    const QUOTA_OWNER: &'static str = "alice";

    fn new(
        mock_dataset_permission_checker: MockDatasetPermissionChecker,
        mock_did_generator: MockDidGenerator,
    ) -> Self {
        Self::new_with(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_permission_checker(Some(mock_dataset_permission_checker))
                .with_maybe_mock_did_generator(Some(mock_did_generator)),
            QuotaDefaultsConfig::default(),
        )
    }

    /// Multi-tenant, where [`Self::QUOTA_OWNER`] has room for one slice of
    /// [`Self::slice_source`] but not for two
    async fn with_room_for_one_slice() -> Self {
        let probe = Self::new_for_quota_owner(QuotaDefaultsConfig::default()).await;
        let slice_size = probe.ingested_slice_size().await;

        Self::new_for_quota_owner(QuotaDefaultsConfig {
            storage: slice_size * 3 / 2,
        })
        .await
    }

    async fn new_for_quota_owner(quota_defaults: QuotaDefaultsConfig) -> Self {
        let harness = Self::new_with(
            BaseUseCaseHarnessOptions::new()
                .with_tenancy_config(TenancyConfig::MultiTenant)
                .with_maybe_permission_checker(Some(MockDatasetPermissionChecker::allowing())),
            quota_defaults,
        );

        harness
            .account_repo
            .save_account(&Account::test(
                odf::AccountID::new_seeded_ed25519(Self::QUOTA_OWNER.as_bytes()),
                Self::QUOTA_OWNER,
            ))
            .await
            .unwrap();

        harness
    }

    fn new_with(options: BaseUseCaseHarnessOptions, quota_defaults: QuotaDefaultsConfig) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(options.without_outbox());

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());

        kamu_datasets_inmem::register_dependencies(&mut b);
        b.add::<PushIngestDataUseCaseImpl>()
            .add_value(RacingDatasetHeadCommitter::default())
            .bind::<dyn DatasetHeadCommitter, RacingDatasetHeadCommitter>()
            .add::<PushIngestPlannerImpl>()
            .add::<PushIngestExecutorImpl>()
            .add::<DataFormatRegistryImpl>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add::<AccountServiceImpl>()
            .add::<InMemoryAccountRepository>()
            .add::<InMemoryAccountQuotaEventStore>()
            .add::<AccountQuotaServiceImpl>()
            .add::<DatasetStatisticsServiceImpl>()
            .add_value(quota_defaults)
            .add::<AccountQuotaCheckerStorageImpl>()
            .add::<RebacServiceImpl>()
            .add::<InMemoryRebacRepository>()
            .add_value(DefaultAccountProperties::default())
            .add_value(DefaultDatasetProperties::default())
            .add::<DummyOutboxImpl>()
            .add_value(EngineConfigDatafusionEmbeddedIngest::default())
            .add::<EngineProvisionerNull>();

        register_message_dispatcher::<kamu_datasets::DatasetExternallyChangedMessage>(
            &mut b,
            kamu_datasets::MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
        );

        let catalog = b.build();

        Self {
            base_use_case_harness,
            use_case: catalog.get_one().unwrap(),
            racing_committer: catalog.get_one().unwrap(),
            account_repo: catalog.get_one().unwrap(),
        }
    }

    fn move_head_before_next_commit(&self, concurrent_head: odf::Multihash) {
        self.racing_committer
            .move_head_before_next_commit(self.base_use_case_harness.catalog(), concurrent_head);
    }

    async fn ingest_data(
        &self,
        target: ResolvedDataset,
        data_source: DataSource,
        options: PushIngestDataUseCaseOptions,
    ) -> Result<PushIngestResult, PushIngestDataError> {
        self.use_case
            .execute(target, data_source, options, None)
            .await
    }

    async fn ingest_ndjson(
        &self,
        created: &CreateDatasetResult,
        expected_head: Option<odf::Multihash>,
        is_ingest_from_upload: bool,
    ) -> Result<PushIngestResult, PushIngestDataError> {
        self.ingest_data(
            ResolvedDataset::from_created(created),
            Self::ndjson_source(),
            Self::ndjson_options(expected_head, is_ingest_from_upload),
        )
        .await
    }

    async fn ingest_ndjson_recording(
        &self,
        created: &CreateDatasetResult,
    ) -> (
        Result<PushIngestResult, PushIngestDataError>,
        Vec<&'static str>,
    ) {
        let listener = Arc::new(RecordingListener::default());
        let result = self
            .use_case
            .execute(
                ResolvedDataset::from_created(created),
                Self::ndjson_source(),
                Self::ndjson_options(None, false),
                Some(listener.clone()),
            )
            .await;
        (result, listener.events())
    }

    fn ndjson_source() -> DataSource {
        DataSource::Stream(Box::new(std::io::Cursor::new(
            "{\"city\":\"foo\", \"population\":100}",
        )))
    }

    fn ndjson_options(
        expected_head: Option<odf::Multihash>,
        is_ingest_from_upload: bool,
    ) -> PushIngestDataUseCaseOptions {
        PushIngestDataUseCaseOptions {
            source_name: None,
            source_event_time: None,
            is_ingest_from_upload,
            media_type: Some(MediaType::NDJSON.to_owned()),
            expected_head,
            skip_quota_check: false,
        }
    }

    /// A root dataset with a CSV push source per name, each with `city` and
    /// `population` columns and a header row
    async fn create_root_dataset_with_push_sources(
        &self,
        alias: &odf::DatasetAlias,
        source_names: &[&str],
    ) -> CreateDatasetResult {
        let events = source_names
            .iter()
            .map(|name| {
                MetadataFactory::add_push_source()
                    .source_name(*name)
                    .read(odf::metadata::ReadStepCsv {
                        header: Some(true),
                        schema: Some(odf::schema::DataSchema::new(vec![
                            odf::schema::DataField::string("city"),
                            odf::schema::DataField::i64("population"),
                        ])),
                        ..odf::metadata::ReadStepCsv::default()
                    })
                    .build()
                    .into()
            })
            .collect();
        self.create_root_dataset_with_events(alias, events).await
    }

    async fn ingest_csv(
        &self,
        created: &CreateDatasetResult,
        source_name: Option<&str>,
    ) -> Result<PushIngestResult, PushIngestDataError> {
        self.ingest_data(
            ResolvedDataset::from_created(created),
            DataSource::Stream(Box::new(std::io::Cursor::new("city,population\nfoo,100\n"))),
            PushIngestDataUseCaseOptions {
                source_name: source_name.map(str::to_string),
                media_type: Some(MediaType::CSV.to_owned()),
                ..Self::ndjson_options(None, false)
            },
        )
        .await
    }

    /// Pushes a zero-length input without naming a source
    async fn ingest_empty(
        &self,
        created: &CreateDatasetResult,
    ) -> Result<PushIngestResult, PushIngestDataError> {
        self.ingest_data(
            ResolvedDataset::from_created(created),
            DataSource::Stream(Box::new(std::io::Cursor::new(""))),
            PushIngestDataUseCaseOptions {
                media_type: None,
                ..Self::ndjson_options(None, false)
            },
        )
        .await
    }

    /// Uploads a zero-length NDJSON file
    async fn ingest_empty_upload(
        &self,
        created: &CreateDatasetResult,
    ) -> Result<PushIngestResult, PushIngestDataError> {
        self.ingest_data(
            ResolvedDataset::from_created(created),
            DataSource::Stream(Box::new(std::io::Cursor::new(""))),
            Self::ndjson_options(None, true),
        )
        .await
    }

    async fn head_event(&self, created: &CreateDatasetResult) -> odf::MetadataEvent {
        let head = self.get_head(created).await;
        created
            .dataset
            .as_metadata_chain()
            .get_block(&head)
            .await
            .unwrap()
            .event
    }

    async fn ingest_multi_data(
        &self,
        target: ResolvedDataset,
        data_sources: Vec<DataSource>,
        options: PushIngestDataUseCaseOptions,
    ) -> Result<PushIngestResult, PushIngestDataError> {
        self.use_case
            .execute_multi(target, data_sources, options, None)
            .await
    }

    async fn create_quota_owner_dataset(&self, name: &str) -> CreateDatasetResult {
        let alias = odf::DatasetAlias::new(
            Some(odf::AccountName::new_unchecked(Self::QUOTA_OWNER)),
            odf::DatasetName::new_unchecked(name),
        );
        self.create_root_dataset_with_push_source(&alias).await
    }

    /// One record, so that every slice of it has about the same size
    fn slice_source(city: &'static str) -> DataSource {
        DataSource::Buffer(Bytes::from(format!(
            r#"{{"city":"{city}","population":100}}"#
        )))
    }

    async fn ingest_slices(
        &self,
        target: &CreateDatasetResult,
        cities: &[&'static str],
    ) -> Result<PushIngestResult, PushIngestDataError> {
        self.ingest_multi_data(
            ResolvedDataset::from_created(target),
            cities.iter().copied().map(Self::slice_source).collect(),
            PushIngestDataUseCaseOptions {
                media_type: None,
                ..Self::ndjson_options(None, false)
            },
        )
        .await
    }

    /// Size of the data file one slice of [`Self::slice_source`] adds
    async fn ingested_slice_size(&self) -> u64 {
        let target = self.create_quota_owner_dataset("probe").await;
        self.ingest_slices(&target, &["foo"]).await.unwrap();

        let block = target
            .dataset
            .as_metadata_chain()
            .get_block(&self.get_head(&target).await)
            .await
            .unwrap();
        let odf::MetadataEvent::AddData(add_data) = block.event else {
            panic!("Expected AddData, got: {block:?}");
        };
        add_data.new_data.unwrap().size
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
