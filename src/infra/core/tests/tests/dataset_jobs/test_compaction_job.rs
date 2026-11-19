// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::testing::{BaseRepoHarness, BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::*;
use kamu_core::testing::ReportedHeads;
use kamu_core::*;
use kamu_datasets::{
    CreateDatasetResult,
    DatasetPermission,
    MockDatasetPermissionChecker,
    ResolvedDataset,
};
use kamu_datasets_services::DatasetReferenceUpdaterImpl;
use pretty_assertions::assert_matches;

use crate::utils::RecordingListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_try_compact_not_found_dataset() {
    let not_found_dataset_handle =
        odf::metadata::testing::handle(&"user", &"not-found-dataset", odf::DatasetKind::Root);

    let harness = CompactionJobHarness::new(
        MockDatasetPermissionChecker::new().expect_authorize_for_not_found(
            DatasetPermission::Maintain,
            &not_found_dataset_handle.id,
            1,
        ),
        MockDidGenerator::new(),
    );

    assert_matches!(
        harness
            .job
            .run(
                &not_found_dataset_handle,
                CompactionOptions::default(),
                None
            )
            .await,
        Err(CompactionError::NotFound(e))
            if e.dataset_ref == not_found_dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compact_dataset_unauthorized() {
    for (operation, options) in CompactionJobHarness::compact_and_reset_to_metadata() {
        let alias_foo = BaseRepoHarness::alias("foo");
        let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

        let harness = CompactionJobHarness::new(
            MockDatasetPermissionChecker::new().expect_denied_but_readable(
                DatasetPermission::Maintain,
                &dataset_id_foo,
                1,
            ),
            MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
        );

        let foo = harness.create_root_dataset(&alias_foo).await;

        assert_matches!(
            harness
                .compact_dataset_with(ResolvedDataset::from_created(&foo), options)
                .await,
            Err(CompactionError::Access(_)),
            "{operation}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compact_dataset_commits_head() {
    for (operation, options) in CompactionJobHarness::compact_and_reset_to_metadata() {
        let (harness, foo, compacted_head) =
            CompactionJobHarness::new_compacting_new_root(&BaseRepoHarness::alias("foo")).await;

        let (result, events) = harness
            .compact_dataset_recording(ResolvedDataset::from_created(&foo), options)
            .await;
        assert_matches!(
            result,
            Ok(CompactionResult::Success {
                old_head,
                new_head,
                ..
            }) if old_head == foo.head && new_head == compacted_head,
            "{operation}"
        );
        assert_eq!(harness.get_head(&foo).await, compacted_head, "{operation}");
        assert_eq!(events, vec!["begin", "success"], "{operation}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compact_dataset_ref_moved_concurrently() {
    let (harness, foo, _) =
        CompactionJobHarness::new_compacting_new_root(&BaseRepoHarness::alias("foo")).await;
    let concurrent_head = harness.move_head_concurrently(&foo).await;

    let (result, events) = harness
        .compact_dataset_recording(
            ResolvedDataset::from_created(&foo),
            CompactionOptions::default(),
        )
        .await;
    assert_matches!(
        result,
        Err(CompactionError::RefCASFailed(odf::dataset::RefCASError {
            expected: Some(expected),
            actual: Some(actual),
            ..
        })) if expected == foo.head && actual == concurrent_head
    );
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
    // The executor succeeded, but nothing is reported as compacted before the
    // new `HEAD` is set
    assert_eq!(events, vec!["begin", "error"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct CompactionJobHarness {
    base_use_case_harness: BaseUseCaseHarness,
    job: Arc<dyn CompactionJob>,
}

impl CompactionJobHarness {
    fn new(
        mock_dataset_permission_checker: MockDatasetPermissionChecker,
        mock_did_generator: MockDidGenerator,
    ) -> Self {
        Self::build(mock_dataset_permission_checker, mock_did_generator, None)
    }

    fn with_compaction_executor(
        mock_dataset_permission_checker: MockDatasetPermissionChecker,
        mock_did_generator: MockDidGenerator,
        mock_compaction_executor: MockCompactionExecutor,
    ) -> Self {
        Self::build(
            mock_dataset_permission_checker,
            mock_did_generator,
            Some(mock_compaction_executor),
        )
    }

    /// Creates root `alias`, permitted to maintain, and expects one
    /// compaction of it reporting a new head, appended unreferenced; returns
    /// the dataset and that head
    async fn new_compacting_new_root(
        alias: &odf::DatasetAlias,
    ) -> (Self, CreateDatasetResult, odf::Multihash) {
        let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
        let reported_heads = ReportedHeads::default();
        let harness = Self::with_compaction_executor(
            MockDatasetPermissionChecker::new().expect_authorize_for(
                DatasetPermission::Maintain,
                &dataset_id,
                1,
                true,
            ),
            MockDidGenerator::predefined_dataset_ids(vec![dataset_id]),
            MockCompactionExecutor::new()
                .make_expect_compaction_success(alias.clone(), reported_heads.clone()),
        );
        let created = harness.create_root_dataset(alias).await;
        let compacted_head = harness
            .append_unreferenced_block(&created, "compacted")
            .await;
        reported_heads.set(&created.head, &compacted_head);
        (harness, created, compacted_head)
    }

    fn build(
        mock_dataset_permission_checker: MockDatasetPermissionChecker,
        mock_did_generator: MockDidGenerator,
        maybe_mock_compaction_executor: Option<MockCompactionExecutor>,
    ) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_permission_checker(Some(mock_dataset_permission_checker))
                .with_maybe_mock_did_generator(Some(mock_did_generator)),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<CompactionJobImpl>()
            .add::<CompactionPlannerImpl>()
            .add_value(EngineConfigDatafusionEmbeddedCompaction::default())
            .add::<ObjectStoreRegistryImpl>()
            .add::<DatasetReferenceUpdaterImpl>();
        if let Some(mock_compaction_executor) = maybe_mock_compaction_executor {
            b.add_value(mock_compaction_executor)
                .bind::<dyn CompactionExecutor, MockCompactionExecutor>();
        } else {
            b.add::<CompactionExecutorImpl>();
        }
        let catalog = b.build();

        let job = catalog.get_one::<dyn CompactionJob>().unwrap();

        Self {
            base_use_case_harness,
            job,
        }
    }

    /// Both operations the job runs: a compaction and a reset to
    /// metadata only
    fn compact_and_reset_to_metadata() -> [(&'static str, CompactionOptions); 2] {
        [
            ("compact", CompactionOptions::default()),
            (
                "reset to metadata",
                CompactionOptions {
                    keep_metadata_only: true,
                    ..CompactionOptions::default()
                },
            ),
        ]
    }

    async fn compact_dataset_with(
        &self,
        target: ResolvedDataset,
        options: CompactionOptions,
    ) -> Result<CompactionResult, CompactionError> {
        self.job.run(target.get_handle(), options, None).await
    }

    async fn compact_dataset_recording(
        &self,
        target: ResolvedDataset,
        options: CompactionOptions,
    ) -> (Result<CompactionResult, CompactionError>, Vec<&'static str>) {
        let listener = Arc::new(RecordingListener::default());
        let result = self
            .job
            .run(target.get_handle(), options, Some(listener.clone()))
            .await;
        (result, listener.events())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
