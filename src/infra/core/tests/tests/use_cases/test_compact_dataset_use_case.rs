// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::*;
use kamu_core::testing::ReportedHeads;
use kamu_core::*;
use kamu_datasets::{DatasetAction, MockDatasetActionAuthorizer, ResolvedDataset};
use kamu_datasets_services::DatasetHeadCommitterImpl;
use pretty_assertions::assert_matches;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compact_dataset_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = CompactUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_authorize_for(
            DatasetAction::Maintain,
            &dataset_id_foo,
            1,
            true,
        ),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;

    assert_matches!(
        harness
            .compact_dataset(ResolvedDataset::from_created(&foo))
            .await,
        Ok(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compact_multiple_datasets_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_bar) = odf::DatasetID::new_generated_ed25519();

    let harness = CompactUseCaseHarness::new(
        MockDatasetActionAuthorizer::new()
            .expect_authorize_for(DatasetAction::Maintain, &dataset_id_foo, 1, true)
            .expect_authorize_for(DatasetAction::Maintain, &dataset_id_bar, 1, true),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo, dataset_id_bar]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;

    let mut responses = harness
        .compact_datasets(vec![
            ResolvedDataset::from_created(&foo),
            ResolvedDataset::from_created(&bar),
        ])
        .await;

    assert_eq!(responses.len(), 2);
    let response_bar = responses.remove(1);
    let response_foo = responses.remove(0);

    assert_matches!(
        response_foo,
        CompactionResponse {
            result: Ok(_),
             dataset_ref
        } if dataset_ref == foo.dataset_handle.as_local_ref());
    assert_matches!(
        response_bar,
        CompactionResponse {
            result: Ok(_),
            dataset_ref
        } if dataset_ref == bar.dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_try_compact_not_found_dataset() {
    let not_found_dataset_handle =
        odf::metadata::testing::handle(&"user", &"not-found-dataset", odf::DatasetKind::Root);

    let harness = CompactUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_authorize_for_not_found(
            DatasetAction::Maintain,
            &not_found_dataset_handle.id,
            1,
        ),
        MockDidGenerator::new(),
    );

    assert_matches!(
        harness
            .use_case
            .execute(
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
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = CompactUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_authorize_for(
            DatasetAction::Maintain,
            &dataset_id_foo,
            1,
            false,
        ),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;

    assert_matches!(
        harness
            .compact_dataset(ResolvedDataset::from_created(&foo))
            .await,
        Err(CompactionError::Access(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_dataset_to_metadata_requires_maintain() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = CompactUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_authorize_for(
            DatasetAction::Maintain,
            &dataset_id_foo,
            1,
            false,
        ),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;

    assert_matches!(
        harness
            .reset_dataset_to_metadata(ResolvedDataset::from_created(&foo))
            .await,
        Err(CompactionError::Access(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compact_dataset_mixed_authorization_outcome() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_bar) = odf::DatasetID::new_generated_ed25519();

    let harness = CompactUseCaseHarness::new(
        MockDatasetActionAuthorizer::new()
            .expect_authorize_for(DatasetAction::Maintain, &dataset_id_foo, 1, false)
            .expect_authorize_for(DatasetAction::Maintain, &dataset_id_bar, 1, true),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo, dataset_id_bar]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;

    let mut responses = harness
        .compact_datasets(vec![
            ResolvedDataset::from_created(&foo),
            ResolvedDataset::from_created(&bar),
        ])
        .await;

    assert_eq!(responses.len(), 2);
    let response_bar = responses.remove(1);
    let response_foo = responses.remove(0);

    assert_matches!(
        response_foo,
        CompactionResponse {
            result: Err(CompactionError::Access(_)),
             dataset_ref
        } if dataset_ref == foo.dataset_handle.as_local_ref());
    assert_matches!(
        response_bar,
        CompactionResponse {
            result: Ok(_),
            dataset_ref
        } if dataset_ref == bar.dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compact_dataset_commits_head_and_reports_breaking() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let reported_heads = ReportedHeads::default();

    let harness = CompactUseCaseHarness::with_compaction_executor(
        MockDatasetActionAuthorizer::new().expect_authorize_for(
            DatasetAction::Maintain,
            &dataset_id_foo,
            1,
            true,
        ),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
        MockCompactionExecutor::new()
            .make_expect_compaction_success(alias_foo.clone(), reported_heads.clone()),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let compacted_head = harness.append_unreferenced_block(&foo, "compacted").await;
    reported_heads.set(&foo.head, &compacted_head);

    assert_matches!(
        harness
            .compact_dataset(ResolvedDataset::from_created(&foo))
            .await,
        Ok(CompactionResult::Success {
            old_head,
            new_head,
            breaking: true,
            ..
        }) if old_head == foo.head && new_head == compacted_head
    );
    assert_eq!(harness.get_head(&foo).await, compacted_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_dataset_to_metadata_commits_head() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let reported_heads = ReportedHeads::default();

    let harness = CompactUseCaseHarness::with_compaction_executor(
        MockDatasetActionAuthorizer::new().expect_authorize_for(
            DatasetAction::Maintain,
            &dataset_id_foo,
            1,
            true,
        ),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
        MockCompactionExecutor::new()
            .make_expect_compaction_success(alias_foo.clone(), reported_heads.clone()),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let reset_head = harness.append_unreferenced_block(&foo, "reset").await;
    reported_heads.set(&foo.head, &reset_head);

    assert_matches!(
        harness
            .reset_dataset_to_metadata(ResolvedDataset::from_created(&foo))
            .await,
        Ok(CompactionResult::Success {
            new_head,
            breaking: true,
            ..
        }) if new_head == reset_head
    );
    assert_eq!(harness.get_head(&foo).await, reset_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compact_dataset_head_moved_concurrently() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let reported_heads = ReportedHeads::default();

    let harness = CompactUseCaseHarness::with_compaction_executor(
        MockDatasetActionAuthorizer::new().expect_authorize_for(
            DatasetAction::Maintain,
            &dataset_id_foo,
            1,
            true,
        ),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
        MockCompactionExecutor::new()
            .make_expect_compaction_success(alias_foo.clone(), reported_heads.clone()),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;
    let compacted_head = harness.append_unreferenced_block(&foo, "compacted").await;
    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
    reported_heads.set(&foo.head, &compacted_head);
    harness.move_head(&foo, &foo.head, &concurrent_head).await;

    assert_matches!(
        harness
            .compact_dataset(ResolvedDataset::from_created(&foo))
            .await,
        Err(CompactionError::RefCASFailed(odf::dataset::RefCASError {
            expected: Some(expected),
            actual: Some(actual),
            ..
        })) if expected == foo.head && actual == concurrent_head
    );
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct CompactUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn CompactDatasetUseCase>,
}

impl CompactUseCaseHarness {
    fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_did_generator: MockDidGenerator,
    ) -> Self {
        Self::build(mock_dataset_action_authorizer, mock_did_generator, None)
    }

    fn with_compaction_executor(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_did_generator: MockDidGenerator,
        mock_compaction_executor: MockCompactionExecutor,
    ) -> Self {
        Self::build(
            mock_dataset_action_authorizer,
            mock_did_generator,
            Some(mock_compaction_executor),
        )
    }

    fn build(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_did_generator: MockDidGenerator,
        maybe_mock_compaction_executor: Option<MockCompactionExecutor>,
    ) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_authorizer(Some(mock_dataset_action_authorizer))
                .with_maybe_mock_did_generator(Some(mock_did_generator)),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<CompactDatasetUseCaseImpl>()
            .add::<CompactionPlannerImpl>()
            .add_value(EngineConfigDatafusionEmbeddedCompaction::default())
            .add::<ObjectStoreRegistryImpl>()
            .add::<DatasetHeadCommitterImpl>();
        if let Some(mock_compaction_executor) = maybe_mock_compaction_executor {
            b.add_value(mock_compaction_executor)
                .bind::<dyn CompactionExecutor, MockCompactionExecutor>();
        } else {
            b.add::<CompactionExecutorImpl>();
        }
        database_common::NoOpDatabasePlugin::init_database_components(&mut b);
        let catalog = b.build();

        let use_case = catalog.get_one::<dyn CompactDatasetUseCase>().unwrap();

        Self {
            base_use_case_harness,
            use_case,
        }
    }

    async fn compact_dataset(
        &self,
        target: ResolvedDataset,
    ) -> Result<CompactionResult, CompactionError> {
        self.compact_dataset_with(target, CompactionOptions::default())
            .await
    }

    async fn reset_dataset_to_metadata(
        &self,
        target: ResolvedDataset,
    ) -> Result<CompactionResult, CompactionError> {
        self.compact_dataset_with(
            target,
            CompactionOptions {
                keep_metadata_only: true,
                ..CompactionOptions::default()
            },
        )
        .await
    }

    async fn compact_dataset_with(
        &self,
        target: ResolvedDataset,
        options: CompactionOptions,
    ) -> Result<CompactionResult, CompactionError> {
        self.use_case
            .execute(target.get_handle(), options, None)
            .await
    }

    async fn compact_datasets(&self, targets: Vec<ResolvedDataset>) -> Vec<CompactionResponse> {
        let handles: Vec<_> = targets
            .into_iter()
            .map(ResolvedDataset::take_handle)
            .collect();

        self.use_case
            .execute_multi(handles, CompactionOptions::default(), None)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
