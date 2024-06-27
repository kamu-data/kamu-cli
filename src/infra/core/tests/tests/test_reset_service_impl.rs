// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use dill::Component;
use event_bus::EventBus;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use opendatafabric::*;
use tempfile::TempDir;

#[test_log::test(tokio::test)]
async fn test_reset_dataset_with_2revisions_drop_last() {
    let harness = ResetTestHarness::new();
    let test_case = harness.a_chain_with_2_blocks().await;

    let current_head = harness.get_dataset_head(&test_case.dataset_handle).await;
    assert_eq!(test_case.hash_polling_source_block, current_head);

    let result = harness
        .reset_svc
        .reset_dataset(&test_case.dataset_handle, &test_case.hash_seed_block)
        .await;
    assert!(result.is_ok());

    let new_head = harness.get_dataset_head(&test_case.dataset_handle).await;
    assert_eq!(test_case.hash_seed_block, new_head);

    let summary = harness.get_dataset_summary(&test_case.dataset_handle).await;
    assert_eq!(new_head, summary.last_block_hash);
}

#[test_log::test(tokio::test)]
async fn test_reset_dataset_with_2revisions_without_changes() {
    let harness = ResetTestHarness::new();
    let test_case = harness.a_chain_with_2_blocks().await;

    let current_head = harness.get_dataset_head(&test_case.dataset_handle).await;
    assert_eq!(test_case.hash_polling_source_block, current_head);

    let result = harness
        .reset_svc
        .reset_dataset(
            &test_case.dataset_handle,
            &test_case.hash_polling_source_block,
        )
        .await;
    assert!(result.is_ok());

    let new_head = harness.get_dataset_head(&test_case.dataset_handle).await;
    assert_eq!(current_head, new_head);

    let summary = harness.get_dataset_summary(&test_case.dataset_handle).await;
    assert_eq!(current_head, summary.last_block_hash);
}

#[test_log::test(tokio::test)]
async fn test_reset_dataset_to_non_existing_block_fails() {
    let harness = ResetTestHarness::new();
    let test_case = harness.a_chain_with_2_blocks().await;

    let a_hash_not_present_in_chain =
        Multihash::from_multibase("zW1a3CNT52HXiJNniLkWMeev3CPRy9QiNRMWGyTrVNg4hY8").unwrap();

    let result = harness
        .reset_svc
        .reset_dataset(&test_case.dataset_handle, &a_hash_not_present_in_chain)
        .await;
    assert_matches!(result, Err(ResetError::BlockNotFound(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ChainWith2BlocksTestCase {
    dataset_handle: DatasetHandle,
    hash_seed_block: Multihash,
    hash_polling_source_block: Multihash,
}

impl ChainWith2BlocksTestCase {
    fn new(
        dataset_handle: DatasetHandle,
        hash_seed_block: Multihash,
        hash_polling_source_block: Multihash,
    ) -> Self {
        Self {
            dataset_handle,
            hash_seed_block,
            hash_polling_source_block,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ResetTestHarness {
    _temp_dir: TempDir,
    dataset_repo: Arc<dyn DatasetRepository>,
    reset_svc: Arc<dyn ResetService>,
}

impl ResetTestHarness {
    fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<SystemTimeSourceDefault>()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(MockDatasetActionAuthorizer::new().expect_check_write_a_dataset(1))
            .bind::<dyn auth::DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add::<ResetServiceImpl>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
        let reset_svc = catalog.get_one::<dyn ResetService>().unwrap();

        Self {
            _temp_dir: tempdir,
            dataset_repo,
            reset_svc,
        }
    }

    async fn a_chain_with_2_blocks(&self) -> ChainWith2BlocksTestCase {
        let dataset_name = DatasetName::try_from("foo").unwrap();

        let seed_block = MetadataFactory::metadata_block(
            MetadataFactory::seed(DatasetKind::Root)
                .id_from(dataset_name.as_str())
                .build(),
        )
        .build_typed();

        let create_result = self
            .dataset_repo
            .create_dataset(&DatasetAlias::new(None, dataset_name.clone()), seed_block)
            .await
            .unwrap();

        let dataset_handle = create_result.dataset_handle;
        let hash_seed_block = create_result.head;
        let hash_polling_source_block = create_result
            .dataset
            .commit_event(
                MetadataEvent::SetPollingSource(MetadataFactory::set_polling_source().build()),
                CommitOpts::default(),
            )
            .await
            .unwrap()
            .new_head;

        ChainWith2BlocksTestCase::new(dataset_handle, hash_seed_block, hash_polling_source_block)
    }

    async fn get_dataset_head(&self, dataset_handle: &DatasetHandle) -> Multihash {
        let dataset = self.resolve_dataset(dataset_handle).await;
        dataset
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
            .await
            .unwrap()
    }

    async fn get_dataset_summary(&self, dataset_handle: &DatasetHandle) -> DatasetSummary {
        let dataset = self.resolve_dataset(dataset_handle).await;
        dataset
            .get_summary(GetSummaryOpts::default())
            .await
            .unwrap()
    }

    async fn resolve_dataset(&self, dataset_handle: &DatasetHandle) -> Arc<dyn Dataset> {
        self.dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await
            .unwrap()
    }
}
