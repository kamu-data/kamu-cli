// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;
use std::assert_matches::assert_matches;
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
        Multihash::from_multibase_str("zW1a3CNT52HXiJNniLkWMeev3CPRy9QiNRMWGyTrVNg4hY8").unwrap();

    let result = harness
        .reset_svc
        .reset_dataset(&test_case.dataset_handle, &a_hash_not_present_in_chain)
        .await;
    assert_matches!(result, Err(ResetError::BlockNotFound(_)));
}

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

struct ResetTestHarness {
    _temp_dir: TempDir,
    local_repo: Arc<DatasetRepositoryLocalFs>,
    reset_svc: ResetServiceImpl,
}

impl ResetTestHarness {
    fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let workspace_layout = Arc::new(WorkspaceLayout::create(temp_dir.path()).unwrap());
        let local_repo = Arc::new(DatasetRepositoryLocalFs::new(workspace_layout));

        let reset_svc = ResetServiceImpl::new(local_repo.clone());

        Self {
            _temp_dir: temp_dir,
            local_repo,
            reset_svc,
        }
    }

    async fn a_chain_with_2_blocks(&self) -> ChainWith2BlocksTestCase {
        let dataset_name = DatasetName::try_from("foo").unwrap();
        let mut dataset_builder = self.a_dataset_builder(&dataset_name).await;

        let chain = dataset_builder.as_dataset().as_metadata_chain();
        let (hash_seed_block, seed_block_sequence_number) =
            self.a_seed_block(chain, &dataset_name).await;
        let hash_polling_source_block = self
            .a_polling_source_block(chain, &hash_seed_block, seed_block_sequence_number)
            .await;

        let dataset_handle = dataset_builder.finish().await.unwrap();

        ChainWith2BlocksTestCase::new(dataset_handle, hash_seed_block, hash_polling_source_block)
    }

    async fn a_dataset_builder(&self, dataset_name: &DatasetName) -> Box<dyn DatasetBuilder> {
        self.local_repo
            .create_dataset(&DatasetAlias::new(None, dataset_name.clone()))
            .await
            .unwrap()
    }

    async fn a_seed_block(
        &self,
        chain: &dyn MetadataChain,
        dataset_name: &DatasetName,
    ) -> (Multihash, i32) {
        let seed_block = MetadataFactory::metadata_block(
            MetadataFactory::seed(DatasetKind::Root)
                .id_from(dataset_name.as_str())
                .build(),
        )
        .build();
        let seed_block_sequence_number = seed_block.sequence_number;

        (
            chain
                .append(seed_block, AppendOpts::default())
                .await
                .unwrap(),
            seed_block_sequence_number,
        )
    }

    async fn a_polling_source_block(
        &self,
        chain: &dyn MetadataChain,
        prev_block_hash: &Multihash,
        prev_sequence_number: i32,
    ) -> Multihash {
        chain
            .append(
                MetadataFactory::metadata_block(MetadataFactory::set_polling_source().build())
                    .prev(prev_block_hash, prev_sequence_number)
                    .build(),
                AppendOpts::default(),
            )
            .await
            .unwrap()
    }

    async fn get_dataset_head(&self, dataset_handle: &DatasetHandle) -> Multihash {
        let dataset = self.resolve_dataset(dataset_handle).await;
        dataset
            .as_metadata_chain()
            .get_ref(&BlockRef::Head)
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
        self.local_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await
            .unwrap()
    }
}
