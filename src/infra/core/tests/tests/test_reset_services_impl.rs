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

use kamu::domain::*;
use kamu::testing::{BaseRepoHarness, *};
use kamu::*;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reset_dataset_with_2revisions_drop_last() {
    let harness = ResetTestHarness::new();
    let test_case = harness.a_chain_with_2_blocks().await;

    let current_head = harness.get_dataset_head(&test_case.dataset_handle).await;
    assert_eq!(test_case.hash_polling_source_block, current_head);

    let result = harness
        .reset_dataset(
            &test_case.dataset_handle,
            Some(&test_case.hash_seed_block),
            None,
        )
        .await;
    assert!(result.is_ok());

    let new_head = harness.get_dataset_head(&test_case.dataset_handle).await;
    assert_eq!(test_case.hash_seed_block, new_head);

    let summary = harness.get_dataset_summary(&test_case.dataset_handle).await;
    assert_eq!(new_head, summary.last_block_hash);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reset_dataset_with_2revisions_without_changes() {
    let harness = ResetTestHarness::new();
    let test_case = harness.a_chain_with_2_blocks().await;

    let current_head = harness.get_dataset_head(&test_case.dataset_handle).await;
    assert_eq!(test_case.hash_polling_source_block, current_head);

    let result = harness
        .reset_dataset(
            &test_case.dataset_handle,
            Some(&test_case.hash_polling_source_block),
            None,
        )
        .await;
    assert!(result.is_ok());

    let new_head = harness.get_dataset_head(&test_case.dataset_handle).await;
    assert_eq!(current_head, new_head);

    let summary = harness.get_dataset_summary(&test_case.dataset_handle).await;
    assert_eq!(current_head, summary.last_block_hash);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reset_dataset_to_non_existing_block_fails() {
    let harness = ResetTestHarness::new();
    let test_case = harness.a_chain_with_2_blocks().await;

    let a_hash_not_present_in_chain =
        Multihash::from_multibase("zW1a3CNT52HXiJNniLkWMeev3CPRy9QiNRMWGyTrVNg4hY8").unwrap();

    let result = harness
        .reset_dataset(
            &test_case.dataset_handle,
            Some(&a_hash_not_present_in_chain),
            None,
        )
        .await;
    assert_matches!(
        result,
        Err(ResetError::Execution(
            ResetExecutionError::SetReferenceFailed(SetRefError::BlockNotFound(_))
        ))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reset_dataset_with_wrong_head() {
    let harness = ResetTestHarness::new();
    let test_case = harness.a_chain_with_2_blocks().await;

    let result = harness
        .reset_dataset(
            &test_case.dataset_handle,
            Some(&test_case.hash_seed_block),
            Some(&test_case.hash_seed_block),
        )
        .await;
    assert_matches!(
        result,
        Err(ResetError::Planning(ResetPlanningError::OldHeadMismatch(_)))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reset_dataset_with_default_seed_block() {
    let harness = ResetTestHarness::new();
    let test_case = harness.a_chain_with_2_blocks().await;

    let current_head = harness.get_dataset_head(&test_case.dataset_handle).await;
    assert_eq!(test_case.hash_polling_source_block, current_head);

    let result = harness
        .reset_dataset(
            &test_case.dataset_handle,
            None,
            Some(&test_case.hash_polling_source_block),
        )
        .await;
    assert!(result.is_ok());

    let new_head = harness.get_dataset_head(&test_case.dataset_handle).await;
    assert_eq!(test_case.hash_seed_block, new_head);

    let summary = harness.get_dataset_summary(&test_case.dataset_handle).await;
    assert_eq!(new_head, summary.last_block_hash);
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

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct ResetTestHarness {
    base_repo_harness: BaseRepoHarness,
    reset_planner: Arc<dyn ResetPlanner>,
    reset_executor: Arc<dyn ResetExecutor>,
}

impl ResetTestHarness {
    fn new() -> Self {
        let base_repo_harness = BaseRepoHarness::new(TenancyConfig::SingleTenant, None);

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<ResetPlannerImpl>()
            .add::<ResetExecutorImpl>()
            .build();

        let reset_planner = catalog.get_one::<dyn ResetPlanner>().unwrap();
        let reset_executor = catalog.get_one::<dyn ResetExecutor>().unwrap();

        Self {
            base_repo_harness,
            reset_planner,
            reset_executor,
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
            .dataset_repo_writer()
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

    async fn reset_dataset(
        &self,
        dataset_handle: &DatasetHandle,
        block_hash: Option<&Multihash>,
        old_head_maybe: Option<&Multihash>,
    ) -> Result<ResetResult, ResetError> {
        let target = self.resolve_dataset(dataset_handle);

        let reset_plan = self
            .reset_planner
            .plan_reset(target.clone(), block_hash, old_head_maybe)
            .await?;

        let reset_result = self.reset_executor.execute(target, reset_plan).await?;

        Ok(reset_result)
    }

    async fn get_dataset_head(&self, dataset_handle: &DatasetHandle) -> Multihash {
        let resolved_dataset = self.resolve_dataset(dataset_handle);
        resolved_dataset
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
            .await
            .unwrap()
    }

    async fn get_dataset_summary(&self, dataset_handle: &DatasetHandle) -> DatasetSummary {
        let resolved_dataset = self.resolve_dataset(dataset_handle);
        resolved_dataset
            .get_summary(GetSummaryOpts::default())
            .await
            .unwrap()
    }

    fn resolve_dataset(&self, dataset_handle: &DatasetHandle) -> ResolvedDataset {
        self.dataset_registry()
            .get_dataset_by_handle(dataset_handle)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
