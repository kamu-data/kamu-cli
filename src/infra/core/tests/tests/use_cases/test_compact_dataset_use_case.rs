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

use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::*;
use kamu_core::*;
use kamu_datasets::ResolvedDataset;
use kamu_datasets_services::testing::MockDatasetActionAuthorizer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_compact_dataset_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = CompactUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_maintain_dataset(&dataset_id_foo, 1, true),
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
            .expect_check_maintain_dataset(&dataset_id_foo, 1, true)
            .expect_check_maintain_dataset(&dataset_id_bar, 1, true),
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
async fn test_compact_dataset_unauthorized() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = CompactUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_maintain_dataset(&dataset_id_foo, 1, false),
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
async fn test_compact_dataset_mixed_authorization_outcome() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_bar) = odf::DatasetID::new_generated_ed25519();

    let harness = CompactUseCaseHarness::new(
        MockDatasetActionAuthorizer::new()
            .expect_check_maintain_dataset(&dataset_id_foo, 1, false)
            .expect_check_maintain_dataset(&dataset_id_bar, 1, true),
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
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_authorizer(Some(mock_dataset_action_authorizer))
                .with_maybe_mock_did_generator(Some(mock_did_generator)),
        );

        let catalog = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog())
            .add::<CompactDatasetUseCaseImpl>()
            .add::<CompactionPlannerImpl>()
            .add_value(EngineConfigDatafusionEmbeddedCompaction::default())
            .add::<CompactionExecutorImpl>()
            .add::<ObjectStoreRegistryImpl>()
            .build();

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
        self.use_case
            .execute(target.get_handle(), CompactionOptions::default(), None)
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
