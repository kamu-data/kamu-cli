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

use kamu::testing::MockDatasetActionAuthorizer;
use kamu::*;
use kamu_core::*;
use opendatafabric::*;

use super::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_verify_success() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let harness = VerifyUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_read_dataset(&alias_foo, 1, true),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    assert_matches!(
        harness.verify_dataset(ResolvedDataset::from(&foo)).await,
        VerificationResult {
            dataset_handle: Some(dataset_handle),
            outcome: Ok(()),
        } if dataset_handle == foo.dataset_handle
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_verify_multiple_success() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let alias_bar = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));

    let harness = VerifyUseCaseHarness::new(
        MockDatasetActionAuthorizer::new()
            .expect_check_read_dataset(&alias_foo, 1, true)
            .expect_check_read_dataset(&alias_bar, 1, true),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;

    let mut responses = harness
        .verify_datasets(vec![
            ResolvedDataset::from(&foo),
            ResolvedDataset::from(&bar),
        ])
        .await;

    assert_eq!(responses.len(), 2);
    let response_bar = responses.remove(1);
    let response_foo = responses.remove(0);

    assert_matches!(
        response_foo,
        VerificationResult {
            dataset_handle,
            outcome: Ok(_),
        }
        if dataset_handle == Some(foo.dataset_handle)
    );
    assert_matches!(
        response_bar,
        VerificationResult {
            dataset_handle,
            outcome: Ok(_),
        }
        if dataset_handle == Some(bar.dataset_handle)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_verify_unauthorized() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let harness = VerifyUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_read_dataset(&alias_foo, 1, false),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    assert_matches!(
        harness.verify_dataset(ResolvedDataset::from(&foo)).await,
        VerificationResult {
            dataset_handle: Some(dataset_handle),
            outcome: Err(VerificationError::Access(_)),
        } if dataset_handle == foo.dataset_handle
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_verify_mixed_authorization_outcome() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let alias_bar = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));
    let alias_baz = DatasetAlias::new(None, DatasetName::new_unchecked("baz"));

    let harness = VerifyUseCaseHarness::new(
        MockDatasetActionAuthorizer::new()
            .expect_check_read_dataset(&alias_foo, 1, true)
            .expect_check_read_dataset(&alias_bar, 1, false)
            .expect_check_read_dataset(&alias_baz, 1, true),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let baz = harness.create_root_dataset(&alias_baz).await;

    let mut responses = harness
        .verify_datasets(vec![
            ResolvedDataset::from(&foo),
            ResolvedDataset::from(&bar),
            ResolvedDataset::from(&baz),
        ])
        .await;

    assert_eq!(responses.len(), 3);
    let response_baz = responses.remove(2);
    let response_foo = responses.remove(1);
    let response_bar = responses.remove(0);

    assert_matches!(
        response_foo,
        VerificationResult {
            dataset_handle,
            outcome: Ok(_),
        }
        if dataset_handle == Some(foo.dataset_handle)
    );
    assert_matches!(
        response_bar,
        VerificationResult {
            dataset_handle,
            outcome: Err(VerificationError::Access(_)),
        }
        if dataset_handle == Some(bar.dataset_handle)
    );
    assert_matches!(
        response_baz,
        VerificationResult {
            dataset_handle,
            outcome: Ok(_),
        }
        if dataset_handle == Some(baz.dataset_handle)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_harness)]
struct VerifyUseCaseHarness {
    base_harness: BaseUseCaseHarness,
    use_case: Arc<dyn VerifyDatasetUseCase>,
}

impl VerifyUseCaseHarness {
    fn new(mock_dataset_action_authorizer: MockDatasetActionAuthorizer) -> Self {
        let base_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new().with_authorizer(mock_dataset_action_authorizer),
        );

        let catalog = dill::CatalogBuilder::new_chained(base_harness.catalog())
            .add::<VerifyDatasetUseCaseImpl>()
            .add::<VerificationServiceImpl>()
            .add::<TransformRequestPlannerImpl>()
            .add::<TransformExecutionServiceImpl>()
            .add::<EngineProvisionerNull>()
            .build();

        let use_case = catalog.get_one().unwrap();

        Self {
            base_harness,
            use_case,
        }
    }

    async fn verify_dataset(&self, target: ResolvedDataset) -> VerificationResult {
        self.use_case
            .execute(
                VerificationRequest::<DatasetHandle> {
                    target: target.handle,
                    block_range: (None, None),
                    options: VerificationOptions::default(),
                },
                None,
            )
            .await
    }

    async fn verify_datasets(&self, targets: Vec<ResolvedDataset>) -> Vec<VerificationResult> {
        let requests: Vec<_> = targets
            .into_iter()
            .map(|target| VerificationRequest::<DatasetHandle> {
                target: target.handle,
                block_range: (None, None),
                options: VerificationOptions::default(),
            })
            .collect();

        self.use_case.execute_multi(requests, None).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
