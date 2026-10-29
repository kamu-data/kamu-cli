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

use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::*;
use kamu_core::*;
use kamu_datasets::{
    CreateDatasetResult,
    DatasetPermission,
    MockDatasetPermissionChecker,
    ResolvedDataset,
};
use odf::metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_verify_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = VerifyUseCaseHarness::new(
        MockDatasetPermissionChecker::new().expect_authorize_for(
            DatasetPermission::Read,
            &dataset_id_foo,
            1,
            true,
        ),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    assert_matches!(
        harness.verify_dataset(ResolvedDataset::from_created(&foo)).await,
        VerificationResult {
            dataset_handle: Some(dataset_handle),
            outcome: Ok(()),
        } if dataset_handle == foo.dataset_handle
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_verify_multiple_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_bar) = odf::DatasetID::new_generated_ed25519();

    let harness = VerifyUseCaseHarness::new(
        MockDatasetPermissionChecker::new()
            .expect_authorize_for(DatasetPermission::Read, &dataset_id_foo, 1, true)
            .expect_authorize_for(DatasetPermission::Read, &dataset_id_bar, 1, true),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo, dataset_id_bar]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;

    let mut responses = harness
        .verify_datasets(vec![
            ResolvedDataset::from_created(&foo),
            ResolvedDataset::from_created(&bar),
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
async fn test_verify_replay_denied_on_input_replays_nothing() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_bar) = odf::DatasetID::new_generated_ed25519();

    let harness = VerifyUseCaseHarness::new(
        MockDatasetPermissionChecker::new()
            .expect_authorize_for(DatasetPermission::Read, &dataset_id_foo, 1, false)
            .expect_authorize_for(DatasetPermission::Read, &dataset_id_bar, 1, true),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo, dataset_id_bar]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    harness.append_execute_transform(&bar, &foo).await;

    assert_matches!(
        harness
            .verify_dataset_with_replay(ResolvedDataset::from_created(&bar))
            .await,
        VerificationResult {
            dataset_handle: Some(dataset_handle),
            outcome: Err(VerificationError::Access(odf::AccessError::Unauthorized(e))),
        } if dataset_handle == bar.dataset_handle
            && e.to_string().contains(&foo.dataset_handle.alias.to_string())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_verify_unauthorized() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = VerifyUseCaseHarness::new(
        MockDatasetPermissionChecker::new().expect_authorize_for(
            DatasetPermission::Read,
            &dataset_id_foo,
            1,
            false,
        ),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    assert_matches!(
        harness.verify_dataset(ResolvedDataset::from_created(&foo)).await,
        VerificationResult {
            dataset_handle: Some(dataset_handle),
            outcome: Err(VerificationError::Access(_)),
        } if dataset_handle == foo.dataset_handle
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_verify_mixed_authorization_outcome() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_bar) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_baz) = odf::DatasetID::new_generated_ed25519();

    let harness = VerifyUseCaseHarness::new(
        MockDatasetPermissionChecker::new()
            .expect_authorize_for(DatasetPermission::Read, &dataset_id_foo, 1, true)
            .expect_authorize_for(DatasetPermission::Read, &dataset_id_bar, 1, false)
            .expect_authorize_for(DatasetPermission::Read, &dataset_id_baz, 1, true),
        MockDidGenerator::predefined_dataset_ids(vec![
            dataset_id_foo,
            dataset_id_bar,
            dataset_id_baz,
        ]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let baz = harness.create_root_dataset(&alias_baz).await;

    let mut responses = harness
        .verify_datasets(vec![
            ResolvedDataset::from_created(&foo),
            ResolvedDataset::from_created(&bar),
            ResolvedDataset::from_created(&baz),
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

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct VerifyUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn VerifyDatasetUseCase>,
}

impl VerifyUseCaseHarness {
    fn new(
        mock_dataset_permission_checker: MockDatasetPermissionChecker,
        mock_did_generator: MockDidGenerator,
    ) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_permission_checker(Some(mock_dataset_permission_checker))
                .with_maybe_mock_did_generator(Some(mock_did_generator)),
        );

        let catalog = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog())
            .add::<VerifyDatasetUseCaseImpl>()
            .add::<VerificationServiceImpl>()
            .add::<TransformPlannerImpl>()
            .add::<TransformExecutorImpl>()
            .add::<EngineProvisionerNull>()
            .build();

        let use_case = catalog.get_one().unwrap();

        Self {
            base_use_case_harness,
            use_case,
        }
    }

    async fn verify_dataset(&self, target: ResolvedDataset) -> VerificationResult {
        self.use_case
            .execute(
                VerificationRequest::<odf::DatasetHandle> {
                    target: target.take_handle(),
                    block_range: (None, None),
                    options: VerificationOptions::default(),
                },
                None,
            )
            .await
    }

    async fn verify_dataset_with_replay(&self, target: ResolvedDataset) -> VerificationResult {
        self.use_case
            .execute(
                VerificationRequest::<odf::DatasetHandle> {
                    target: target.take_handle(),
                    block_range: (None, None),
                    options: VerificationOptions {
                        replay_transformations: true,
                        ..VerificationOptions::default()
                    },
                },
                None,
            )
            .await
    }

    /// Records a transform of `derived` that read `input`, as if it had run
    async fn append_execute_transform(
        &self,
        derived: &CreateDatasetResult,
        input: &CreateDatasetResult,
    ) {
        let chain = derived.dataset.as_metadata_chain();
        let head = self.get_head(derived).await;
        let head_block = chain.get_block(&head).await.unwrap();

        chain
            .append(
                MetadataFactory::metadata_block(
                    MetadataFactory::execute_transform()
                        .empty_query_inputs_from_particular_ids([input.dataset_handle.id.clone()])
                        .build(),
                )
                .prev(&head, head_block.sequence_number)
                .system_time(self.system_time_source().now())
                .build(),
                // A transform that read nothing yet: replay planning only needs the record
                odf::dataset::AppendOpts {
                    validation: odf::dataset::AppendValidation::None,
                    ..odf::dataset::AppendOpts::default()
                },
            )
            .await
            .unwrap();
    }

    async fn verify_datasets(&self, targets: Vec<ResolvedDataset>) -> Vec<VerificationResult> {
        let requests: Vec<_> = targets
            .into_iter()
            .map(|target| VerificationRequest::<odf::DatasetHandle> {
                target: target.take_handle(),
                block_range: (None, None),
                options: VerificationOptions::default(),
            })
            .collect();

        self.use_case.execute_multi(requests, None).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
