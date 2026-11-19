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

use kamu::testing::{BaseRepoHarness, BaseUseCaseHarness, BaseUseCaseHarnessOptions};
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
    let alias_foo = BaseRepoHarness::alias("foo");
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = VerifyJobHarness::new(
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
async fn test_verify_replay_with_unreadable_input_replays_nothing() {
    let alias_foo = BaseRepoHarness::alias("foo");
    let alias_bar = BaseRepoHarness::alias("bar");
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_bar) = odf::DatasetID::new_generated_ed25519();

    let harness = VerifyJobHarness::new(
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
            outcome: Err(VerificationError::DatasetNotFound(e)),
        } if dataset_handle == bar.dataset_handle
            && e.dataset_ref == foo.dataset_handle.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The integrity checks explain a broken range better than the replay
/// planning that fails on it, so they run first
#[tokio::test]
async fn test_verify_replay_of_invalid_range_reports_integrity_error() {
    let alias_foo = BaseRepoHarness::alias("foo");
    let alias_bar = BaseRepoHarness::alias("bar");
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_bar) = odf::DatasetID::new_generated_ed25519();

    let harness = VerifyJobHarness::new(
        MockDatasetPermissionChecker::new().expect_authorize_for(
            DatasetPermission::Read,
            &dataset_id_bar,
            1,
            true,
        ),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo, dataset_id_bar]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    let tail_of_another_chain = harness.get_head(&foo).await;

    assert_matches!(
        harness
            .verify_range_with_replay(
                ResolvedDataset::from_created(&bar),
                (Some(tail_of_another_chain), None),
            )
            .await,
        VerificationResult {
            dataset_handle: Some(dataset_handle),
            outcome: Err(VerificationError::InvalidInterval(_)),
        } if dataset_handle == bar.dataset_handle
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_verify_unreadable_is_not_found() {
    let alias_foo = BaseRepoHarness::alias("foo");
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = VerifyJobHarness::new(
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
            outcome: Err(VerificationError::DatasetNotFound(_)),
        } if dataset_handle == foo.dataset_handle
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct VerifyJobHarness {
    base_use_case_harness: BaseUseCaseHarness,
    job: Arc<dyn VerifyJob>,
}

impl VerifyJobHarness {
    fn new(
        mock_dataset_permission_checker: MockDatasetPermissionChecker,
        mock_did_generator: MockDidGenerator,
    ) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_permission_checker(Some(mock_dataset_permission_checker))
                .with_maybe_mock_did_generator(Some(mock_did_generator)),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add::<VerifyJobImpl>()
            .add::<VerificationServiceImpl>()
            .add::<TransformPlannerImpl>()
            .add::<TransformExecutorImpl>()
            .add::<EngineProvisionerNull>();
        let catalog = b.build();

        let job = catalog.get_one().unwrap();

        Self {
            base_use_case_harness,
            job,
        }
    }

    async fn verify_dataset(&self, target: ResolvedDataset) -> VerificationResult {
        self.job
            .run(
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
        self.verify_range_with_replay(target, (None, None)).await
    }

    async fn verify_range_with_replay(
        &self,
        target: ResolvedDataset,
        block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
    ) -> VerificationResult {
        self.job
            .run(
                VerificationRequest::<odf::DatasetHandle> {
                    target: target.take_handle(),
                    block_range,
                    options: VerificationOptions {
                        replay_transformations: true,
                        ..VerificationOptions::default()
                    },
                },
                None,
            )
            .await
    }

    /// Records a transform of `derived` that consumed nothing of `input` yet,
    /// as if it had run
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
                // Replay planning only needs the record, so the block is not validated
                odf::dataset::AppendOpts {
                    validation: odf::dataset::AppendValidation::None,
                    ..odf::dataset::AppendOpts::default()
                },
            )
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
