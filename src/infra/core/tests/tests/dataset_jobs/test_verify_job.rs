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

#[tokio::test(flavor = "multi_thread")]
async fn test_verify_covers_head_of_the_job_transaction() {
    let alias_foo = BaseRepoHarness::alias("foo");
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = VerifyJobHarness::new(
        MockDatasetPermissionChecker::new().expect_authorize_for(
            DatasetPermission::Read,
            &dataset_id_foo,
            2,
            true,
        ),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    let broken_head = harness
        .append_unreferenced_block_out_of_sequence(&foo)
        .await;
    assert_matches!(
        harness
            .verify_range(
                ResolvedDataset::from_created(&foo),
                (None, Some(broken_head.clone()))
            )
            .await,
        VerificationResult {
            outcome: Err(VerificationError::BlockMalformed(e)),
            ..
        } if e.hash == broken_head
    );

    // `HEAD` moves onto the broken block once the verification has started
    assert_matches!(
        harness
            .verify_dataset_moving_head_to(&foo, &broken_head)
            .await,
        VerificationResult {
            dataset_handle: Some(dataset_handle),
            outcome: Ok(()),
        } if dataset_handle == foo.dataset_handle
    );
    assert_eq!(harness.get_head(&foo).await, broken_head);
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

    async fn verify_range(
        &self,
        target: ResolvedDataset,
        block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
    ) -> VerificationResult {
        self.job
            .run(
                VerificationRequest::<odf::DatasetHandle> {
                    target: target.take_handle(),
                    block_range,
                    options: VerificationOptions::default(),
                },
                None,
            )
            .await
    }

    /// Verifies all of `created`, moving its `HEAD` to `new_head` when the
    /// verification begins
    async fn verify_dataset_moving_head_to(
        &self,
        created: &CreateDatasetResult,
        new_head: &odf::Multihash,
    ) -> VerificationResult {
        let listener = Arc::new(HeadMovingListener {
            dataset: created.dataset.clone(),
            old_head: self.get_head(created).await,
            new_head: new_head.clone(),
        });
        self.job
            .run(
                VerificationRequest::<odf::DatasetHandle> {
                    target: created.dataset_handle.clone(),
                    block_range: (None, None),
                    options: VerificationOptions::default(),
                },
                Some(listener),
            )
            .await
    }

    /// Appends a block after `HEAD` that skips a sequence number, without
    /// moving any reference; returns its hash
    async fn append_unreferenced_block_out_of_sequence(
        &self,
        created: &CreateDatasetResult,
    ) -> odf::Multihash {
        let chain = created.dataset.as_metadata_chain();
        let head = self.get_head(created).await;
        let head_block = chain.get_block(&head).await.unwrap();

        chain
            .append(
                BaseRepoHarness::set_info_block(
                    &head,
                    head_block.sequence_number + 1,
                    "out of sequence",
                    head_block.system_time,
                ),
                odf::dataset::AppendOpts {
                    validation: odf::dataset::AppendValidation::None,
                    update_ref: None,
                    ..odf::dataset::AppendOpts::default()
                },
            )
            .await
            .unwrap()
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

/// Moves `HEAD` of `dataset` from `old_head` to `new_head` when the
/// verification begins, the way a concurrent writer would
struct HeadMovingListener {
    dataset: Arc<dyn odf::Dataset>,
    old_head: odf::Multihash,
    new_head: odf::Multihash,
}

impl VerificationListener for HeadMovingListener {
    fn begin(&self) {
        // The listener is synchronous, while moving a reference is not
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.dataset
                    .as_metadata_chain()
                    .set_ref(
                        &odf::BlockRef::Head,
                        Some(&self.old_head),
                        &self.new_head,
                        odf::dataset::SetRefOpts {
                            validate_block_present: true,
                        },
                    )
                    .await
                    .unwrap();
            });
        });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
