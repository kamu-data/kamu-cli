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

use kamu::testing::{MetadataFactory, MockDatasetActionAuthorizer};
use kamu::*;
use kamu_core::*;
use opendatafabric::*;

use super::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_success() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = DatasetID::new_generated_ed25519();

    let harness = ResetUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, true),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    foo.dataset
        .commit_event(
            MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    assert_eq!(harness.num_blocks(ResolvedDataset::from(&foo)).await, 3);

    let reset_result = harness
        .use_case
        .execute(&foo.dataset_handle, Some(&foo.head), None)
        .await
        .unwrap();

    assert_eq!(reset_result.new_head, foo.head);
    assert_eq!(harness.num_blocks(ResolvedDataset::from(&foo)).await, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_dataset_unauthorized() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = DatasetID::new_generated_ed25519();

    let harness = ResetUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, false),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;

    assert_matches!(
        harness
            .use_case
            .execute(&foo.dataset_handle, Some(&foo.head), None)
            .await,
        Err(ResetError::Access(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_harness)]
struct ResetUseCaseHarness {
    base_harness: BaseUseCaseHarness,
    use_case: Arc<dyn ResetDatasetUseCase>,
}

impl ResetUseCaseHarness {
    fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_did_generator: MockDidGenerator,
    ) -> Self {
        let base_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_authorizer(mock_dataset_action_authorizer)
                .with_maybe_mock_did_generator(Some(mock_did_generator)),
        );

        let catalog = dill::CatalogBuilder::new_chained(base_harness.catalog())
            .add::<ResetDatasetUseCaseImpl>()
            .add::<ResetPlannerImpl>()
            .add::<ResetExecutorImpl>()
            .build();

        let use_case = catalog.get_one::<dyn ResetDatasetUseCase>().unwrap();

        Self {
            base_harness,
            use_case,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
