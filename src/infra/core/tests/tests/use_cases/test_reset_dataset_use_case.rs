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

    let harness = ResetUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, true),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;

    harness
        .commit_event(
            &foo,
            MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
            CommitOpts::default(),
        )
        .await
        .unwrap();
    assert_eq!(harness.num_blocks(&foo).await, 3);

    let new_head = harness
        .use_case
        .execute(&foo.dataset_handle, Some(&foo.head), None)
        .await
        .unwrap();

    assert_eq!(new_head, foo.head);
    assert_eq!(harness.num_blocks(&foo).await, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_reset_dataset_unauthorized() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let harness = ResetUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, false),
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
    fn new(mock_dataset_action_authorizer: MockDatasetActionAuthorizer) -> Self {
        let base_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new().with_authorizer(mock_dataset_action_authorizer),
        );

        let catalog = dill::CatalogBuilder::new_chained(base_harness.catalog())
            .add::<ResetDatasetUseCaseImpl>()
            .add::<ResetServiceImpl>()
            .build();

        let use_case = catalog.get_one::<dyn ResetDatasetUseCase>().unwrap();

        Self {
            base_harness,
            use_case,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
