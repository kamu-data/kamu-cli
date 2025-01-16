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
use kamu::RenameDatasetUseCaseImpl;
use kamu_core::{GetDatasetError, MockDidGenerator, RenameDatasetError, RenameDatasetUseCase};
use messaging_outbox::MockOutbox;
use opendatafabric::{DatasetAlias, DatasetID, DatasetName};

use crate::tests::use_cases::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_success_via_ref() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let alias_bar = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));
    let (_, dataset_id_foo) = DatasetID::new_generated_ed25519();

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, true);
    let mut mock_outbox = MockOutbox::new();
    expect_outbox_dataset_renamed(&mut mock_outbox, 1);

    let harness = RenameUseCaseHarness::new(
        mock_authorizer,
        mock_outbox,
        Some(MockDidGenerator::predefined_dataset_ids(vec![
            dataset_id_foo,
        ])),
    );
    harness.create_root_dataset(&alias_foo).await;

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(
        harness.check_dataset_exists(&alias_bar).await,
        Err(GetDatasetError::NotFound(_))
    );

    harness
        .use_case
        .execute(&alias_foo.as_local_ref(), &alias_bar.dataset_name)
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(GetDatasetError::NotFound(_))
    );
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_not_found() {
    let harness =
        RenameUseCaseHarness::new(MockDatasetActionAuthorizer::new(), MockOutbox::new(), None);

    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    assert_matches!(
        harness
            .use_case
            .execute(
                &alias_foo.as_local_ref(),
                &DatasetName::new_unchecked("bar")
            )
            .await,
        Err(RenameDatasetError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_unauthorized() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = DatasetID::new_generated_ed25519();

    let harness = RenameUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, false),
        MockOutbox::new(),
        Some(MockDidGenerator::predefined_dataset_ids(vec![
            dataset_id_foo,
        ])),
    );

    harness.create_root_dataset(&alias_foo).await;

    assert_matches!(
        harness
            .use_case
            .execute(
                &alias_foo.as_local_ref(),
                &DatasetName::new_unchecked("bar")
            )
            .await,
        Err(RenameDatasetError::Access(_))
    );

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_harness)]
struct RenameUseCaseHarness {
    base_harness: BaseUseCaseHarness,
    use_case: Arc<dyn RenameDatasetUseCase>,
}

impl RenameUseCaseHarness {
    fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_outbox: MockOutbox,
        maybe_mock_did_generator: Option<MockDidGenerator>,
    ) -> Self {
        let base_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_authorizer(mock_dataset_action_authorizer)
                .with_outbox(mock_outbox)
                .with_maybe_mock_did_generator(maybe_mock_did_generator),
        );

        let catalog = dill::CatalogBuilder::new_chained(base_harness.catalog())
            .add::<RenameDatasetUseCaseImpl>()
            .build();

        let use_case = catalog.get_one::<dyn RenameDatasetUseCase>().unwrap();

        Self {
            base_harness,
            use_case,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
