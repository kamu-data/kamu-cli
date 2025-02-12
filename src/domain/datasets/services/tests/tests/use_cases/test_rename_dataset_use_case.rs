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

use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions, MockDatasetActionAuthorizer};
use kamu_core::MockDidGenerator;
use kamu_datasets::RenameDatasetUseCase;
use kamu_datasets_services::{
    DatasetEntryWriter,
    MockDatasetEntryWriter,
    RenameDatasetUseCaseImpl,
};
use mockall::predicate::function;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_success_via_ref() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let (_, foo_id) = odf::DatasetID::new_generated_ed25519();

    let foo_id_clone = foo_id.clone();
    let mut mock_entry_writer = MockDatasetEntryWriter::new();
    mock_entry_writer
        .expect_rename_entry()
        .with(
            function(move |hdl: &odf::DatasetHandle| hdl.id == foo_id_clone),
            function(|new_name: &odf::DatasetName| new_name.as_str() == "bar"),
        )
        .once()
        .returning(|_, _| Ok(()));

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&foo_id, 1, true);

    let harness = RenameUseCaseHarness::new(
        mock_entry_writer,
        mock_authorizer,
        Some(MockDidGenerator::predefined_dataset_ids(vec![foo_id])),
    );
    harness.create_root_dataset(&alias_foo).await;

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(
        harness.check_dataset_exists(&alias_bar).await,
        Err(odf::dataset::GetDatasetError::NotFound(_))
    );

    harness
        .use_case
        .execute(&alias_foo.as_local_ref(), &alias_bar.dataset_name)
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::dataset::GetDatasetError::NotFound(_))
    );
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_not_found() {
    let harness = RenameUseCaseHarness::new(
        MockDatasetEntryWriter::new(),
        MockDatasetActionAuthorizer::new(),
        None,
    );

    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    assert_matches!(
        harness
            .use_case
            .execute(
                &alias_foo.as_local_ref(),
                &odf::DatasetName::new_unchecked("bar")
            )
            .await,
        Err(odf::dataset::RenameDatasetError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_unauthorized() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = RenameUseCaseHarness::new(
        MockDatasetEntryWriter::new(),
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, false),
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
                &odf::DatasetName::new_unchecked("bar")
            )
            .await,
        Err(odf::dataset::RenameDatasetError::Access(_))
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
        mock_dataset_entry_writer: MockDatasetEntryWriter,
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        maybe_mock_did_generator: Option<MockDidGenerator>,
    ) -> Self {
        let base_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_authorizer(Some(mock_dataset_action_authorizer))
                .with_maybe_mock_did_generator(maybe_mock_did_generator),
        );

        let catalog = dill::CatalogBuilder::new_chained(base_harness.catalog())
            .add::<RenameDatasetUseCaseImpl>()
            .add_value(mock_dataset_entry_writer)
            .bind::<dyn DatasetEntryWriter, MockDatasetEntryWriter>()
            .build();

        let use_case = catalog.get_one::<dyn RenameDatasetUseCase>().unwrap();

        Self {
            base_harness,
            use_case,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
