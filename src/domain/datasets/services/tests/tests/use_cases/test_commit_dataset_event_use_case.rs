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
use kamu_datasets::CommitDatasetEventUseCase;
use kamu_datasets_services::{
    CommitDatasetEventUseCaseImpl,
    DependencyGraphWriter,
    MockDependencyGraphWriter,
};
use odf::metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_dataset_event() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, true);

    let harness = CommitDatasetEventUseCaseHarness::new(
        mock_authorizer,
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
        MockDependencyGraphWriter::new(),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;

    let res = harness
        .use_case
        .execute(
            &foo.dataset_handle,
            odf::MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
        )
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_event_unauthorized() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, false);

    let harness = CommitDatasetEventUseCaseHarness::new(
        mock_authorizer,
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
        MockDependencyGraphWriter::new(),
    );
    let foo = harness.create_root_dataset(&alias_foo).await;

    let res = harness
        .use_case
        .execute(
            &foo.dataset_handle,
            odf::MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
        )
        .await;
    assert_matches!(res, Err(odf::dataset::CommitError::Access(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_event_with_new_dependencies() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_bar) = odf::DatasetID::new_generated_ed25519();

    let mock_authorizer = MockDatasetActionAuthorizer::new()
        .expect_check_write_dataset(&dataset_id_bar, 1, true)
        .expect_check_read_dataset(&dataset_id_foo, 1, true);

    let mut mock_dependency_writer = MockDependencyGraphWriter::new();
    mock_dependency_writer
        .expect_update_dataset_node_dependencies()
        .once()
        .returning(|_, _, _| Ok(()));

    let harness = CommitDatasetEventUseCaseHarness::new(
        mock_authorizer,
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo, dataset_id_bar]),
        mock_dependency_writer,
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let res = harness
        .use_case
        .execute(
            &bar.dataset_handle,
            odf::MetadataEvent::SetTransform(
                MetadataFactory::set_transform()
                    .inputs_from_refs_and_aliases(vec![(
                        foo.dataset_handle.id,
                        alias_foo.to_string(),
                    )])
                    .build(),
            ),
        )
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct CommitDatasetEventUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn CommitDatasetEventUseCase>,
}

impl CommitDatasetEventUseCaseHarness {
    fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_did_generator: MockDidGenerator,
        mock_dependency_graph_writer: MockDependencyGraphWriter,
    ) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_authorizer(Some(mock_dataset_action_authorizer))
                .with_maybe_mock_did_generator(Some(mock_did_generator)),
        );

        let catalog = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog())
            .add::<CommitDatasetEventUseCaseImpl>()
            .add_value(mock_dependency_graph_writer)
            .bind::<dyn DependencyGraphWriter, MockDependencyGraphWriter>()
            .build();

        let use_case = catalog.get_one::<dyn CommitDatasetEventUseCase>().unwrap();

        Self {
            base_use_case_harness,
            use_case,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
