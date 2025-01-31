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

use kamu::testing::{expect_outbox_dataset_created, BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu_datasets::CreateDatasetUseCase;
use kamu_datasets_services::{
    CreateDatasetUseCaseImpl,
    DatasetEntryWriter,
    DependencyGraphWriter,
    MockDatasetEntryWriter,
    MockDependencyGraphWriter,
};
use messaging_outbox::MockOutbox;
use mockall::predicate::{always, eq};
use odf::metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_root_dataset() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let mut mock_dataset_entry_writer = MockDatasetEntryWriter::new();
    mock_dataset_entry_writer
        .expect_create_entry()
        .with(always(), always(), eq(alias_foo.dataset_name.clone()))
        .once()
        .returning(|_, _, _| Ok(()));

    let mut mock_dependency_graph_writer = MockDependencyGraphWriter::new();
    mock_dependency_graph_writer
        .expect_create_dataset_node()
        .once()
        .returning(|_| Ok(()));

    let mut mock_outbox = MockOutbox::new();
    expect_outbox_dataset_created(&mut mock_outbox, 1);

    let harness = CreateUseCaseHarness::new(
        mock_dataset_entry_writer,
        mock_dependency_graph_writer,
        mock_outbox,
    );

    harness
        .use_case
        .execute(
            &alias_foo,
            MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
                .build_typed(),
            Default::default(),
        )
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_harness)]
struct CreateUseCaseHarness {
    base_harness: BaseUseCaseHarness,
    use_case: Arc<dyn CreateDatasetUseCase>,
}

impl CreateUseCaseHarness {
    fn new(
        mock_dataset_entry_writer: MockDatasetEntryWriter,
        mock_dependency_graph_writer: MockDependencyGraphWriter,
        mock_outbox: MockOutbox,
    ) -> Self {
        let base_harness =
            BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new().with_outbox(mock_outbox));

        let catalog = dill::CatalogBuilder::new_chained(base_harness.catalog())
            .add::<CreateDatasetUseCaseImpl>()
            .add_value(mock_dataset_entry_writer)
            .bind::<dyn DatasetEntryWriter, MockDatasetEntryWriter>()
            .add_value(mock_dependency_graph_writer)
            .bind::<dyn DependencyGraphWriter, MockDependencyGraphWriter>()
            .build();

        let use_case = catalog.get_one::<dyn CreateDatasetUseCase>().unwrap();

        Self {
            base_harness,
            use_case,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
