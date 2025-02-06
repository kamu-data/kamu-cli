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

use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu_datasets::CreateDatasetFromSnapshotUseCase;
use kamu_datasets_services::testing::expect_outbox_dataset_created;
use kamu_datasets_services::{
    CreateDatasetFromSnapshotUseCaseImpl,
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
async fn test_create_root_dataset_from_snapshot() {
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

    // Expect only DatasetCreated message for "foo"
    let mut mock_outbox = MockOutbox::new();
    expect_outbox_dataset_created(&mut mock_outbox, 1);

    let harness = CreateFromSnapshotUseCaseHarness::new(
        mock_dataset_entry_writer,
        mock_dependency_graph_writer,
        mock_outbox,
    );

    let snapshot = MetadataFactory::dataset_snapshot()
        .name(alias_foo.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    harness
        .use_case
        .execute(snapshot, Default::default())
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_derived_dataset_from_snapshot() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let mut mock_dataset_entry_writer = MockDatasetEntryWriter::new();
    mock_dataset_entry_writer
        .expect_create_entry()
        .with(always(), always(), eq(alias_foo.dataset_name.clone()))
        .once()
        .returning(|_, _, _| Ok(()));
    mock_dataset_entry_writer
        .expect_create_entry()
        .with(always(), always(), eq(alias_bar.dataset_name.clone()))
        .once()
        .returning(|_, _, _| Ok(()));

    let mut mock_dependency_graph_writer = MockDependencyGraphWriter::new();
    mock_dependency_graph_writer
        .expect_create_dataset_node()
        .times(2)
        .returning(|_| Ok(()));
    mock_dependency_graph_writer
        .expect_update_dataset_node_dependencies()
        .times(1)
        .returning(|_, _, _| Ok(()));

    // Expect DatasetCreated messages for "foo" and "bar"
    let mut mock_outbox = MockOutbox::new();
    expect_outbox_dataset_created(&mut mock_outbox, 2);

    let harness = CreateFromSnapshotUseCaseHarness::new(
        mock_dataset_entry_writer,
        mock_dependency_graph_writer,
        mock_outbox,
    );

    let snapshot_root = MetadataFactory::dataset_snapshot()
        .name(alias_foo.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let snapshot_derived = MetadataFactory::dataset_snapshot()
        .name(alias_bar.clone())
        .kind(odf::DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs(vec![alias_foo.as_local_ref()])
                .build(),
        )
        .build();

    let options = Default::default();

    harness
        .use_case
        .execute(snapshot_root, options)
        .await
        .unwrap();
    harness
        .use_case
        .execute(snapshot_derived, options)
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_harness)]
struct CreateFromSnapshotUseCaseHarness {
    base_harness: BaseUseCaseHarness,
    use_case: Arc<dyn CreateDatasetFromSnapshotUseCase>,
}

impl CreateFromSnapshotUseCaseHarness {
    fn new(
        mock_dataset_entry_writer: MockDatasetEntryWriter,
        mock_dependency_graph_writer: MockDependencyGraphWriter,
        mock_outbox: MockOutbox,
    ) -> Self {
        let base_harness =
            BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new().with_outbox(mock_outbox));

        let catalog = dill::CatalogBuilder::new_chained(base_harness.catalog())
            .add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<CreateDatasetUseCaseImpl>()
            .add_value(mock_dataset_entry_writer)
            .bind::<dyn DatasetEntryWriter, MockDatasetEntryWriter>()
            .add_value(mock_dependency_graph_writer)
            .bind::<dyn DependencyGraphWriter, MockDependencyGraphWriter>()
            .build();

        let use_case = catalog.get_one().unwrap();

        Self {
            base_harness,
            use_case,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
