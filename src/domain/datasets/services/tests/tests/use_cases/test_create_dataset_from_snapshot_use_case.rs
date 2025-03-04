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

use chrono::{TimeZone, Utc};
use dill::Component;
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu_core::MockDidGenerator;
use kamu_datasets::{
    CreateDatasetFromSnapshotUseCase,
    DatasetLifecycleMessage,
    DatasetReferenceMessage,
    DatasetReferenceRepository,
    MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use kamu_datasets_inmem::InMemoryDatasetReferenceRepository;
use kamu_datasets_services::testing::TestDatasetOutboxListener;
use kamu_datasets_services::utils::DatasetCreateHelper;
use kamu_datasets_services::{
    CreateDatasetFromSnapshotUseCaseImpl,
    DatasetEntryWriter,
    DatasetReferenceServiceImpl,
    DependencyGraphWriter,
    MockDatasetEntryWriter,
    MockDependencyGraphWriter,
};
use messaging_outbox::{register_message_dispatcher, Outbox, OutboxImmediateImpl};
use mockall::predicate::{always, eq};
use odf::metadata::testing::MetadataFactory;
use time_source::SystemTimeSourceStub;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_root_dataset_from_snapshot() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let predefined_foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

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

    let harness = CreateFromSnapshotUseCaseHarness::new(
        mock_dataset_entry_writer,
        mock_dependency_graph_writer,
        vec![predefined_foo_id],
    );

    let snapshot = MetadataFactory::dataset_snapshot()
        .name(alias_foo.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let foo_created = harness
        .use_case
        .execute(snapshot, Default::default())
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_eq!(
        harness
            .get_dataset_reference(&foo_created.dataset_handle.id, &odf::BlockRef::Head)
            .await,
        foo_created.head,
    );

    // Note: the stability of these identifiers is ensured via
    //  predefined dataset ID and stubbed system time
    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Lifecycle Messages: 1
              Created {
                Dataset ID: did:odf:fed01666f6fb3b7370000666f6fb3b737000060f6f60600000000895cddbcb7f7b8cc
                Dataset Name: foo
                Owner: did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f
                Visibility: private
              }
            Dataset Reference Messages: 1
              Ref Updated {
                Dataset ID: did:odf:fed01666f6fb3b7370000666f6fb3b737000060f6f60600000000895cddbcb7f7b8cc
                Ref: head
                Prev Head: None
                New Head: Multihash<Sha3_256>(f162017e757a57a4851490c0f8d576f65e2eb12dfc1e5e09cbaf281a3b761f7841f7e)
              }
            "#
        ),
        format!("{}", harness.test_dataset_outbox_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_derived_dataset_from_snapshot() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let predefined_foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let predefined_bar_id = odf::DatasetID::new_seeded_ed25519(b"bar");

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

    let harness = CreateFromSnapshotUseCaseHarness::new(
        mock_dataset_entry_writer,
        mock_dependency_graph_writer,
        vec![predefined_foo_id.clone(), predefined_bar_id.clone()],
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

    let foo_created = harness
        .use_case
        .execute(snapshot_root, options)
        .await
        .unwrap();
    let bar_created = harness
        .use_case
        .execute(snapshot_derived, options)
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));

    assert_eq!(
        harness
            .get_dataset_reference(&foo_created.dataset_handle.id, &odf::BlockRef::Head)
            .await,
        foo_created.head,
    );
    assert_eq!(
        harness
            .get_dataset_reference(&bar_created.dataset_handle.id, &odf::BlockRef::Head)
            .await,
        bar_created.head,
    );

    // Note: the stability of these identifiers is ensured via
    //  predefined dataset ID and stubbed system time
    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Lifecycle Messages: 2
              Created {
                Dataset ID: did:odf:fed01666f6fb3b7370000666f6fb3b737000060f6f60600000000895cddbcb7f7b8cc
                Dataset Name: foo
                Owner: did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f
                Visibility: private
              }
              Created {
                Dataset ID: did:odf:fed01626172b130390000626172b1303900002016260700000000508ebebd3079f00e
                Dataset Name: bar
                Owner: did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f
                Visibility: private
              }
            Dataset Reference Messages: 2
              Ref Updated {
                Dataset ID: did:odf:fed01666f6fb3b7370000666f6fb3b737000060f6f60600000000895cddbcb7f7b8cc
                Ref: head
                Prev Head: None
                New Head: Multihash<Sha3_256>(f162017e757a57a4851490c0f8d576f65e2eb12dfc1e5e09cbaf281a3b761f7841f7e)
              }
              Ref Updated {
                Dataset ID: did:odf:fed01626172b130390000626172b1303900002016260700000000508ebebd3079f00e
                Ref: head
                Prev Head: None
                New Head: Multihash<Sha3_256>(f16203eae1d761a2c8076ca7bc2d0c54c86105e1684fb5acadf667649fc31dd4afd64)
              }
            "#
        ),
        format!("{}", harness.test_dataset_outbox_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct CreateFromSnapshotUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    dataset_reference_repo: Arc<dyn DatasetReferenceRepository>,
    test_dataset_outbox_listener: Arc<TestDatasetOutboxListener>,
}

impl CreateFromSnapshotUseCaseHarness {
    fn new(
        mock_dataset_entry_writer: MockDatasetEntryWriter,
        mock_dependency_graph_writer: MockDependencyGraphWriter,
        predefined_dataset_ids: Vec<odf::DatasetID>,
    ) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_system_time_source_stub(SystemTimeSourceStub::new_set(
                    Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
                ))
                .without_outbox()
                .with_maybe_mock_did_generator(Some(MockDidGenerator::predefined_dataset_ids(
                    predefined_dataset_ids,
                ))),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add_builder(
            messaging_outbox::OutboxImmediateImpl::builder()
                .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
        )
        .bind::<dyn Outbox, OutboxImmediateImpl>()
        .add::<CreateDatasetFromSnapshotUseCaseImpl>()
        .add::<DatasetCreateHelper>()
        .add_value(mock_dataset_entry_writer)
        .bind::<dyn DatasetEntryWriter, MockDatasetEntryWriter>()
        .add_value(mock_dependency_graph_writer)
        .bind::<dyn DependencyGraphWriter, MockDependencyGraphWriter>()
        .add::<DatasetReferenceServiceImpl>()
        .add::<InMemoryDatasetReferenceRepository>()
        .add::<TestDatasetOutboxListener>();

        register_message_dispatcher::<DatasetLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        );

        register_message_dispatcher::<DatasetReferenceMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
        );

        let catalog = b.build();

        Self {
            base_use_case_harness,
            use_case: catalog.get_one().unwrap(),
            dataset_reference_repo: catalog.get_one().unwrap(),
            test_dataset_outbox_listener: catalog.get_one().unwrap(),
        }
    }

    async fn get_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> odf::Multihash {
        self.dataset_reference_repo
            .get_dataset_reference(dataset_id, block_ref)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
