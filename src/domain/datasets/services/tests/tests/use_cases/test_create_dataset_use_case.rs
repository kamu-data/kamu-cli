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
use dill::*;
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu_core::MockDidGenerator;
use kamu_datasets::{
    CreateDatasetUseCase,
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
    CreateDatasetUseCaseImpl,
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
async fn test_create_root_dataset() {
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

    let harness = CreateUseCaseHarness::new(
        mock_dataset_entry_writer,
        mock_dependency_graph_writer,
        predefined_foo_id.clone(),
    );

    let foo_created = harness
        .use_case
        .execute(
            &alias_foo,
            MetadataFactory::metadata_block(
                MetadataFactory::seed(odf::DatasetKind::Root)
                    .id(predefined_foo_id)
                    .build(),
            )
            .system_time(harness.system_time_source().now())
            .build_typed(),
            Default::default(),
        )
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
                New Head: Multihash<Sha3_256>(f16204a8d23bf4ee5eb409b85c9c3806682c2f77cd0518d027e6de5be1fd024aadab8)
              }
            "#
        ),
        format!("{}", harness.test_dataset_outbox_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct CreateUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn CreateDatasetUseCase>,
    dataset_reference_repo: Arc<dyn DatasetReferenceRepository>,
    test_dataset_outbox_listener: Arc<TestDatasetOutboxListener>,
}

impl CreateUseCaseHarness {
    fn new(
        mock_dataset_entry_writer: MockDatasetEntryWriter,
        mock_dependency_graph_writer: MockDependencyGraphWriter,
        predefined_dataset_id: odf::DatasetID,
    ) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_system_time_source_stub(SystemTimeSourceStub::new_set(
                    Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
                ))
                .without_outbox()
                .with_maybe_mock_did_generator(Some(MockDidGenerator::predefined_dataset_ids(
                    vec![predefined_dataset_id],
                ))),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add_builder(
            messaging_outbox::OutboxImmediateImpl::builder()
                .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
        )
        .bind::<dyn Outbox, OutboxImmediateImpl>()
        .add::<CreateDatasetUseCaseImpl>()
        .add_value(mock_dataset_entry_writer)
        .bind::<dyn DatasetEntryWriter, MockDatasetEntryWriter>()
        .add_value(mock_dependency_graph_writer)
        .bind::<dyn DependencyGraphWriter, MockDependencyGraphWriter>()
        .add::<DatasetCreateHelper>()
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
