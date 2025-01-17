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

use dill::Catalog;
use kamu::testing::MockDatasetActionAuthorizer;
use kamu::DeleteDatasetUseCaseImpl;
use kamu_core::{DatasetLifecycleMessage, DeleteDatasetUseCase, MockDidGenerator};
use kamu_datasets_inmem::InMemoryDatasetDependencyRepository;
use kamu_datasets_services::{DependencyGraphIndexer, DependencyGraphServiceImpl};
use messaging_outbox::{consume_deserialized_message, ConsumerFilter, Message, MockOutbox};

use crate::tests::use_cases::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_success_via_ref() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let mut mock_outbox = MockOutbox::new();
    expect_outbox_dataset_deleted(&mut mock_outbox, 1);

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, true);

    let harness = DeleteUseCaseHarness::new(
        mock_authorizer,
        mock_outbox,
        Some(MockDidGenerator::predefined_dataset_ids(vec![
            dataset_id_foo,
        ])),
    );

    harness.create_root_dataset(&alias_foo).await;
    harness.reindex_dependency_graph().await;

    harness
        .use_case
        .execute_via_ref(&alias_foo.as_local_ref())
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::dataset::GetDatasetError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_success_via_handle() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let mut mock_outbox = MockOutbox::new();
    expect_outbox_dataset_deleted(&mut mock_outbox, 1);

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, true);

    let harness = DeleteUseCaseHarness::new(
        mock_authorizer,
        mock_outbox,
        Some(MockDidGenerator::predefined_dataset_ids(vec![
            dataset_id_foo,
        ])),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    harness.reindex_dependency_graph().await;

    harness
        .use_case
        .execute_via_handle(&foo.dataset_handle)
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::dataset::GetDatasetError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_not_found() {
    let harness =
        DeleteUseCaseHarness::new(MockDatasetActionAuthorizer::new(), MockOutbox::new(), None);

    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    assert_matches!(
        harness
            .use_case
            .execute_via_ref(&alias_foo.as_local_ref())
            .await,
        Err(odf::dataset::DeleteDatasetError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_unauthorized() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = DeleteUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, false),
        MockOutbox::new(),
        Some(MockDidGenerator::predefined_dataset_ids(vec![
            dataset_id_foo,
        ])),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    harness.reindex_dependency_graph().await;

    assert_matches!(
        harness
            .use_case
            .execute_via_handle(&foo.dataset_handle)
            .await,
        Err(odf::dataset::DeleteDatasetError::Access(_))
    );

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_respects_dangling_refs() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let mut mock_outbox = MockOutbox::new();
    expect_outbox_dataset_deleted(&mut mock_outbox, 2);

    let harness =
        DeleteUseCaseHarness::new(MockDatasetActionAuthorizer::allowing(), mock_outbox, None);

    let root = harness.create_root_dataset(&alias_foo).await;
    let derived = harness
        .create_derived_dataset(&alias_bar, vec![alias_foo.as_local_ref()])
        .await;
    harness.reindex_dependency_graph().await;

    assert_matches!(
        harness.use_case.execute_via_handle(&root.dataset_handle).await,
        Err(odf::dataset::DeleteDatasetError::DanglingReference(e)) if e.children == vec![derived.dataset_handle.clone()]
    );

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));

    harness
        .use_case
        .execute_via_handle(&derived.dataset_handle)
        .await
        .unwrap();

    harness
        .consume_message(DatasetLifecycleMessage::deleted(derived.dataset_handle.id))
        .await;

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(
        harness.check_dataset_exists(&alias_bar).await,
        Err(odf::dataset::GetDatasetError::NotFound(_))
    );

    harness
        .use_case
        .execute_via_handle(&root.dataset_handle)
        .await
        .unwrap();

    harness
        .consume_message(DatasetLifecycleMessage::deleted(root.dataset_handle.id))
        .await;

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::dataset::GetDatasetError::NotFound(_))
    );
    assert_matches!(
        harness.check_dataset_exists(&alias_bar).await,
        Err(odf::dataset::GetDatasetError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_harness)]
struct DeleteUseCaseHarness {
    base_harness: BaseUseCaseHarness,
    catalog: Catalog,
    use_case: Arc<dyn DeleteDatasetUseCase>,
    indexer: Arc<DependencyGraphIndexer>,
}

impl DeleteUseCaseHarness {
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
            .add::<DeleteDatasetUseCaseImpl>()
            .add::<DependencyGraphServiceImpl>()
            .add::<InMemoryDatasetDependencyRepository>()
            .add::<DependencyGraphIndexer>()
            .build();

        let use_case = catalog.get_one().unwrap();
        let indexer = catalog.get_one().unwrap();

        Self {
            base_harness,
            catalog,
            use_case,
            indexer,
        }
    }

    async fn consume_message<TMessage: Message + 'static>(&self, message: TMessage) {
        let content_json = serde_json::to_string(&message).unwrap();
        consume_deserialized_message::<TMessage>(
            &self.catalog,
            ConsumerFilter::AllConsumers,
            &content_json,
            TMessage::version(),
        )
        .await
        .unwrap();
    }

    async fn reindex_dependency_graph(&self) {
        use init_on_startup::InitOnStartup;
        self.indexer.run_initialization().await.unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
