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

use dill::{Catalog, Component};
use kamu::testing::{MetadataFactory, MockDatasetActionAuthorizer};
use kamu::{
    DatasetRepositoryLocalFs,
    DatasetRepositoryWriter,
    DeleteDatasetUseCaseImpl,
    DependencyGraphRepositoryInMemory,
    DependencyGraphServiceInMemory,
};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::auth::DatasetActionAuthorizer;
use kamu_core::{
    CreateDatasetResult,
    DatasetLifecycleMessage,
    DatasetRepository,
    DeleteDatasetError,
    DeleteDatasetUseCase,
    DependencyGraphService,
    GetDatasetError,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{consume_deserialized_message, ConsumerFilter, Message, MockOutbox, Outbox};
use mockall::predicate::{eq, function};
use opendatafabric::{DatasetAlias, DatasetKind, DatasetName, DatasetRef};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_success_via_ref() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let mut mock_outbox = MockOutbox::new();
    DeleteUseCaseHarness::add_outbox_dataset_deleted_expectation(&mut mock_outbox, 1);

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, true);

    let harness = DeleteUseCaseHarness::new(mock_authorizer, mock_outbox);

    harness.create_root_dataset(&alias_foo).await;
    harness.dependencies_eager_initialization().await;

    harness
        .use_case
        .execute_via_ref(&alias_foo.as_local_ref(), true)
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(GetDatasetError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_success_via_handle() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let mut mock_outbox = MockOutbox::new();
    DeleteUseCaseHarness::add_outbox_dataset_deleted_expectation(&mut mock_outbox, 1);

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, true);

    let harness = DeleteUseCaseHarness::new(mock_authorizer, mock_outbox);

    let create_result_foo = harness.create_root_dataset(&alias_foo).await;
    harness.dependencies_eager_initialization().await;

    harness
        .use_case
        .execute_via_handle(&create_result_foo.dataset_handle, true)
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(GetDatasetError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_not_found() {
    let harness = DeleteUseCaseHarness::new(MockDatasetActionAuthorizer::new(), MockOutbox::new());

    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    assert_matches!(
        harness
            .use_case
            .execute_via_ref(&alias_foo.as_local_ref(), true)
            .await,
        Err(DeleteDatasetError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_unauthorized() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let harness = DeleteUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, false),
        MockOutbox::new(),
    );

    let create_result_foo = harness.create_root_dataset(&alias_foo).await;
    harness.dependencies_eager_initialization().await;

    assert_matches!(
        harness
            .use_case
            .execute_via_handle(&create_result_foo.dataset_handle, true)
            .await,
        Err(DeleteDatasetError::Access(_))
    );

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_respects_dangling_refs() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let alias_bar = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));

    let mut mock_outbox = MockOutbox::new();
    DeleteUseCaseHarness::add_outbox_dataset_deleted_expectation(&mut mock_outbox, 2);

    let harness = DeleteUseCaseHarness::new(MockDatasetActionAuthorizer::allowing(), mock_outbox);

    let create_result_root = harness.create_root_dataset(&alias_foo).await;
    let create_result_derived = harness
        .create_derived_dataset(&alias_bar, vec![alias_foo.as_local_ref()])
        .await;
    harness.dependencies_eager_initialization().await;

    assert_matches!(
        harness.use_case.execute_via_handle(&create_result_root.dataset_handle, true).await,
        Err(DeleteDatasetError::DanglingReference(e)) if e.children == vec![create_result_derived.dataset_handle.clone()]
    );

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));

    harness
        .use_case
        .execute_via_handle(&create_result_derived.dataset_handle, true)
        .await
        .unwrap();

    harness
        .consume_message(DatasetLifecycleMessage::deleted(
            create_result_derived.dataset_handle.id,
        ))
        .await;

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(
        harness.check_dataset_exists(&alias_bar).await,
        Err(GetDatasetError::NotFound(_))
    );

    harness
        .use_case
        .execute_via_handle(&create_result_root.dataset_handle, true)
        .await
        .unwrap();

    harness
        .consume_message(DatasetLifecycleMessage::deleted(
            create_result_root.dataset_handle.id,
        ))
        .await;

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(GetDatasetError::NotFound(_))
    );
    assert_matches!(
        harness.check_dataset_exists(&alias_bar).await,
        Err(GetDatasetError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DeleteUseCaseHarness {
    _temp_dir: tempfile::TempDir,
    catalog: Catalog,
    use_case: Arc<dyn DeleteDatasetUseCase>,
}

impl DeleteUseCaseHarness {
    fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_outbox: MockOutbox,
    ) -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<DeleteDatasetUseCaseImpl>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<DependencyGraphServiceInMemory>()
            .add_value(mock_dataset_action_authorizer)
            .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
            .add::<SystemTimeSourceDefault>()
            .add_value(mock_outbox)
            .bind::<dyn Outbox, MockOutbox>()
            .build();

        let use_case = catalog.get_one::<dyn DeleteDatasetUseCase>().unwrap();

        Self {
            _temp_dir: tempdir,
            catalog,
            use_case,
        }
    }

    async fn create_root_dataset(&self, alias: &DatasetAlias) -> CreateDatasetResult {
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build();

        let dataset_repo_writer = self
            .catalog
            .get_one::<dyn DatasetRepositoryWriter>()
            .unwrap();

        let result = dataset_repo_writer
            .create_dataset_from_snapshot(snapshot)
            .await
            .unwrap();

        result.create_dataset_result
    }

    async fn create_derived_dataset(
        &self,
        alias: &DatasetAlias,
        input_dataset_refs: Vec<DatasetRef>,
    ) -> CreateDatasetResult {
        let dataset_repo_writer = self
            .catalog
            .get_one::<dyn DatasetRepositoryWriter>()
            .unwrap();

        dataset_repo_writer
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(alias.clone())
                    .kind(DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(input_dataset_refs)
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap()
            .create_dataset_result
    }

    async fn check_dataset_exists(&self, alias: &DatasetAlias) -> Result<(), GetDatasetError> {
        let dataset_repo = self.catalog.get_one::<dyn DatasetRepository>().unwrap();
        dataset_repo
            .find_dataset_by_ref(&alias.as_local_ref())
            .await?;
        Ok(())
    }

    async fn dependencies_eager_initialization(&self) {
        let dependency_graph_service = self
            .catalog
            .get_one::<dyn DependencyGraphService>()
            .unwrap();
        let dataset_repo = self.catalog.get_one::<dyn DatasetRepository>().unwrap();

        dependency_graph_service
            .eager_initialization(&DependencyGraphRepositoryInMemory::new(dataset_repo))
            .await
            .unwrap();
    }

    async fn consume_message<TMessage: Message + 'static>(&self, message: TMessage) {
        let content_json = serde_json::to_string(&message).unwrap();
        consume_deserialized_message::<TMessage>(
            &self.catalog,
            ConsumerFilter::AllConsumers,
            &content_json,
        )
        .await
        .unwrap();
    }

    fn add_outbox_dataset_deleted_expectation(mock_outbox: &mut MockOutbox, times: usize) {
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE),
                function(|message_as_json: &serde_json::Value| {
                    matches!(
                        serde_json::from_value::<DatasetLifecycleMessage>(message_as_json.clone()),
                        Ok(DatasetLifecycleMessage::Deleted(_))
                    )
                }),
            )
            .times(times)
            .returning(|_, _| Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
