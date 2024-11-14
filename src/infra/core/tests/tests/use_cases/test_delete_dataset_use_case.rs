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
use kamu::{
    DeleteDatasetUseCaseImpl,
    DependencyGraphRepositoryInMemory,
    DependencyGraphServiceInMemory,
};
use kamu_core::auth::DatasetActionAuthorizer;
use kamu_core::{
    CreateDatasetResult,
    DatasetLifecycleMessage,
    DatasetRepository,
    DeleteDatasetError,
    DeleteDatasetUseCase,
    DependencyGraphService,
    GetDatasetError,
    TenancyConfig,
};
use messaging_outbox::{consume_deserialized_message, ConsumerFilter, Message, MockOutbox, Outbox};
use opendatafabric::{DatasetAlias, DatasetName, DatasetRef};

use crate::tests::use_cases::*;
use crate::BaseRepoHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_success_via_ref() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let mut mock_outbox = MockOutbox::new();
    expect_outbox_dataset_deleted(&mut mock_outbox, 1);

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, true);

    let harness = DeleteUseCaseHarness::new(mock_authorizer, mock_outbox);

    harness.create_root_dataset(&alias_foo).await;
    harness.dependencies_eager_initialization().await;

    harness
        .use_case
        .execute_via_ref(&alias_foo.as_local_ref())
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
    expect_outbox_dataset_deleted(&mut mock_outbox, 1);

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, true);

    let harness = DeleteUseCaseHarness::new(mock_authorizer, mock_outbox);

    let create_result_foo = harness.create_root_dataset(&alias_foo).await;
    harness.dependencies_eager_initialization().await;

    harness
        .use_case
        .execute_via_handle(&create_result_foo.dataset_handle)
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
            .execute_via_ref(&alias_foo.as_local_ref())
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
            .execute_via_handle(&create_result_foo.dataset_handle)
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
    expect_outbox_dataset_deleted(&mut mock_outbox, 2);

    let harness = DeleteUseCaseHarness::new(MockDatasetActionAuthorizer::allowing(), mock_outbox);

    let create_result_root = harness.create_root_dataset(&alias_foo).await;
    let create_result_derived = harness
        .create_derived_dataset(&alias_bar, vec![alias_foo.as_local_ref()])
        .await;
    harness.dependencies_eager_initialization().await;

    assert_matches!(
        harness.use_case.execute_via_handle(&create_result_root.dataset_handle).await,
        Err(DeleteDatasetError::DanglingReference(e)) if e.children == vec![create_result_derived.dataset_handle.clone()]
    );

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));

    harness
        .use_case
        .execute_via_handle(&create_result_derived.dataset_handle)
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
        .execute_via_handle(&create_result_root.dataset_handle)
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
    base_repo_harness: BaseRepoHarness,
    catalog: Catalog,
    use_case: Arc<dyn DeleteDatasetUseCase>,
}

impl DeleteUseCaseHarness {
    fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_outbox: MockOutbox,
    ) -> Self {
        let base_repo_harness = BaseRepoHarness::new(TenancyConfig::SingleTenant);

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<DeleteDatasetUseCaseImpl>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(mock_dataset_action_authorizer)
            .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
            .add_value(mock_outbox)
            .bind::<dyn Outbox, MockOutbox>()
            .build();

        let use_case = catalog.get_one::<dyn DeleteDatasetUseCase>().unwrap();

        Self {
            base_repo_harness,
            catalog,
            use_case,
        }
    }

    #[inline]
    async fn create_root_dataset(&self, alias: &DatasetAlias) -> CreateDatasetResult {
        self.base_repo_harness.create_root_dataset(alias).await
    }

    #[inline]
    async fn create_derived_dataset(
        &self,
        alias: &DatasetAlias,
        input_dataset_refs: Vec<DatasetRef>,
    ) -> CreateDatasetResult {
        self.base_repo_harness
            .create_derived_dataset(alias, input_dataset_refs)
            .await
    }

    #[inline]
    async fn check_dataset_exists(&self, alias: &DatasetAlias) -> Result<(), GetDatasetError> {
        self.base_repo_harness.check_dataset_exists(alias).await
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
