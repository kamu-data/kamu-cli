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
use kamu::{CommitDatasetEventUseCaseImpl, DatasetRepositoryLocalFs, DatasetRepositoryWriter};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::auth::DatasetActionAuthorizer;
use kamu_core::{
    CommitDatasetEventUseCase,
    CommitError,
    CommitOpts,
    CreateDatasetResult,
    DatasetLifecycleMessage,
    DatasetRepository,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{MockOutbox, Outbox};
use mockall::predicate::{eq, function};
use opendatafabric::{DatasetAlias, DatasetKind, DatasetName, MetadataEvent};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_dataset_event() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, true);

    let mock_outbox = MockOutbox::new();

    let harness = CommitDatasetEventUseCaseHarness::new(mock_authorizer, mock_outbox);
    let create_result_foo = harness.create_dataset(&alias_foo, DatasetKind::Root).await;

    let res = harness
        .use_case
        .execute(
            &create_result_foo.dataset_handle,
            MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
            CommitOpts::default(),
        )
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_event_unauthorized() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, false);

    let mock_outbox = MockOutbox::new();

    let harness = CommitDatasetEventUseCaseHarness::new(mock_authorizer, mock_outbox);
    let create_result_foo = harness.create_dataset(&alias_foo, DatasetKind::Root).await;

    let res = harness
        .use_case
        .execute(
            &create_result_foo.dataset_handle,
            MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
            CommitOpts::default(),
        )
        .await;
    assert_matches!(res, Err(CommitError::Access(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_event_with_new_dependencies() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let alias_bar = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_bar, 1, true);

    let mut mock_outbox = MockOutbox::new();
    CommitDatasetEventUseCaseHarness::add_outbox_dataset_dependencies_updated_expectation(
        &mut mock_outbox,
        1,
    );

    let harness = CommitDatasetEventUseCaseHarness::new(mock_authorizer, mock_outbox);
    let create_result_foo = harness.create_dataset(&alias_foo, DatasetKind::Root).await;
    let create_result_bar = harness
        .create_dataset(&alias_bar, DatasetKind::Derivative)
        .await;

    let res = harness
        .use_case
        .execute(
            &create_result_bar.dataset_handle,
            MetadataEvent::SetTransform(
                MetadataFactory::set_transform()
                    .inputs_from_refs_and_aliases(vec![(
                        create_result_foo.dataset_handle.id,
                        alias_foo.to_string(),
                    )])
                    .build(),
            ),
            CommitOpts::default(),
        )
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CommitDatasetEventUseCaseHarness {
    _temp_dir: tempfile::TempDir,
    catalog: Catalog,
    use_case: Arc<dyn CommitDatasetEventUseCase>,
}

impl CommitDatasetEventUseCaseHarness {
    fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_outbox: MockOutbox,
    ) -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<CommitDatasetEventUseCaseImpl>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(mock_dataset_action_authorizer)
            .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
            .add::<SystemTimeSourceDefault>()
            .add_value(mock_outbox)
            .bind::<dyn Outbox, MockOutbox>()
            .build();

        let use_case = catalog.get_one::<dyn CommitDatasetEventUseCase>().unwrap();

        Self {
            _temp_dir: tempdir,
            catalog,
            use_case,
        }
    }

    async fn create_dataset(&self, alias: &DatasetAlias, kind: DatasetKind) -> CreateDatasetResult {
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(kind)
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

    fn add_outbox_dataset_dependencies_updated_expectation(
        mock_outbox: &mut MockOutbox,
        times: usize,
    ) {
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE),
                function(|message_as_json: &serde_json::Value| {
                    matches!(
                        serde_json::from_value::<DatasetLifecycleMessage>(message_as_json.clone()),
                        Ok(DatasetLifecycleMessage::DependenciesUpdated(_))
                    )
                }),
                eq(1),
            )
            .times(times)
            .returning(|_, _, _| Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
