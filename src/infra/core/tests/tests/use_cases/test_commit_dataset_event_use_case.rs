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
use kamu::testing::{MetadataFactory, MockDatasetActionAuthorizer};
use kamu::CommitDatasetEventUseCaseImpl;
use kamu_core::auth::DatasetActionAuthorizer;
use kamu_core::{
    CommitDatasetEventUseCase,
    CommitError,
    CommitOpts,
    CreateDatasetResult,
    TenancyConfig,
};
use messaging_outbox::{MockOutbox, Outbox};
use opendatafabric::{DatasetAlias, DatasetName, DatasetRef, MetadataEvent};

use crate::tests::use_cases::*;
use crate::BaseRepoHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_dataset_event() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, true);

    let mock_outbox = MockOutbox::new();

    let harness = CommitDatasetEventUseCaseHarness::new(mock_authorizer, mock_outbox);
    let created_foo = harness.create_root_dataset(&alias_foo).await;

    let res = harness
        .use_case
        .execute(
            &created_foo.dataset_handle,
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
    let created_foo = harness.create_root_dataset(&alias_foo).await;

    let res = harness
        .use_case
        .execute(
            &created_foo.dataset_handle,
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
    expect_outbox_dataset_dependencies_updated(&mut mock_outbox, 1);

    let harness = CommitDatasetEventUseCaseHarness::new(mock_authorizer, mock_outbox);
    let created_foo = harness.create_root_dataset(&alias_foo).await;
    let created_bar = harness
        .create_derived_dataset(&alias_bar, vec![created_foo.dataset_handle.as_local_ref()])
        .await;

    let res = harness
        .use_case
        .execute(
            &created_bar.dataset_handle,
            MetadataEvent::SetTransform(
                MetadataFactory::set_transform()
                    .inputs_from_refs_and_aliases(vec![(
                        created_foo.dataset_handle.id,
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
    base_repo_harness: BaseRepoHarness,
    _catalog: Catalog,
    use_case: Arc<dyn CommitDatasetEventUseCase>,
}

impl CommitDatasetEventUseCaseHarness {
    fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_outbox: MockOutbox,
    ) -> Self {
        let base_repo_harness = BaseRepoHarness::new(TenancyConfig::SingleTenant);

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<CommitDatasetEventUseCaseImpl>()
            .add_value(mock_dataset_action_authorizer)
            .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
            .add_value(mock_outbox)
            .bind::<dyn Outbox, MockOutbox>()
            .build();

        let use_case = catalog.get_one::<dyn CommitDatasetEventUseCase>().unwrap();

        Self {
            base_repo_harness,
            _catalog: catalog,
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
