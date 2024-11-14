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
use kamu::RenameDatasetUseCaseImpl;
use kamu_core::auth::DatasetActionAuthorizer;
use kamu_core::{
    CreateDatasetResult,
    DatasetLifecycleMessage,
    GetDatasetError,
    RenameDatasetError,
    RenameDatasetUseCase,
    TenancyConfig,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{MockOutbox, Outbox};
use mockall::predicate::{eq, function};
use opendatafabric::{DatasetAlias, DatasetName};

use crate::BaseRepoHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_success_via_ref() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let alias_bar = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, true);
    let mut mock_outbox = MockOutbox::new();
    RenameUseCaseHarness::add_outbox_dataset_renamed_expectation(&mut mock_outbox, 1);

    let harness = RenameUseCaseHarness::new(mock_authorizer, mock_outbox);
    harness.create_root_dataset(&alias_foo).await;

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(
        harness.check_dataset_exists(&alias_bar).await,
        Err(GetDatasetError::NotFound(_))
    );

    harness
        .use_case
        .execute(&alias_foo.as_local_ref(), &alias_bar.dataset_name)
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(GetDatasetError::NotFound(_))
    );
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_not_found() {
    let harness = RenameUseCaseHarness::new(MockDatasetActionAuthorizer::new(), MockOutbox::new());

    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    assert_matches!(
        harness
            .use_case
            .execute(
                &alias_foo.as_local_ref(),
                &DatasetName::new_unchecked("bar")
            )
            .await,
        Err(RenameDatasetError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_unauthorized() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let harness = RenameUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, false),
        MockOutbox::new(),
    );

    harness.create_root_dataset(&alias_foo).await;

    assert_matches!(
        harness
            .use_case
            .execute(
                &alias_foo.as_local_ref(),
                &DatasetName::new_unchecked("bar")
            )
            .await,
        Err(RenameDatasetError::Access(_))
    );

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct RenameUseCaseHarness {
    base_repo_harness: BaseRepoHarness,
    _catalog: Catalog,
    use_case: Arc<dyn RenameDatasetUseCase>,
}

impl RenameUseCaseHarness {
    fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_outbox: MockOutbox,
    ) -> Self {
        let base_repo_harness = BaseRepoHarness::new(TenancyConfig::SingleTenant);

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<RenameDatasetUseCaseImpl>()
            .add_value(mock_dataset_action_authorizer)
            .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
            .add_value(mock_outbox)
            .bind::<dyn Outbox, MockOutbox>()
            .build();

        let use_case = catalog.get_one::<dyn RenameDatasetUseCase>().unwrap();

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
    async fn check_dataset_exists(&self, alias: &DatasetAlias) -> Result<(), GetDatasetError> {
        self.base_repo_harness.check_dataset_exists(alias).await
    }

    fn add_outbox_dataset_renamed_expectation(mock_outbox: &mut MockOutbox, times: usize) {
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE),
                function(|message_as_json: &serde_json::Value| {
                    matches!(
                        serde_json::from_value::<DatasetLifecycleMessage>(message_as_json.clone()),
                        Ok(DatasetLifecycleMessage::Renamed(_))
                    )
                }),
            )
            .times(times)
            .returning(|_, _| Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
