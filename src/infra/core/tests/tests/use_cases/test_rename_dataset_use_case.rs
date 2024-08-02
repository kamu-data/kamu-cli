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
use kamu::{DatasetRepositoryLocalFs, DatasetRepositoryWriter, RenameDatasetUseCaseImpl};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::auth::DatasetActionAuthorizer;
use kamu_core::{
    CreateDatasetResult,
    DatasetRepository,
    GetDatasetError,
    RenameDatasetError,
    RenameDatasetUseCase,
};
use opendatafabric::{DatasetAlias, DatasetKind, DatasetName};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_success_via_ref() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let alias_bar = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, true);

    let harness = RenameUseCaseHarness::new(mock_authorizer);
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
    let harness = RenameUseCaseHarness::new(MockDatasetActionAuthorizer::new());

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
    _temp_dir: tempfile::TempDir,
    catalog: Catalog,
    use_case: Arc<dyn RenameDatasetUseCase>,
}

impl RenameUseCaseHarness {
    fn new(mock_dataset_action_authorizer: MockDatasetActionAuthorizer) -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<RenameDatasetUseCaseImpl>()
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
            .build();

        let use_case = catalog.get_one::<dyn RenameDatasetUseCase>().unwrap();

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

    async fn check_dataset_exists(&self, alias: &DatasetAlias) -> Result<(), GetDatasetError> {
        let dataset_repo = self.catalog.get_one::<dyn DatasetRepository>().unwrap();
        dataset_repo
            .find_dataset_by_ref(&alias.as_local_ref())
            .await?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
