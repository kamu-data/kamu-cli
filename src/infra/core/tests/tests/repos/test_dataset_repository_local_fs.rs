// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::Component;
use domain::{auth, CurrentAccountSubject, DatasetRepository, DependencyGraphServiceInitializer};
use event_bus::EventBus;
use kamu::testing::MockDatasetActionAuthorizer;
use kamu::*;
use opendatafabric::AccountName;
use tempfile::TempDir;

use super::test_dataset_repository_shared;

/////////////////////////////////////////////////////////////////////////////////////////

struct LocalFsRepoHarness {
    _catalog: dill::Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
}

impl LocalFsRepoHarness {
    pub async fn create<TDatasetActionAuthorizer: auth::DatasetActionAuthorizer + 'static>(
        tempdir: &TempDir,
        dataset_action_authorizer: TDatasetActionAuthorizer,
        multi_tenant: bool,
    ) -> Self {
        let catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(dataset_action_authorizer)
            .bind::<dyn auth::DatasetActionAuthorizer, TDatasetActionAuthorizer>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(tempdir.path().join("datasets"))
                    .with_multi_tenant(multi_tenant),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .build();

        let dependency_graph_initializer = catalog
            .get_one::<dyn DependencyGraphServiceInitializer>()
            .unwrap();
        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

        dependency_graph_initializer
            .full_scan(dataset_repo.as_ref(), false)
            .await
            .unwrap();

        Self {
            _catalog: catalog,
            dataset_repo,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(
        &tempdir,
        auth::AlwaysHappyDatasetActionAuthorizer::new(),
        false,
    )
    .await;

    test_dataset_repository_shared::test_create_dataset(harness.dataset_repo.as_ref(), None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(
        &tempdir,
        auth::AlwaysHappyDatasetActionAuthorizer::new(),
        true,
    )
    .await;

    test_dataset_repository_shared::test_create_dataset(
        harness.dataset_repo.as_ref(),
        Some(AccountName::new_unchecked(auth::DEFAULT_ACCOUNT_NAME)),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset_same_name_multiple_tenants() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(
        &tempdir,
        auth::AlwaysHappyDatasetActionAuthorizer::new(),
        true,
    )
    .await;

    test_dataset_repository_shared::test_create_dataset_same_name_multiple_tenants(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset_from_snapshot() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(
        &tempdir,
        auth::AlwaysHappyDatasetActionAuthorizer::new(),
        false,
    )
    .await;

    test_dataset_repository_shared::test_create_dataset_from_snapshot(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset_from_snapshot_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(
        &tempdir,
        auth::AlwaysHappyDatasetActionAuthorizer::new(),
        true,
    )
    .await;

    test_dataset_repository_shared::test_create_dataset_from_snapshot(
        harness.dataset_repo.as_ref(),
        Some(AccountName::new_unchecked(auth::DEFAULT_ACCOUNT_NAME)),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(
        &tempdir,
        MockDatasetActionAuthorizer::new().expect_check_write_a_dataset(1),
        false,
    )
    .await;

    test_dataset_repository_shared::test_rename_dataset(harness.dataset_repo.as_ref(), None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(
        &tempdir,
        MockDatasetActionAuthorizer::new().expect_check_write_a_dataset(1),
        true,
    )
    .await;

    test_dataset_repository_shared::test_rename_dataset(
        harness.dataset_repo.as_ref(),
        Some(AccountName::new_unchecked(auth::DEFAULT_ACCOUNT_NAME)),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_same_name_multiple_tenants() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(
        &tempdir,
        MockDatasetActionAuthorizer::new().expect_check_write_a_dataset(1),
        true,
    )
    .await;

    test_dataset_repository_shared::test_rename_dataset_same_name_multiple_tenants(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_unauthorized() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness =
        LocalFsRepoHarness::create(&tempdir, MockDatasetActionAuthorizer::denying(), true).await;

    test_dataset_repository_shared::test_rename_dataset_unauthroized(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(
        &tempdir,
        auth::AlwaysHappyDatasetActionAuthorizer::new(),
        false,
    )
    .await;

    test_dataset_repository_shared::test_delete_dataset(harness.dataset_repo.as_ref(), None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(
        &tempdir,
        auth::AlwaysHappyDatasetActionAuthorizer::new(),
        true,
    )
    .await;

    test_dataset_repository_shared::test_delete_dataset(
        harness.dataset_repo.as_ref(),
        Some(AccountName::new_unchecked(auth::DEFAULT_ACCOUNT_NAME)),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_unauthorized() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness =
        LocalFsRepoHarness::create(&tempdir, MockDatasetActionAuthorizer::denying(), true).await;

    test_dataset_repository_shared::test_delete_dataset_unauthroized(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iterate_datasets() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(
        &tempdir,
        auth::AlwaysHappyDatasetActionAuthorizer::new(),
        false,
    )
    .await;

    test_dataset_repository_shared::test_iterate_datasets(harness.dataset_repo.as_ref()).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iterate_datasets_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(
        &tempdir,
        auth::AlwaysHappyDatasetActionAuthorizer::new(),
        true,
    )
    .await;

    test_dataset_repository_shared::test_iterate_datasets_multi_tenant(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////
