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
use domain::{DatasetRepository, DependencyGraphService, SystemTimeSourceDefault};
use kamu::*;
use kamu_accounts::{CurrentAccountSubject, DEFAULT_ACCOUNT_NAME};
use kamu_core::auth::AlwaysHappyDatasetActionAuthorizer;
use kamu_core::{CreateDatasetFromSnapshotUseCase, DeleteDatasetUseCase};
use messaging_outbox::OutboxImmediateImpl;
use tempfile::TempDir;

use super::test_dataset_repository_shared;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct LocalFsRepoHarness {
    catalog: dill::Catalog,
    dataset_repo: Arc<DatasetRepositoryLocalFs>,
    create_dataset_from_snapshot: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    delete_dataset: Arc<dyn DeleteDatasetUseCase>,
}

impl LocalFsRepoHarness {
    pub fn create(tempdir: &TempDir, multi_tenant: bool) -> Self {
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<SystemTimeSourceDefault>()
            .add::<OutboxImmediateImpl>()
            .add::<CoreMessageConsumerMediator>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(CurrentAccountSubject::new_test())
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(multi_tenant),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add::<AlwaysHappyDatasetActionAuthorizer>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<DeleteDatasetUseCaseImpl>()
            .build();

        let dataset_repo = catalog.get_one().unwrap();

        let create_dataset_from_snapshot = catalog.get_one().unwrap();
        let delete_dataset = catalog.get_one().unwrap();

        Self {
            catalog,
            dataset_repo,
            create_dataset_from_snapshot,
            delete_dataset,
        }
    }

    pub async fn dependencies_eager_initialization(&self) {
        let dependency_graph_service = self
            .catalog
            .get_one::<dyn DependencyGraphService>()
            .unwrap();
        dependency_graph_service
            .eager_initialization(&DependencyGraphRepositoryInMemory::new(
                self.dataset_repo.clone(),
            ))
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, false);

    test_dataset_repository_shared::test_create_dataset(harness.dataset_repo.as_ref(), None).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, true);

    test_dataset_repository_shared::test_create_dataset(
        harness.dataset_repo.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset_same_name_multiple_tenants() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, true);

    test_dataset_repository_shared::test_create_dataset_same_name_multiple_tenants(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset_from_snapshot() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, false);

    test_dataset_repository_shared::test_create_dataset_from_snapshot(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset_from_snapshot_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, true);

    test_dataset_repository_shared::test_create_dataset_from_snapshot(
        harness.dataset_repo.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, false);

    test_dataset_repository_shared::test_rename_dataset(harness.dataset_repo.as_ref(), None).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, true);

    test_dataset_repository_shared::test_rename_dataset(
        harness.dataset_repo.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_same_name_multiple_tenants() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, true);

    test_dataset_repository_shared::test_rename_dataset_same_name_multiple_tenants(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[ignore = "Need to migrate authorization to use case level tests"]
#[tokio::test]
async fn test_rename_unauthorized() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, true);

    test_dataset_repository_shared::test_rename_dataset_unauthorized(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, false);
    harness.dependencies_eager_initialization().await;

    test_dataset_repository_shared::test_delete_dataset(
        harness.dataset_repo.as_ref(),
        harness.create_dataset_from_snapshot.as_ref(),
        harness.delete_dataset.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, true);
    harness.dependencies_eager_initialization().await;

    test_dataset_repository_shared::test_delete_dataset(
        harness.dataset_repo.as_ref(),
        harness.create_dataset_from_snapshot.as_ref(),
        harness.delete_dataset.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[ignore = "Need to migrate authorization to use case level tests"]
#[tokio::test]
async fn test_delete_unauthorized() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, true);
    harness.dependencies_eager_initialization().await;

    test_dataset_repository_shared::test_delete_dataset_unauthorized(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iterate_datasets() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, false);

    test_dataset_repository_shared::test_iterate_datasets(harness.dataset_repo.as_ref()).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iterate_datasets_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, true);

    test_dataset_repository_shared::test_iterate_datasets_multi_tenant(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_and_get_case_insensetive_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, false);

    test_dataset_repository_shared::test_create_and_get_case_insensetive_dataset(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_and_get_case_insensetive_dataset_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, true);

    test_dataset_repository_shared::test_create_and_get_case_insensetive_dataset(
        harness.dataset_repo.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
