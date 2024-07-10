// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use kamu::testing::LocalS3Server;
use kamu::utils::s3_context::S3Context;
use kamu::{
    CoreMessageConsumerMediator,
    CreateDatasetFromSnapshotUseCaseImpl,
    DatasetRepositoryS3,
    DatasetRepositoryWriter,
    DeleteDatasetUseCaseImpl,
    DependencyGraphRepositoryInMemory,
    DependencyGraphServiceInMemory,
    S3RegistryCache,
};
use kamu_accounts::{CurrentAccountSubject, DEFAULT_ACCOUNT_NAME};
use kamu_core::auth::AlwaysHappyDatasetActionAuthorizer;
use kamu_core::{
    CreateDatasetFromSnapshotUseCase,
    DatasetRepository,
    DeleteDatasetUseCase,
    DependencyGraphService,
    SystemTimeSourceDefault,
};
use messaging_outbox::OutboxImmediateImpl;

use super::test_dataset_repository_shared;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct S3RepoHarness {
    catalog: dill::Catalog,
    dataset_repo: Arc<DatasetRepositoryS3>,
    create_dataset_from_snapshot: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    delete_dataset: Arc<dyn DeleteDatasetUseCase>,
}

impl S3RepoHarness {
    pub async fn create(s3: &LocalS3Server, multi_tenant: bool, registry_caching: bool) -> Self {
        let s3_context = S3Context::from_url(&s3.url).await;

        let mut catalog = dill::CatalogBuilder::new();

        catalog
            .add::<SystemTimeSourceDefault>()
            .add::<OutboxImmediateImpl>()
            .add::<CoreMessageConsumerMediator>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<AlwaysHappyDatasetActionAuthorizer>()
            .add_builder(
                DatasetRepositoryS3::builder()
                    .with_s3_context(s3_context)
                    .with_multi_tenant(multi_tenant),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryS3>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryS3>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<DeleteDatasetUseCaseImpl>();

        if registry_caching {
            catalog.add::<S3RegistryCache>();
        }

        let catalog = catalog.build();

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

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, false, false).await;

    test_dataset_repository_shared::test_create_dataset(harness.dataset_repo.as_ref(), None).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, true, false).await;

    test_dataset_repository_shared::test_create_dataset(
        harness.dataset_repo.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_multi_tenant_with_caching() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, true, true).await;

    test_dataset_repository_shared::test_create_dataset(
        harness.dataset_repo.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_same_name_multiple_tenants() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, true, false).await;

    test_dataset_repository_shared::test_create_dataset_same_name_multiple_tenants(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_from_snapshot() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, false, false).await;

    test_dataset_repository_shared::test_create_dataset_from_snapshot(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_from_snapshot_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, true, false).await;

    test_dataset_repository_shared::test_create_dataset_from_snapshot(
        harness.dataset_repo.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, false, false).await;

    test_dataset_repository_shared::test_rename_dataset(harness.dataset_repo.as_ref(), None).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, true, false).await;

    test_dataset_repository_shared::test_rename_dataset(
        harness.dataset_repo.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset_multi_tenant_with_caching() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, true, true).await;

    test_dataset_repository_shared::test_rename_dataset(
        harness.dataset_repo.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset_same_name_multiple_tenants() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, true, false).await;

    test_dataset_repository_shared::test_rename_dataset_same_name_multiple_tenants(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[ignore = "Need to migrate authorization to use case level tests"]
#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_unauthorized() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, true, false).await;

    test_dataset_repository_shared::test_rename_dataset_unauthorized(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, false, false).await;
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

#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, true, false).await;
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
#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_unauthorized() {
    let s3: LocalS3Server = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, true, false).await;
    harness.dependencies_eager_initialization().await;

    test_dataset_repository_shared::test_delete_dataset_unauthorized(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_iterate_datasets() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, false, false).await;

    test_dataset_repository_shared::test_iterate_datasets(harness.dataset_repo.as_ref()).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_iterate_datasets_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, true, false).await;

    test_dataset_repository_shared::test_iterate_datasets_multi_tenant(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_and_get_case_insensetive_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, false, false).await;

    test_dataset_repository_shared::test_create_and_get_case_insensetive_dataset(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_and_get_case_insensetive_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, true, false).await;

    test_dataset_repository_shared::test_create_and_get_case_insensetive_dataset(
        harness.dataset_repo.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
