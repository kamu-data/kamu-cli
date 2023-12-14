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
use event_bus::EventBus;
use kamu::domain::{auth, CurrentAccountSubject};
use kamu::testing::{LocalS3Server, MockDatasetActionAuthorizer};
use kamu::utils::s3_context::S3Context;
use kamu::{DatasetRepositoryS3, DependencyGraphServiceInMemory};
use kamu_core::{DatasetRepository, DependencyGraphService, DependencyGraphServiceInitializer};
use opendatafabric::AccountName;

use super::test_dataset_repository_shared;

/////////////////////////////////////////////////////////////////////////////////////////

struct S3RepoHarness {
    catalog: dill::Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
}

impl S3RepoHarness {
    pub async fn create<TDatasetActionAuthorizer: auth::DatasetActionAuthorizer + 'static>(
        s3: &LocalS3Server,
        dataset_action_authorizer: TDatasetActionAuthorizer,
        multi_tenant: bool,
    ) -> Self {
        let s3_context = S3Context::from_url(&s3.url).await;

        let catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(dataset_action_authorizer)
            .bind::<dyn auth::DatasetActionAuthorizer, TDatasetActionAuthorizer>()
            .add_builder(
                DatasetRepositoryS3::builder()
                    .with_s3_context(s3_context)
                    .with_multi_tenant(multi_tenant),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryS3>()
            .build();

        let dataset_repo = catalog.get_one().unwrap();

        Self {
            catalog,
            dataset_repo,
        }
    }

    pub async fn dependencies_eager_initialization(&self) {
        let dependency_graph_service = self
            .catalog
            .get_one::<dyn DependencyGraphService>()
            .unwrap();
        dependency_graph_service
            .eager_initialization(&DependencyGraphServiceInitializer::new(
                self.dataset_repo.clone(),
            ))
            .await
            .unwrap();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness =
        S3RepoHarness::create(&s3, auth::AlwaysHappyDatasetActionAuthorizer::new(), false).await;

    test_dataset_repository_shared::test_create_dataset(harness.dataset_repo.as_ref(), None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness =
        S3RepoHarness::create(&s3, auth::AlwaysHappyDatasetActionAuthorizer::new(), true).await;

    test_dataset_repository_shared::test_create_dataset(
        harness.dataset_repo.as_ref(),
        Some(AccountName::new_unchecked(auth::DEFAULT_ACCOUNT_NAME)),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_same_name_multiple_tenants() {
    let s3 = LocalS3Server::new().await;
    let harness =
        S3RepoHarness::create(&s3, auth::AlwaysHappyDatasetActionAuthorizer::new(), true).await;

    test_dataset_repository_shared::test_create_dataset_same_name_multiple_tenants(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_from_snapshot() {
    let s3 = LocalS3Server::new().await;
    let harness =
        S3RepoHarness::create(&s3, auth::AlwaysHappyDatasetActionAuthorizer::new(), false).await;

    test_dataset_repository_shared::test_create_dataset_from_snapshot(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_from_snapshot_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness =
        S3RepoHarness::create(&s3, auth::AlwaysHappyDatasetActionAuthorizer::new(), true).await;

    test_dataset_repository_shared::test_create_dataset_from_snapshot(
        harness.dataset_repo.as_ref(),
        Some(AccountName::new_unchecked(auth::DEFAULT_ACCOUNT_NAME)),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(
        &s3,
        MockDatasetActionAuthorizer::new().expect_check_write_a_dataset(1),
        false,
    )
    .await;

    test_dataset_repository_shared::test_rename_dataset(harness.dataset_repo.as_ref(), None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(
        &s3,
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

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset_same_name_multiple_tenants() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(
        &s3,
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

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_unauthorized() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, MockDatasetActionAuthorizer::denying(), true).await;

    test_dataset_repository_shared::test_rename_dataset_unauthroized(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness =
        S3RepoHarness::create(&s3, auth::AlwaysHappyDatasetActionAuthorizer::new(), false).await;
    harness.dependencies_eager_initialization().await;

    test_dataset_repository_shared::test_delete_dataset(harness.dataset_repo.as_ref(), None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness =
        S3RepoHarness::create(&s3, auth::AlwaysHappyDatasetActionAuthorizer::new(), true).await;
    harness.dependencies_eager_initialization().await;

    test_dataset_repository_shared::test_delete_dataset(
        harness.dataset_repo.as_ref(),
        Some(AccountName::new_unchecked(auth::DEFAULT_ACCOUNT_NAME)),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_unauthorized() {
    let s3: LocalS3Server = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, MockDatasetActionAuthorizer::denying(), true).await;
    harness.dependencies_eager_initialization().await;

    test_dataset_repository_shared::test_delete_dataset_unauthroized(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_iterate_datasets() {
    let s3 = LocalS3Server::new().await;
    let harness =
        S3RepoHarness::create(&s3, auth::AlwaysHappyDatasetActionAuthorizer::new(), false).await;

    test_dataset_repository_shared::test_iterate_datasets(harness.dataset_repo.as_ref()).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_iterate_datasets_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness =
        S3RepoHarness::create(&s3, auth::AlwaysHappyDatasetActionAuthorizer::new(), true).await;

    test_dataset_repository_shared::test_iterate_datasets_multi_tenant(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////
