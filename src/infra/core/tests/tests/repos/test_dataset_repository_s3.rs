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
    CreateDatasetFromSnapshotUseCaseImpl,
    DatasetRepositoryS3,
    DatasetRepositoryWriter,
    S3RegistryCache,
};
use kamu_accounts::{CurrentAccountSubject, DEFAULT_ACCOUNT_NAME};
use kamu_core::{
    CreateDatasetFromSnapshotUseCase,
    DatasetRepository,
    DidGeneratorDefault,
    TenancyConfig,
};
use messaging_outbox::{Outbox, OutboxImmediateImpl};
use time_source::SystemTimeSourceDefault;

use super::test_dataset_repository_shared;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct S3RepoHarness {
    _catalog: dill::Catalog,
    dataset_repo: Arc<DatasetRepositoryS3>,
    create_dataset_from_snapshot: Arc<dyn CreateDatasetFromSnapshotUseCase>,
}

impl S3RepoHarness {
    pub async fn create(
        s3: &LocalS3Server,
        tenancy_config: TenancyConfig,
        registry_caching: bool,
    ) -> Self {
        let s3_context = S3Context::from_url(&s3.url).await;

        let mut b = dill::CatalogBuilder::new();

        b.add::<SystemTimeSourceDefault>()
            .add::<DidGeneratorDefault>()
            .add_builder(
                messaging_outbox::OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            )
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(tenancy_config)
            .add_builder(DatasetRepositoryS3::builder().with_s3_context(s3_context))
            .bind::<dyn DatasetRepository, DatasetRepositoryS3>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryS3>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>();

        if registry_caching {
            b.add::<S3RegistryCache>();
        }

        let catalog = b.build();

        let dataset_repo = catalog.get_one().unwrap();

        let create_dataset_from_snapshot = catalog.get_one().unwrap();

        Self {
            _catalog: catalog,
            dataset_repo,
            create_dataset_from_snapshot,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, TenancyConfig::SingleTenant, false).await;

    test_dataset_repository_shared::test_create_dataset(harness.dataset_repo.as_ref(), None).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

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
    let harness = S3RepoHarness::create(&s3, TenancyConfig::MultiTenant, true).await;

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
    let harness = S3RepoHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

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
    let harness = S3RepoHarness::create(&s3, TenancyConfig::SingleTenant, false).await;

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
    let harness = S3RepoHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

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
    let harness = S3RepoHarness::create(&s3, TenancyConfig::SingleTenant, false).await;

    test_dataset_repository_shared::test_rename_dataset(harness.dataset_repo.as_ref(), None).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

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
    let harness = S3RepoHarness::create(&s3, TenancyConfig::MultiTenant, true).await;

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
    let harness = S3RepoHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

    test_dataset_repository_shared::test_rename_dataset_same_name_multiple_tenants(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, TenancyConfig::SingleTenant, false).await;

    test_dataset_repository_shared::test_delete_dataset(
        harness.dataset_repo.as_ref(),
        harness.create_dataset_from_snapshot.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

    test_dataset_repository_shared::test_delete_dataset(
        harness.dataset_repo.as_ref(),
        harness.create_dataset_from_snapshot.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_iterate_datasets() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, TenancyConfig::SingleTenant, false).await;

    test_dataset_repository_shared::test_iterate_datasets(harness.dataset_repo.as_ref()).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_iterate_datasets_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3RepoHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

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
    let harness = S3RepoHarness::create(&s3, TenancyConfig::SingleTenant, false).await;

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
    let harness = S3RepoHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

    test_dataset_repository_shared::test_create_and_get_case_insensetive_dataset(
        harness.dataset_repo.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
