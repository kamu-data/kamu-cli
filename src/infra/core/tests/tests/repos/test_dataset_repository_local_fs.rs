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
use domain::DatasetRepository;
use kamu::*;
use kamu_accounts::{CurrentAccountSubject, DEFAULT_ACCOUNT_NAME};
use kamu_core::{CreateDatasetFromSnapshotUseCase, DidGeneratorDefault, TenancyConfig};
use messaging_outbox::{Outbox, OutboxImmediateImpl};
use tempfile::TempDir;
use time_source::SystemTimeSourceDefault;

use super::test_dataset_repository_shared;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct LocalFsRepoHarness {
    _catalog: dill::Catalog,
    dataset_repo: Arc<DatasetRepositoryLocalFs>,
    create_dataset_from_snapshot: Arc<dyn CreateDatasetFromSnapshotUseCase>,
}

impl LocalFsRepoHarness {
    pub fn create(tempdir: &TempDir, tenancy_config: TenancyConfig) -> Self {
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

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
            .add_builder(DatasetRepositoryLocalFs::builder().with_root(datasets_dir))
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>();

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

#[tokio::test]
async fn test_create_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::SingleTenant);

    test_dataset_repository_shared::test_create_dataset(harness.dataset_repo.as_ref(), None).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::MultiTenant);

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
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::MultiTenant);

    test_dataset_repository_shared::test_create_dataset_same_name_multiple_tenants(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset_from_snapshot() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::SingleTenant);

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
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::MultiTenant);

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
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::SingleTenant);

    test_dataset_repository_shared::test_rename_dataset(harness.dataset_repo.as_ref(), None).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::MultiTenant);

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
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::MultiTenant);

    test_dataset_repository_shared::test_rename_dataset_same_name_multiple_tenants(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::SingleTenant);

    test_dataset_repository_shared::test_delete_dataset(
        harness.dataset_repo.as_ref(),
        harness.create_dataset_from_snapshot.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::MultiTenant);

    test_dataset_repository_shared::test_delete_dataset(
        harness.dataset_repo.as_ref(),
        harness.create_dataset_from_snapshot.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iterate_datasets() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::SingleTenant);

    test_dataset_repository_shared::test_iterate_datasets(harness.dataset_repo.as_ref()).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iterate_datasets_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::MultiTenant);

    test_dataset_repository_shared::test_iterate_datasets_multi_tenant(
        harness.dataset_repo.as_ref(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_and_get_case_insensetive_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::SingleTenant);

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
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::MultiTenant);

    test_dataset_repository_shared::test_create_and_get_case_insensetive_dataset(
        harness.dataset_repo.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_multiple_datasets_with_same_id() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::SingleTenant);

    test_dataset_repository_shared::test_create_multiple_datasets_with_same_id(
        harness.dataset_repo.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_multiple_datasets_with_same_id_multi_tenant() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsRepoHarness::create(&tempdir, TenancyConfig::MultiTenant);

    test_dataset_repository_shared::test_create_multiple_datasets_with_same_id(
        harness.dataset_repo.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
