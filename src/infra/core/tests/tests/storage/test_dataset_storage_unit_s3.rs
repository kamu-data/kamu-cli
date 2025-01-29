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
use kamu::{CreateDatasetFromSnapshotUseCaseImpl, DatasetStorageUnitS3, S3RegistryCache};
use kamu_accounts::{CurrentAccountSubject, DEFAULT_ACCOUNT_NAME};
use kamu_core::{
    CreateDatasetFromSnapshotUseCase,
    DatasetStorageUnitWriter,
    DidGeneratorDefault,
    TenancyConfig,
};
use messaging_outbox::{Outbox, OutboxImmediateImpl};
use s3_utils::S3Context;
use test_utils::LocalS3Server;
use time_source::SystemTimeSourceDefault;

use super::test_dataset_storage_unit_shared;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct S3StorageUnitHarness {
    _catalog: dill::Catalog,
    storage_unit: Arc<DatasetStorageUnitS3>,
    create_dataset_from_snapshot: Arc<dyn CreateDatasetFromSnapshotUseCase>,
}

impl S3StorageUnitHarness {
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
            .add_builder(DatasetStorageUnitS3::builder().with_s3_context(s3_context))
            .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitS3>()
            .bind::<dyn DatasetStorageUnitWriter, DatasetStorageUnitS3>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>();

        if registry_caching {
            b.add::<S3RegistryCache>();
        }

        let catalog = b.build();

        let storage_unit = catalog.get_one().unwrap();

        let create_dataset_from_snapshot = catalog.get_one().unwrap();

        Self {
            _catalog: catalog,
            storage_unit,
            create_dataset_from_snapshot,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::SingleTenant, false).await;

    test_dataset_storage_unit_shared::test_create_dataset(harness.storage_unit.as_ref(), None)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

    test_dataset_storage_unit_shared::test_create_dataset(
        harness.storage_unit.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_multi_tenant_with_caching() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::MultiTenant, true).await;

    test_dataset_storage_unit_shared::test_create_dataset(
        harness.storage_unit.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_same_name_multiple_tenants() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

    test_dataset_storage_unit_shared::test_create_dataset_same_name_multiple_tenants(
        harness.storage_unit.as_ref(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_from_snapshot() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::SingleTenant, false).await;

    test_dataset_storage_unit_shared::test_create_dataset_from_snapshot(
        harness.storage_unit.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_from_snapshot_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

    test_dataset_storage_unit_shared::test_create_dataset_from_snapshot(
        harness.storage_unit.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::SingleTenant, false).await;

    test_dataset_storage_unit_shared::test_rename_dataset(harness.storage_unit.as_ref(), None)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

    test_dataset_storage_unit_shared::test_rename_dataset(
        harness.storage_unit.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset_multi_tenant_with_caching() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::MultiTenant, true).await;

    test_dataset_storage_unit_shared::test_rename_dataset(
        harness.storage_unit.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset_same_name_multiple_tenants() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

    test_dataset_storage_unit_shared::test_rename_dataset_same_name_multiple_tenants(
        harness.storage_unit.as_ref(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::SingleTenant, false).await;

    test_dataset_storage_unit_shared::test_delete_dataset(
        harness.storage_unit.as_ref(),
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
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

    test_dataset_storage_unit_shared::test_delete_dataset(
        harness.storage_unit.as_ref(),
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
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::SingleTenant, false).await;

    test_dataset_storage_unit_shared::test_iterate_datasets(harness.storage_unit.as_ref()).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_iterate_datasets_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

    test_dataset_storage_unit_shared::test_iterate_datasets_multi_tenant(
        harness.storage_unit.as_ref(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_and_get_case_insensetive_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::SingleTenant, false).await;

    test_dataset_storage_unit_shared::test_create_and_get_case_insensetive_dataset(
        harness.storage_unit.as_ref(),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_and_get_case_insensetive_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, TenancyConfig::MultiTenant, false).await;

    test_dataset_storage_unit_shared::test_create_and_get_case_insensetive_dataset(
        harness.storage_unit.as_ref(),
        Some(DEFAULT_ACCOUNT_NAME.clone()),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
