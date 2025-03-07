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
use opendatafabric_dataset_impl::{DatasetStorageUnitS3, S3RegistryCache};
use s3_utils::S3Context;
use test_utils::LocalS3Server;
use time_source::SystemTimeSourceDefault;

use super::test_dataset_storage_unit_shared;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct S3StorageUnitHarness {
    _catalog: dill::Catalog,
    storage_unit: Arc<DatasetStorageUnitS3>,
}

impl S3StorageUnitHarness {
    pub async fn create(s3: &LocalS3Server, registry_caching: bool) -> Self {
        let s3_context = S3Context::from_url(&s3.url).await;

        let mut b = dill::CatalogBuilder::new();

        b.add::<SystemTimeSourceDefault>()
            .add_builder(DatasetStorageUnitS3::builder().with_s3_context(s3_context))
            .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitS3>()
            .bind::<dyn odf::DatasetStorageUnitWriter, DatasetStorageUnitS3>()
            .add::<odf::dataset::DatasetDefaultS3Builder>()
            .bind::<dyn odf::dataset::DatasetS3Builder, odf::dataset::DatasetDefaultS3Builder>();

        if registry_caching {
            b.add::<S3RegistryCache>();
        }

        let catalog = b.build();

        Self {
            storage_unit: catalog.get_one().unwrap(),
            _catalog: catalog,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_store_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, false).await;

    test_dataset_storage_unit_shared::test_store_dataset(harness.storage_unit.as_ref()).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_dataset() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, false).await;

    test_dataset_storage_unit_shared::test_delete_dataset(harness.storage_unit.as_ref()).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_iterate_datasets() {
    let s3 = LocalS3Server::new().await;
    let harness = S3StorageUnitHarness::create(&s3, false).await;

    test_dataset_storage_unit_shared::test_iterate_datasets(harness.storage_unit.as_ref()).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
