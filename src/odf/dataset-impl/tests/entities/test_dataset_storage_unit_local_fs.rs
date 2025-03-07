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
use opendatafabric_dataset_impl::DatasetStorageUnitLocalFs;
use tempfile::TempDir;

use super::test_dataset_storage_unit_shared;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct LocalFsStorageUnitHarness {
    _catalog: dill::Catalog,
    storage_unit: Arc<DatasetStorageUnitLocalFs>,
}

impl LocalFsStorageUnitHarness {
    pub fn create(tempdir: &TempDir) -> Self {
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let mut b = dill::CatalogBuilder::new();
        b.add_builder(DatasetStorageUnitLocalFs::builder().with_root(datasets_dir))
            .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitLocalFs>()
            .bind::<dyn odf::DatasetStorageUnitWriter, DatasetStorageUnitLocalFs>()
            .add::<odf::dataset::DatasetDefaultLfsBuilder>()
            .bind::<dyn odf::dataset::DatasetLfsBuilder, odf::dataset::DatasetDefaultLfsBuilder>();

        let catalog = b.build();

        Self {
            storage_unit: catalog.get_one().unwrap(),
            _catalog: catalog,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_store_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsStorageUnitHarness::create(&tempdir);

    test_dataset_storage_unit_shared::test_store_dataset(harness.storage_unit.as_ref()).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsStorageUnitHarness::create(&tempdir);

    test_dataset_storage_unit_shared::test_delete_dataset(harness.storage_unit.as_ref()).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_iterate_datasets() {
    let tempdir = tempfile::tempdir().unwrap();
    let harness = LocalFsStorageUnitHarness::create(&tempdir);

    test_dataset_storage_unit_shared::test_iterate_datasets(harness.storage_unit.as_ref()).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
