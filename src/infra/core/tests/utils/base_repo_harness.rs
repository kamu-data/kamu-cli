// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{Catalog, Component};
use kamu::testing::MetadataFactory;
use kamu::{DatasetRegistryRepoBridge, DatasetRepositoryLocalFs, DatasetRepositoryWriter};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{
    CreateDatasetResult,
    DatasetRegistry,
    DatasetRepository,
    MetadataChainExt,
    TenancyConfig,
};
use opendatafabric::{DatasetAlias, DatasetKind, DatasetRef};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct BaseRepoHarness {
    _temp_dir: tempfile::TempDir,
    catalog: Catalog,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
}

impl BaseRepoHarness {
    pub fn new(tenancy_config: TenancyConfig) -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add_value(tenancy_config)
            .add_builder(DatasetRepositoryLocalFs::builder().with_root(datasets_dir))
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add::<DatasetRegistryRepoBridge>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<SystemTimeSourceDefault>()
            .build();

        let dataset_registry = catalog.get_one().unwrap();
        let dataset_repo_writer = catalog.get_one().unwrap();

        Self {
            _temp_dir: tempdir,
            catalog,
            dataset_registry,
            dataset_repo_writer,
        }
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn dataset_registry(&self) -> &dyn DatasetRegistry {
        self.dataset_registry.as_ref()
    }

    pub async fn create_root_dataset(&self, alias: &DatasetAlias) -> CreateDatasetResult {
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build();

        let result = self
            .dataset_repo_writer
            .create_dataset_from_snapshot(snapshot)
            .await
            .unwrap();

        result.create_dataset_result
    }

    pub async fn create_derived_dataset(
        &self,
        alias: &DatasetAlias,
        input_dataset_refs: Vec<DatasetRef>,
    ) -> CreateDatasetResult {
        self.dataset_repo_writer
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(alias.clone())
                    .kind(DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(input_dataset_refs)
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap()
            .create_dataset_result
    }

    pub async fn num_blocks(&self, create_result: &CreateDatasetResult) -> usize {
        use futures::StreamExt;
        create_result
            .dataset
            .as_metadata_chain()
            .iter_blocks()
            .count()
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
