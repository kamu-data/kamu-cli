// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use dill::{Catalog, Component};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{
    CreateDatasetResult,
    DatasetRegistry,
    DatasetRegistryExt,
    DatasetRepository,
    DidGenerator,
    DidGeneratorDefault,
    GetDatasetError,
    MetadataChainExt,
    MockDidGenerator,
    ResolvedDataset,
    RunInfoDir,
    TenancyConfig,
};
use opendatafabric::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use opendatafabric::serde::MetadataBlockSerializer;
use opendatafabric::{DatasetAlias, DatasetKind, DatasetRef, MetadataBlock, Multicodec, Multihash};
use time_source::SystemTimeSourceDefault;

use crate::testing::MetadataFactory;
use crate::{DatasetRegistryRepoBridge, DatasetRepositoryLocalFs, DatasetRepositoryWriter};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct BaseRepoHarness {
    temp_dir: tempfile::TempDir,
    catalog: Catalog,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
}

impl BaseRepoHarness {
    pub fn new(
        tenancy_config: TenancyConfig,
        maybe_mock_did_generator: Option<MockDidGenerator>,
    ) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();

        let datasets_dir = temp_dir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let run_info_dir = temp_dir.path().join("run");
        std::fs::create_dir(&run_info_dir).unwrap();

        let catalog = {
            let mut b = dill::CatalogBuilder::new();

            b.add_value(RunInfoDir::new(run_info_dir))
                .add_value(tenancy_config)
                .add_builder(DatasetRepositoryLocalFs::builder().with_root(datasets_dir))
                .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
                .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
                .add::<DatasetRegistryRepoBridge>()
                .add_value(CurrentAccountSubject::new_test())
                .add::<SystemTimeSourceDefault>();

            if let Some(mock_did_generator) = maybe_mock_did_generator {
                b.add_value(mock_did_generator)
                    .bind::<dyn DidGenerator, MockDidGenerator>();
            } else {
                b.add::<DidGeneratorDefault>();
            }

            b.build()
        };

        Self {
            temp_dir,
            dataset_registry: catalog.get_one().unwrap(),
            dataset_repo_writer: catalog.get_one().unwrap(),
            catalog,
        }
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn temp_dir_path(&self) -> &Path {
        self.temp_dir.path()
    }

    pub fn dataset_registry(&self) -> &dyn DatasetRegistry {
        self.dataset_registry.as_ref()
    }

    pub fn dataset_repo_writer(&self) -> &dyn DatasetRepositoryWriter {
        self.dataset_repo_writer.as_ref()
    }

    pub async fn check_dataset_exists(&self, alias: &DatasetAlias) -> Result<(), GetDatasetError> {
        self.dataset_registry
            .get_dataset_by_ref(&alias.as_local_ref())
            .await?;
        Ok(())
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

    pub async fn num_blocks(&self, target: ResolvedDataset) -> usize {
        use futures::StreamExt;
        target.as_metadata_chain().iter_blocks().count().await
    }

    pub fn hash_from_block(block: &MetadataBlock) -> Multihash {
        let block_data = FlatbuffersMetadataBlockSerializer
            .write_manifest(block)
            .unwrap();

        Multihash::from_digest::<sha3::Sha3_256>(Multicodec::Sha3_256, &block_data)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
