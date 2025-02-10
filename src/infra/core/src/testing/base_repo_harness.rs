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
    DatasetRegistry,
    DatasetRegistryExt,
    DidGenerator,
    DidGeneratorDefault,
    MockDidGenerator,
    ResolvedDataset,
    RunInfoDir,
    TenancyConfig,
};
use odf::dataset::testing::create_test_dataset_fron_snapshot;
use odf::metadata::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use odf::metadata::serde::MetadataBlockSerializer;
use odf::metadata::testing::MetadataFactory;
use time_source::{SystemTimeSource, SystemTimeSourceDefault};

use crate::{DatasetRegistrySoloUnitBridge, DatasetStorageUnitLocalFs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct BaseRepoHarness {
    temp_dir: tempfile::TempDir,
    catalog: Catalog,
    did_generator: Arc<dyn DidGenerator>,
    system_time_source: Arc<dyn SystemTimeSource>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
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
                .add_builder(DatasetStorageUnitLocalFs::builder().with_root(datasets_dir))
                .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitLocalFs>()
                .bind::<dyn odf::DatasetStorageUnitWriter, DatasetStorageUnitLocalFs>()
                .add::<DatasetRegistrySoloUnitBridge>()
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
            did_generator: catalog.get_one().unwrap(),
            system_time_source: catalog.get_one().unwrap(),
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
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

    pub fn dataset_storage_unit_writer(&self) -> &dyn odf::DatasetStorageUnitWriter {
        self.dataset_storage_unit_writer.as_ref()
    }

    pub async fn check_dataset_exists(
        &self,
        alias: &odf::DatasetAlias,
    ) -> Result<(), odf::dataset::GetDatasetError> {
        self.dataset_registry
            .get_dataset_by_ref(&alias.as_local_ref())
            .await?;
        Ok(())
    }

    pub async fn create_root_dataset(&self, alias: &odf::DatasetAlias) -> odf::CreateDatasetResult {
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build();

        create_test_dataset_fron_snapshot(
            self.dataset_registry.as_ref(),
            self.dataset_storage_unit_writer.as_ref(),
            snapshot,
            self.did_generator.generate_dataset_id(),
            self.system_time_source.now(),
        )
        .await
        .unwrap()
    }

    pub async fn create_derived_dataset(
        &self,
        alias: &odf::DatasetAlias,
        input_dataset_refs: Vec<odf::DatasetRef>,
    ) -> odf::CreateDatasetResult {
        create_test_dataset_fron_snapshot(
            self.dataset_registry.as_ref(),
            self.dataset_storage_unit_writer.as_ref(),
            MetadataFactory::dataset_snapshot()
                .name(alias.clone())
                .kind(odf::DatasetKind::Derivative)
                .push_event(
                    MetadataFactory::set_transform()
                        .inputs_from_refs(input_dataset_refs)
                        .build(),
                )
                .build(),
            self.did_generator.generate_dataset_id(),
            self.system_time_source.now(),
        )
        .await
        .unwrap()
    }

    pub async fn num_blocks(&self, target: ResolvedDataset) -> usize {
        use futures::StreamExt;
        use odf::dataset::MetadataChainExt;
        target.as_metadata_chain().iter_blocks().count().await
    }

    pub fn hash_from_block(block: &odf::MetadataBlock) -> odf::Multihash {
        let block_data = FlatbuffersMetadataBlockSerializer
            .write_manifest(block)
            .unwrap();

        odf::Multihash::from_digest::<sha3::Sha3_256>(
            odf::metadata::Multicodec::Sha3_256,
            &block_data,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
