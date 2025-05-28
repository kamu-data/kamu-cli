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

use bon::bon;
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
use kamu_datasets::CreateDatasetResult;
use odf::dataset::DatasetStorageUnitLocalFs;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::serde::MetadataBlockSerializer;
use odf::metadata::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use odf::metadata::testing::MetadataFactory;
use time_source::{SystemTimeSource, SystemTimeSourceDefault, SystemTimeSourceStub};

use crate::DatasetRegistrySoloUnitBridge;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct BaseRepoHarness {
    temp_dir: tempfile::TempDir,
    catalog: dill::Catalog,
    did_generator: Arc<dyn DidGenerator>,
    system_time_source: Arc<dyn SystemTimeSource>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
}

#[bon]
impl BaseRepoHarness {
    #[builder]
    pub fn new(
        tenancy_config: TenancyConfig,
        mock_did_generator: Option<MockDidGenerator>,
        current_account_subject: Option<CurrentAccountSubject>,
        system_time_source_stub: Option<SystemTimeSourceStub>,
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
                .add_builder(DatasetStorageUnitLocalFs::builder(datasets_dir))
                .add::<odf::dataset::DatasetLfsBuilderDefault>()
                .add::<DatasetRegistrySoloUnitBridge>();

            if let Some(current_account_subject) = current_account_subject {
                b.add_value(current_account_subject);
            } else {
                b.add_value(CurrentAccountSubject::new_test());
            }

            if let Some(mock_did_generator) = mock_did_generator {
                b.add_value(mock_did_generator)
                    .bind::<dyn DidGenerator, MockDidGenerator>();
            } else {
                b.add::<DidGeneratorDefault>();
            }

            if let Some(system_time_source_stub) = system_time_source_stub {
                b.add_value(system_time_source_stub)
                    .bind::<dyn SystemTimeSource, SystemTimeSourceStub>();
            } else {
                b.add::<SystemTimeSourceDefault>();
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

    pub fn catalog(&self) -> &dill::Catalog {
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

    pub fn system_time_source(&self) -> &dyn SystemTimeSource {
        self.system_time_source.as_ref()
    }

    pub async fn check_dataset_exists(
        &self,
        alias: &odf::DatasetAlias,
    ) -> Result<(), odf::DatasetRefUnresolvedError> {
        self.dataset_registry
            .get_dataset_by_ref(&alias.as_local_ref())
            .await?;
        Ok(())
    }

    pub async fn create_root_dataset(&self, alias: &odf::DatasetAlias) -> CreateDatasetResult {
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build();

        let store_result = create_test_dataset_from_snapshot(
            self.dataset_registry.as_ref(),
            self.dataset_storage_unit_writer.as_ref(),
            snapshot,
            self.did_generator.generate_dataset_id().0,
            self.system_time_source.now(),
        )
        .await
        .unwrap();

        CreateDatasetResult::from_stored(store_result, alias.clone())
    }

    pub async fn create_root_dataset_with_push_source(
        &self,
        alias: &odf::DatasetAlias,
    ) -> CreateDatasetResult {
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(MetadataFactory::add_push_source().build())
            .build();

        let store_result = create_test_dataset_from_snapshot(
            self.dataset_registry.as_ref(),
            self.dataset_storage_unit_writer.as_ref(),
            snapshot,
            self.did_generator.generate_dataset_id().0,
            self.system_time_source.now(),
        )
        .await
        .unwrap();

        CreateDatasetResult::from_stored(store_result, alias.clone())
    }

    pub async fn create_derived_dataset(
        &self,
        alias: &odf::DatasetAlias,
        input_dataset_refs: Vec<odf::DatasetRef>,
    ) -> CreateDatasetResult {
        let store_result = create_test_dataset_from_snapshot(
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
            self.did_generator.generate_dataset_id().0,
            self.system_time_source.now(),
        )
        .await
        .unwrap();

        CreateDatasetResult::from_stored(store_result, alias.clone())
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
