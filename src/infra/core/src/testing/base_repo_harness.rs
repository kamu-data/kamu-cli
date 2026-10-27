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
use chrono::{DateTime, Utc};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{DidGenerator, DidGeneratorDefault, MockDidGenerator, RunInfoDir, TenancyConfig};
use kamu_datasets::{CreateDatasetResult, DatasetRegistry, DatasetRegistryExt, ResolvedDataset};
use odf::dataset::DatasetStorageUnitLocalFs;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::testing::{DatasetSnapshotBuilder, MetadataFactory};
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
        self.create_root_dataset_with_events(
            alias,
            vec![MetadataFactory::set_polling_source().build().into()],
        )
        .await
    }

    pub async fn create_root_dataset_with_push_source(
        &self,
        alias: &odf::DatasetAlias,
    ) -> CreateDatasetResult {
        self.create_root_dataset_with_events(
            alias,
            vec![MetadataFactory::add_push_source().build().into()],
        )
        .await
    }

    /// A root dataset polling the CSV file at `src_path`, with `city` and
    /// `population` columns and a header row
    pub async fn create_root_dataset_polling_csv(
        &self,
        alias: &odf::DatasetAlias,
        src_path: &Path,
    ) -> CreateDatasetResult {
        let source = MetadataFactory::set_polling_source()
            .fetch(odf::metadata::FetchStep::Url(odf::metadata::FetchStepUrl {
                url: url::Url::from_file_path(src_path).unwrap().to_string(),
                event_time: Some(odf::metadata::EventTimeSourceFromSystemTime {}.into()),
                cache: None,
                headers: None,
            }))
            .read(odf::metadata::ReadStep::Csv(odf::metadata::ReadStepCsv {
                header: Some(true),
                schema: Some(odf::schema::DataSchema::new(vec![
                    odf::schema::DataField::string("city"),
                    odf::schema::DataField::i64("population"),
                ])),
                ..odf::metadata::ReadStepCsv::default()
            }))
            .build();

        self.create_root_dataset_with_events(alias, vec![source.into()])
            .await
    }

    pub async fn create_root_dataset_without_source(
        &self,
        alias: &odf::DatasetAlias,
    ) -> CreateDatasetResult {
        self.create_root_dataset_with_events(alias, vec![]).await
    }

    async fn create_root_dataset_with_events(
        &self,
        alias: &odf::DatasetAlias,
        events: Vec<odf::MetadataEvent>,
    ) -> CreateDatasetResult {
        let snapshot = events
            .into_iter()
            .fold(
                MetadataFactory::dataset_snapshot()
                    .name(alias.clone())
                    .kind(odf::DatasetKind::Root),
                DatasetSnapshotBuilder::push_event,
            )
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

    /// Appends a `SetInfo` block after the current `HEAD` without moving any
    /// reference.
    pub async fn append_unreferenced_block(
        &self,
        created: &CreateDatasetResult,
        description: &str,
    ) -> odf::Multihash {
        let head = self.get_head(created).await;
        self.append_unreferenced_block_after(created, &head, description)
            .await
    }

    /// Appends a `SetInfo` block after `prev` without moving any reference
    pub async fn append_unreferenced_block_after(
        &self,
        created: &CreateDatasetResult,
        prev: &odf::Multihash,
        description: &str,
    ) -> odf::Multihash {
        let chain = created.dataset.as_metadata_chain();
        let prev_block = chain.get_block(prev).await.unwrap();

        let block = MetadataFactory::metadata_block(
            MetadataFactory::set_info().description(description).build(),
        )
        .prev(prev, prev_block.sequence_number)
        .system_time(self.system_time_source.now())
        .build();

        chain
            .append(
                block,
                odf::dataset::AppendOpts {
                    update_ref: None,
                    ..odf::dataset::AppendOpts::default()
                },
            )
            .await
            .unwrap()
    }

    /// Gives a root dataset a schema and a block that only advances its
    /// watermark to `watermark`, so a derivative of it has something to
    /// transform without data files
    pub async fn append_schema_and_watermark(
        &self,
        created: &CreateDatasetResult,
        watermark: DateTime<Utc>,
    ) {
        let chain = created.dataset.as_metadata_chain();
        for event in [
            odf::MetadataEvent::SetDataSchema(MetadataFactory::set_data_schema().build()),
            odf::MetadataEvent::AddData(
                MetadataFactory::add_data()
                    .new_watermark(Some(watermark))
                    .build(),
            ),
        ] {
            let head = self.get_head(created).await;
            let head_block = chain.get_block(&head).await.unwrap();
            chain
                .append(
                    MetadataFactory::metadata_block(event)
                        .prev(&head, head_block.sequence_number)
                        .system_time(self.system_time_source.now())
                        .build(),
                    odf::dataset::AppendOpts::default(),
                )
                .await
                .unwrap();
        }
    }

    /// Moves `HEAD` the way a concurrent writer would
    pub async fn move_head(
        &self,
        created: &CreateDatasetResult,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) {
        created
            .dataset
            .as_metadata_chain()
            .set_ref(
                &odf::BlockRef::Head,
                new_head,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Some(Some(old_head)),
                },
            )
            .await
            .unwrap();
    }

    pub async fn get_head(&self, created: &CreateDatasetResult) -> odf::Multihash {
        created
            .dataset
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .unwrap()
    }

    pub async fn num_blocks(&self, target: ResolvedDataset) -> usize {
        use futures::StreamExt;
        use odf::dataset::MetadataChainExt;
        target.as_metadata_chain().iter_blocks().count().await
    }

    pub fn serialize_block(
        block: &odf::MetadataBlock,
    ) -> (odf::Multihash, odf::MetadataBlockBytes) {
        let bytes = odf::storage::serialize_metadata_block(block).unwrap();

        let hash = odf::Multihash::from_digest::<sha3::Sha3_256>(
            odf::metadata::Multicodec::Sha3_256,
            &bytes,
        );

        (hash, bytes)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
