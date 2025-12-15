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

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use file_utils::OwnedFile;
use kamu::testing::{MockDatasetActionAuthorizer, ParquetWriterHelper};
use kamu::{
    DatasetRegistrySoloUnitBridge,
    EngineConfigDatafusionEmbeddedBatchQuery,
    ObjectStoreBuilderLocalFs,
    ObjectStoreBuilderS3,
    ObjectStoreRegistryImpl,
    SessionContextBuilder,
};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{DidGenerator, DidGeneratorDefault, TenancyConfig, auth};
use kamu_datasets::{DatasetRegistry, ResolvedDataset};
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::testing::MetadataFactory;
use s3_utils::S3Context;
use test_utils::LocalS3Server;
use time_source::{SystemTimeSource, SystemTimeSourceStub};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn create_empty_dataset(
    catalog: &dill::Catalog,
    name: &str,
) -> (odf::dataset::StoreDatasetResult, odf::DatasetAlias) {
    let dataset_storage_unit_writer = catalog
        .get_one::<dyn odf::DatasetStorageUnitWriter>()
        .unwrap();

    let dataset_registry = catalog.get_one::<dyn DatasetRegistry>().unwrap();
    let did_generator = catalog.get_one::<dyn DidGenerator>().unwrap();
    let time_source = catalog.get_one::<dyn SystemTimeSource>().unwrap();

    let dataset_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(name));

    let stored = create_test_dataset_from_snapshot(
        dataset_registry.as_ref(),
        dataset_storage_unit_writer.as_ref(),
        MetadataFactory::dataset_snapshot()
            .name(dataset_alias.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build(),
        did_generator.generate_dataset_id().0,
        time_source.now(),
    )
    .await
    .unwrap();

    (stored, dataset_alias)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn create_test_dataset(
    catalog: &dill::Catalog,
    tempdir: &Path,
    name: &str,
) -> ResolvedDataset {
    // Empty dataset first
    let (stored, dataset_alias) = create_empty_dataset(catalog, name).await;

    // Write schema
    let tmp_data_path = tempdir.join("data");
    let schema = Arc::new(Schema::new(vec![
        Field::new("offset", DataType::UInt64, false),
        Field::new("blah", DataType::Utf8, false),
    ]));

    stored
        .dataset
        .commit_event(
            MetadataFactory::set_data_schema()
                .schema_from_arrow(&schema)
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    // Write data spread over two commits
    let batches = [
        (
            UInt64Array::from(vec![0, 1]),
            StringArray::from(vec!["a", "b"]),
        ),
        (
            UInt64Array::from(vec![2, 3]),
            StringArray::from(vec!["c", "d"]),
        ),
    ];

    // TODO: Replace with DataWriter
    let mut prev_offset = None;
    for (a, b) in batches {
        let record_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(a), Arc::new(b)]).unwrap();
        ParquetWriterHelper::from_record_batch(&tmp_data_path, &record_batch).unwrap();

        let start_offset = prev_offset.map_or(0, |v| v + 1);
        let end_offset = start_offset + record_batch.num_rows() as u64 - 1;

        stored
            .dataset
            .commit_add_data(
                odf::dataset::AddDataParams {
                    prev_checkpoint: None,
                    prev_offset,
                    new_offset_interval: Some(odf::metadata::OffsetInterval {
                        start: start_offset,
                        end: end_offset,
                    }),
                    new_linked_objects: None,
                    new_watermark: None,
                    new_source_state: None,
                },
                Some(OwnedFile::new(tmp_data_path.clone())),
                None,
                odf::dataset::CommitOpts::default(),
            )
            .await
            .unwrap();

        prev_offset = Some(end_offset);
    }

    ResolvedDataset::from_stored(&stored, &dataset_alias)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_base_catalog(dataset_action_authorizer: MockDatasetActionAuthorizer) -> dill::Catalog {
    dill::CatalogBuilder::new()
        .add::<DidGeneratorDefault>()
        .add::<SystemTimeSourceStub>()
        .add_value(TenancyConfig::SingleTenant)
        .add::<DatasetRegistrySoloUnitBridge>()
        .add_value(EngineConfigDatafusionEmbeddedBatchQuery::default())
        .add::<SessionContextBuilder>()
        .add::<ObjectStoreRegistryImpl>()
        .add_value(CurrentAccountSubject::new_test())
        .add_value(dataset_action_authorizer)
        .bind::<dyn auth::DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
        .build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn create_base_catalog_with_local_workspace(
    tempdir: &std::path::Path,
    dataset_action_authorizer: MockDatasetActionAuthorizer,
) -> dill::Catalog {
    let base_catalog = create_base_catalog(dataset_action_authorizer);

    let datasets_dir = tempdir.join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    dill::CatalogBuilder::new_chained(&base_catalog)
        .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
            datasets_dir,
        ))
        .add::<odf::dataset::DatasetLfsBuilderDefault>()
        .add::<ObjectStoreBuilderLocalFs>()
        .build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn create_base_catalog_with_s3_workspace(
    s3: &LocalS3Server,
    dataset_action_authorizer: MockDatasetActionAuthorizer,
) -> dill::Catalog {
    let base_catalog = create_base_catalog(dataset_action_authorizer);

    let s3_context = S3Context::from_url(&s3.url).await;

    dill::CatalogBuilder::new_chained(&base_catalog)
        .add_builder(odf::dataset::DatasetStorageUnitS3::builder(
            s3_context.clone(),
        ))
        .add_builder(odf::dataset::DatasetS3BuilderDefault::builder(None))
        .add_builder(ObjectStoreBuilderS3::builder(s3_context, true))
        .build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
