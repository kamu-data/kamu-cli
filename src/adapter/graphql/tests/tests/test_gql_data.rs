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
use datafusion::arrow::record_batch::RecordBatch;
use dill::Component;
use event_bus::EventBus;
use kamu::testing::{MetadataFactory, ParquetWriterHelper};
use kamu::*;
use kamu_core::*;
use opendatafabric::*;

/////////////////////////////////////////////////////////////////////////////////////////

fn create_catalog_with_local_workspace(tempdir: &Path, is_multitenant: bool) -> dill::Catalog {
    let datasets_dir = tempdir.join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    dill::CatalogBuilder::new()
        .add::<SystemTimeSourceDefault>()
        .add::<EventBus>()
        .add::<DependencyGraphServiceInMemory>()
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(datasets_dir)
                .with_current_account_subject(Arc::new(CurrentAccountSubject::new_test()))
                .with_multi_tenant(is_multitenant),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .add::<QueryServiceImpl>()
        .add::<ObjectStoreRegistryImpl>()
        .add::<ObjectStoreBuilderLocalFs>()
        .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
        .build()
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn create_test_dataset(
    catalog: &dill::Catalog,
    tempdir: &Path,
    account_name: Option<AccountName>,
) {
    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

    let dataset = dataset_repo
        .create_dataset(
            &DatasetAlias::new(account_name, DatasetName::new_unchecked("foo")),
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
        )
        .await
        .unwrap()
        .dataset;

    let schema = Arc::new(Schema::new(vec![
        Field::new("offset", DataType::UInt64, false),
        Field::new("blah", DataType::Utf8, false),
    ]));

    dataset
        .commit_event(
            MetadataFactory::set_data_schema()
                .schema(&schema)
                .build()
                .into(),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let a: Arc<dyn Array> = Arc::new(UInt64Array::from(vec![0, 1, 2, 3]));
    let b: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&a), Arc::clone(&b)]).unwrap();

    // TODO: Use DataWriter
    let tmp_data_path = tempdir.join("data");
    ParquetWriterHelper::from_record_batch(&tmp_data_path, &record_batch).unwrap();

    dataset
        .commit_add_data(
            AddDataParams {
                prev_checkpoint: None,
                prev_offset: None,
                new_offset_interval: Some(OffsetInterval { start: 0, end: 3 }),
                new_watermark: None,
                new_source_state: None,
            },
            Some(OwnedFile::new(tmp_data_path)),
            None,
            CommitOpts::default(),
        )
        .await
        .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_schema_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path(), true);
    create_test_dataset(&catalog, tempdir.path(), None).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(indoc::indoc!(
                r#"
                {
                    datasets {
                        byOwnerAndName(accountName: "kamu", datasetName: "foo") {
                            name
                            data {
                                tail(limit: 1, schemaFormat: PARQUET_JSON, dataFormat: JSON) {
                                    ... on DataQueryResultSuccess {
                                        schema { content }
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .data(catalog),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");
    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let data_schema = &json["datasets"]["byOwnerAndName"]["data"]["tail"]["schema"]["content"];
    let data_schema =
        serde_json::from_str::<serde_json::Value>(data_schema.as_str().unwrap()).unwrap();
    assert_eq!(
        data_schema,
        serde_json::json!({
            "name": "arrow_schema",
            "type": "struct",
            "fields": [{
                "name": "offset",
                "repetition": "REQUIRED",
                "type": "INT64",
                "logicalType": "INTEGER(64,false)"
            }, {
                "name": "blah",
                "repetition": "REQUIRED",
                "type": "BYTE_ARRAY",
                "logicalType": "STRING"
            }]
        })
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path(), true);
    create_test_dataset(&catalog, tempdir.path(), None).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(indoc::indoc!(
                r#"
                {
                    datasets {
                        byOwnerAndName(accountName: "kamu", datasetName: "foo") {
                            name
                            data {
                                tail(skip: 1, limit: 1, schemaFormat: PARQUET_JSON, dataFormat: JSON) {
                                    ... on DataQueryResultSuccess {
                                        data { content }
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .data(catalog),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");
    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let data = &json["datasets"]["byOwnerAndName"]["data"]["tail"]["data"]["content"];
    let data = serde_json::from_str::<serde_json::Value>(data.as_str().unwrap()).unwrap();
    assert_eq!(data, serde_json::json!([{"blah": "c", "offset": 2}]));
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_empty_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path(), true);
    create_test_dataset(&catalog, tempdir.path(), None).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(indoc::indoc!(
                r#"
                {
                    datasets {
                        byOwnerAndName(accountName: "kamu", datasetName: "foo") {
                            name
                            data {
                                tail(skip: 10, schemaFormat: PARQUET_JSON, dataFormat: JSON) {
                                    ... on DataQueryResultSuccess {
                                        data { content }
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .data(catalog),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");
    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let data = &json["datasets"]["byOwnerAndName"]["data"]["tail"]["data"]["content"];
    let data = serde_json::from_str::<serde_json::Value>(data.as_str().unwrap()).unwrap();
    assert_eq!(data, serde_json::json!([]));
}

/////////////////////////////////////////////////////////////////////////////////////////
