// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use kamu::domain::*;
use kamu::infra;
use kamu::testing::MetadataFactory;
use kamu::testing::ParquetWriterHelper;
use opendatafabric::*;

use std::path::Path;
use std::sync::Arc;

async fn create_test_dataset(tempdir: &Path) -> dill::Catalog {
    let workspace_layout = Arc::new(infra::WorkspaceLayout::create(tempdir).unwrap());
    let local_repo = infra::DatasetRepositoryLocalFs::new(workspace_layout.clone());

    let cat = dill::CatalogBuilder::new()
        .add_value(local_repo)
        .add_value(workspace_layout.as_ref().clone())
        .bind::<dyn DatasetRepository, infra::DatasetRepositoryLocalFs>()
        .add::<infra::QueryServiceImpl>()
        .bind::<dyn QueryService, infra::QueryServiceImpl>()
        .build();

    let local_repo = cat.get_one::<dyn DatasetRepository>().unwrap();

    let mut dataset_builder = local_repo
        .create_dataset(&DatasetAlias::new(None, DatasetName::new_unchecked("foo")))
        .await
        .unwrap();

    let ds = dataset_builder.as_dataset();
    ds.commit_event(
        MetadataEvent::Seed(MetadataFactory::seed(DatasetKind::Root).build()),
        CommitOpts::default(),
    )
    .await
    .unwrap();

    let tmp_data_path = tempdir.join("data");
    let schema = Arc::new(Schema::new(vec![
        Field::new("offset", DataType::UInt64, false),
        Field::new("blah", DataType::Utf8, false),
    ]));
    let a: Arc<dyn Array> = Arc::new(UInt64Array::from(vec![0, 1, 2]));
    let b: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&a), Arc::clone(&b)]).unwrap();
    ParquetWriterHelper::from_record_batch(&tmp_data_path, &record_batch).unwrap();

    ds.commit_add_data(
        None,
        Some(OffsetInterval { start: 0, end: 3 }),
        Some(tmp_data_path),
        None,
        None,
        None,
        CommitOpts::default(),
    )
    .await
    .unwrap();

    dataset_builder.finish().await.unwrap();

    cat
}

#[test_log::test(tokio::test)]
#[cfg_attr(not(unix), ignore)] // TODO: DataFusion crashes on windows
async fn test_dataset_schema() {
    let tempdir = tempfile::tempdir().unwrap();
    let cat = create_test_dataset(tempdir.path()).await;

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(indoc::indoc!(
            "{
                datasets {
                    byOwnerAndName(accountName: \"kamu\", datasetName: \"foo\") {
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
            }"
        ))
        .await;
    assert!(res.is_ok(), "{:?}", res);
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

#[test_log::test(tokio::test)]
#[cfg_attr(not(unix), ignore)] // TODO: DataFusion crashes on windows
async fn test_dataset_tail() {
    let tempdir = tempfile::tempdir().unwrap();
    let cat = create_test_dataset(tempdir.path()).await;

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(indoc::indoc!(
            "{
                datasets {
                    byOwnerAndName(accountName: \"kamu\", datasetName: \"foo\") {
                        name 
                        data {
                            tail(limit: 1, schemaFormat: PARQUET_JSON, dataFormat: JSON) {
                                ... on DataQueryResultSuccess {
                                    data { content }
                                }
                            }
                        }
                    }
                }
            }"
        ))
        .await;
    assert!(res.is_ok(), "{:?}", res);
    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let data = &json["datasets"]["byOwnerAndName"]["data"]["tail"]["data"]["content"];
    let data = serde_json::from_str::<serde_json::Value>(data.as_str().unwrap()).unwrap();
    assert_eq!(data, serde_json::json!([{"blah":"c","offset":2}]));
}

#[test_log::test(tokio::test)]
#[cfg_attr(not(unix), ignore)] // TODO: DataFusion crashes on windows
async fn test_dataset_tail_empty() {
    let tempdir = tempfile::tempdir().unwrap();
    let cat = create_test_dataset(tempdir.path()).await;

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(indoc::indoc!(
            "{
                datasets {
                    byOwnerAndName(accountName: \"kamu\", datasetName: \"foo\") {
                        name 
                        data {
                            tail(limit: 0, schemaFormat: PARQUET_JSON, dataFormat: JSON) {
                                ... on DataQueryResultSuccess {
                                    data { content }
                                }
                            }
                        }
                    }
                }
            }"
        ))
        .await;
    assert!(res.is_ok(), "{:?}", res);
    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let data = &json["datasets"]["byOwnerAndName"]["data"]["tail"]["data"]["content"];
    let data = serde_json::from_str::<serde_json::Value>(data.as_str().unwrap()).unwrap();
    assert_eq!(data, serde_json::json!([]));
}
