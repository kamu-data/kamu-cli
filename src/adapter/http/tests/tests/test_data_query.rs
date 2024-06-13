// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion::arrow::array::{RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::*;
use datafusion::prelude::*;
use dill::Component;
use kamu::domain::*;
use kamu::*;
use kamu_ingest_datafusion::DataWriterDataFusion;
use opendatafabric::*;
use serde_json::json;
use testing::MetadataFactory;

use crate::harness::*;

/////////////////////////////////////////////////////////////////////////////////////////

struct Harness {
    #[allow(dead_code)]
    run_info_dir: tempfile::TempDir,
    server_harness: ServerSideLocalFsHarness,
    root_url: url::Url,
    dataset_alias: DatasetAlias,
    dataset_url: url::Url,
}

impl Harness {
    async fn new() -> Self {
        // TODO: Need access to these from harness level
        let run_info_dir = tempfile::tempdir().unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<DataFormatRegistryImpl>()
            .add::<QueryServiceImpl>()
            .add_builder(
                PushIngestServiceImpl::builder()
                    .with_run_info_dir(run_info_dir.path().to_path_buf()),
            )
            .bind::<dyn PushIngestService, PushIngestServiceImpl>()
            .add::<EngineProvisionerNull>()
            .build();

        let server_harness = ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            multi_tenant: true,
            authorized_writes: true,
            base_catalog: Some(catalog),
        });

        let system_time = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();
        server_harness.system_time_source().set(system_time);

        let alias = DatasetAlias::new(
            server_harness.operating_account_name(),
            DatasetName::new_unchecked("population"),
        );
        let create_result = server_harness
            .cli_dataset_repository()
            .create_dataset(
                &alias,
                MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                    .system_time(system_time)
                    .build_typed(),
            )
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let mut writer = DataWriterDataFusion::builder(create_result.dataset.clone(), ctx.clone())
            .with_metadata_state_scanned(None)
            .await
            .unwrap()
            .build();

        writer
            .write(
                Some(
                    ctx.read_batch(
                        RecordBatch::try_new(
                            Arc::new(Schema::new(vec![
                                Field::new("city", DataType::Utf8, false),
                                Field::new("population", DataType::UInt64, false),
                            ])),
                            vec![
                                Arc::new(StringArray::from(vec!["A", "B"])),
                                Arc::new(UInt64Array::from(vec![100, 200])),
                            ],
                        )
                        .unwrap(),
                    )
                    .unwrap(),
                ),
                WriteDataOpts {
                    system_time,
                    source_event_time: system_time,
                    new_watermark: None,
                    new_source_state: None,
                    data_staging_path: run_info_dir.path().join(".temp-data"),
                },
            )
            .await
            .unwrap();

        let root_url = url::Url::parse(
            format!("http://{}", server_harness.api_server_addr()).trim_end_matches('/'),
        )
        .unwrap();

        let dataset_url =
            server_harness.dataset_url_with_scheme(&create_result.dataset_handle.alias, "http");

        Self {
            run_info_dir,
            server_harness,
            root_url,
            dataset_alias: create_result.dataset_handle.alias,
            dataset_url,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_tail_handler() {
    let harness = Harness::new().await;

    let client = async move {
        let cl = reqwest::Client::new();

        // All points
        let tail_url = format!("{}/tail", harness.dataset_url);
        let res = cl
            .get(&tail_url)
            .query(&[("includeSchema", "false")])
            .send()
            .await
            .unwrap();

        assert_eq!(res.status(), http::StatusCode::OK);
        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({"data": [{
                "city": "A",
                "event_time": "2050-01-01T12:00:00Z",
                "offset": 0,
                "op": 0,
                "population": 100,
                "system_time": "2050-01-01T12:00:00Z"
            }, {
                "city": "B",
                "event_time": "2050-01-01T12:00:00Z",
                "offset": 1,
                "op": 0,
                "population": 200,
                "system_time": "2050-01-01T12:00:00Z"
            }]})
        );

        // Limit
        let res = cl
            .get(&tail_url)
            .query(&[("limit", "1"), ("includeSchema", "false")])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({"data": [{
                "city": "B",
                "event_time": "2050-01-01T12:00:00Z",
                "offset": 1,
                "op": 0,
                "population": 200,
                "system_time": "2050-01-01T12:00:00Z"
            }]})
        );

        // Skip
        let res = cl
            .get(&tail_url)
            .query(&[("skip", "1"), ("includeSchema", "false")])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({"data": [{
                "city": "A",
                "event_time": "2050-01-01T12:00:00Z",
                "offset": 0,
                "op": 0,
                "population": 100,
                "system_time": "2050-01-01T12:00:00Z"
            }]})
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_query_handler_full() {
    let harness = Harness::new().await;

    let client = async move {
        let cl = reqwest::Client::new();

        let query = format!(
            "select offset, city, population from \"{}\" order by offset desc",
            harness.dataset_alias
        );
        let query_url = format!("{}query", harness.root_url);
        let res = cl
            .get(&query_url)
            .query(&[("query", query.as_str())])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({
                "data": [{
                    "city": "B",
                    "offset": 1,
                    "population": 200,
                }, {
                    "city": "A",
                    "offset": 0,
                    "population": 100,
                }],
                "schema": "{\"fields\":[{\"name\":\"offset\",\"data_type\":\"Int64\",\"nullable\":true,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}},{\"name\":\"city\",\"data_type\":\"Utf8\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}},{\"name\":\"population\",\"data_type\":\"UInt64\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}}],\"metadata\":{}}",
                "resultHash": "f9680c001200b3483eecc3d5c6b50ee6b8cba11b51c08f89ea1f53d3a334c743199f5fe656e",
                "state": {
                    "inputs": [{
                        "id": "did:odf:fed01df230b49615d175307d580c33d6fda61fc7b9aec91df0f5c1a5ebe3b8cbfee02",
                        "blockHash": "f16204cec6245fadfbf0663b0e9e9a01c73268cc13e29087b33ce3454af08eb4d3e0b",
                    }]
                }
            })
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_query_handler_ranges() {
    let harness = Harness::new().await;

    let client = async move {
        let cl = reqwest::Client::new();

        let query = format!(
            "select offset, city, population from \"{}\" order by offset desc",
            harness.dataset_alias
        );
        let query_url = format!("{}query", harness.root_url);

        // Limit
        let res = cl
            .get(&query_url)
            .query(&[
                ("query", query.as_str()),
                ("limit", "1"),
                ("includeSchema", "false"),
                ("includeState", "false"),
                ("includeResultHash", "false"),
            ])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({"data": [{
                "city": "B",
                "offset": 1,
                "population": 200,
            }]})
        );

        // Skip
        let res = cl
            .get(&query_url)
            .query(&[
                ("query", query.as_str()),
                ("skip", "1"),
                ("includeSchema", "false"),
                ("includeState", "false"),
                ("includeResultHash", "false"),
            ])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({"data": [{
                "city": "A",
                "offset": 0,
                "population": 100,
            }]})
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_query_handler_data_formats() {
    let harness = Harness::new().await;

    let client = async move {
        let cl = reqwest::Client::new();

        let query = format!(
            "select offset, city, population from \"{}\" order by offset desc",
            harness.dataset_alias
        );
        let query_url = format!("{}query", harness.root_url);
        let res = cl
            .get(&query_url)
            .query(&[
                ("query", query.as_str()),
                ("dataFormat", "json-aos"),
                ("includeSchema", "false"),
                ("includeState", "false"),
                ("includeResultHash", "false"),
            ])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({"data": [{
                "city": "B",
                "offset": 1,
                "population": 200,
            }, {
                "city": "A",
                "offset": 0,
                "population": 100,
            }]})
        );

        let res = cl
            .get(&query_url)
            .query(&[
                ("query", query.as_str()),
                ("dataFormat", "json-soa"),
                ("includeSchema", "false"),
                ("includeState", "false"),
                ("includeResultHash", "false"),
            ])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({"data": {
                "offset": [1, 0],
                "city": ["B", "A"],
                "population": [200, 100],
            }})
        );

        let res = cl
            .get(&query_url)
            .query(&[
                ("query", query.as_str()),
                ("dataFormat", "json-aoa"),
                ("includeSchema", "false"),
                ("includeState", "false"),
                ("includeResultHash", "false"),
            ])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({"data": [
                [1, "B", 200],
                [0, "A", 100],
            ]})
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_query_handler_schema_formats() {
    let harness = Harness::new().await;

    let client = async move {
        let cl = reqwest::Client::new();

        let query = format!(
            "select offset, city, population from \"{}\"",
            harness.dataset_alias
        );
        let query_url = format!("{}query", harness.root_url);
        let res = cl
            .get(&query_url)
            .query(&[
                ("query", query.as_str()),
                ("schemaFormat", "arrow-json"),
                ("includeSchema", "true"),
                ("includeState", "false"),
                ("includeResultHash", "false"),
            ])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap()["schema"]
                .as_str()
                .unwrap(),
            r#"{"fields":[{"name":"offset","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"city","data_type":"Utf8","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"population","data_type":"UInt64","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}}],"metadata":{}}"#
        );

        let res = cl
            .get(&query_url)
            .query(&[
                ("query", query.as_str()),
                ("schemaFormat", "ArrowJson"),
                ("includeSchema", "true"),
                ("includeState", "false"),
                ("includeResultHash", "false"),
            ])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap()["schema"]
                .as_str()
                .unwrap(),
            r#"{"fields":[{"name":"offset","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"city","data_type":"Utf8","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}},{"name":"population","data_type":"UInt64","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}}],"metadata":{}}"#
        );

        let res = cl
            .get(&query_url)
            .query(&[
                ("query", query.as_str()),
                ("schemaFormat", "parquet"),
                ("includeSchema", "true"),
                ("includeState", "false"),
                ("includeResultHash", "false"),
            ])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap()["schema"]
                .as_str()
                .unwrap(),
            indoc::indoc!(
                r#"message arrow_schema {
                  OPTIONAL INT64 offset;
                  REQUIRED BYTE_ARRAY city (STRING);
                  REQUIRED INT64 population (INTEGER(64,false));
                }
                "#
            )
        );

        let res = cl
            .get(&query_url)
            .query(&[
                ("query", query.as_str()),
                ("schemaFormat", "parquet-json"),
                ("includeSchema", "true"),
                ("includeState", "false"),
                ("includeResultHash", "false"),
            ])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap()["schema"]
                .as_str()
                .unwrap(),
            r#"{"name": "arrow_schema", "type": "struct", "fields": [{"name": "offset", "repetition": "OPTIONAL", "type": "INT64"}, {"name": "city", "repetition": "REQUIRED", "type": "BYTE_ARRAY", "logicalType": "STRING"}, {"name": "population", "repetition": "REQUIRED", "type": "INT64", "logicalType": "INTEGER(64,false)"}]}"#
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////
