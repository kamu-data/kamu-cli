// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{TimeZone, Utc};
use dill::Component;
use kamu::domain::*;
use kamu::*;
use opendatafabric::{MergeStrategy, *};
use serde_json::json;

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
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add::<EngineProvisionerNull>()
            .build();

        let server_harness = ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            multi_tenant: true,
            authorized_writes: true,
            base_catalog: Some(catalog),
        });

        let system_time = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();
        server_harness.system_time_source().set(system_time);

        let create_result = server_harness
            .cli_dataset_repository()
            .create_dataset_from_snapshot(DatasetSnapshot {
                name: DatasetAlias::new(
                    server_harness.operating_account_name(),
                    DatasetName::new_unchecked("population"),
                ),
                kind: DatasetKind::Root,
                metadata: vec![AddPushSource {
                    source_name: "source1".to_string(),
                    read: ReadStepNdJson {
                        schema: Some(vec![
                            "event_time TIMESTAMP".to_owned(),
                            "city STRING".to_owned(),
                            "population BIGINT".to_owned(),
                        ]),
                        ..Default::default()
                    }
                    .into(),
                    preprocess: None,
                    merge: MergeStrategy::Ledger(MergeStrategyLedger {
                        primary_key: vec!["event_time".to_owned(), "city".to_owned()],
                    }),
                }
                .into()],
            })
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

// TODO: Replace with initialization using DF writer
async fn add_example_data(cl: &reqwest::Client, dataset_url: &url::Url) {
    let ingest_url = format!("{dataset_url}/ingest");

    cl.post(&ingest_url)
        .json(&json!(
            [
                {
                    "event_time": "2020-01-01T00:00:00",
                    "city": "A",
                    "population": 100,
                },
                {
                    "event_time": "2020-01-02T00:00:00",
                    "city": "B",
                    "population": 200,
                }
            ]
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_tail_handler() {
    let harness = Harness::new().await;

    let client = async move {
        let cl = reqwest::Client::new();
        add_example_data(&cl, &harness.dataset_url).await;

        // All points
        let tail_url = format!("{}/tail", harness.dataset_url);
        let res = cl
            .get(&tail_url)
            .query(&[("schema", "false")])
            .send()
            .await
            .unwrap();

        assert_eq!(res.status(), http::StatusCode::OK);
        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({"data": [{
                "city": "A",
                "event_time": "2020-01-01T00:00:00Z",
                "offset": 0,
                "op": 0,
                "population": 100,
                "system_time": "2050-01-01T12:00:00Z"
            }, {
                "city": "B",
                "event_time": "2020-01-02T00:00:00Z",
                "offset": 1,
                "op": 0,
                "population": 200,
                "system_time": "2050-01-01T12:00:00Z"
            }]})
        );

        // Limit
        let res = cl
            .get(&tail_url)
            .query(&[("limit", "1"), ("schema", "false")])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({"data": [{
                "city": "B",
                "event_time": "2020-01-02T00:00:00Z",
                "offset": 1,
                "op": 0,
                "population": 200,
                "system_time": "2050-01-01T12:00:00Z"
            }]})
        );

        // Skip
        let res = cl
            .get(&tail_url)
            .query(&[("skip", "1"), ("schema", "false")])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({"data": [{
                "city": "A",
                "event_time": "2020-01-01T00:00:00Z",
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
async fn test_data_query_handler() {
    let harness = Harness::new().await;

    let client = async move {
        let cl = reqwest::Client::new();
        add_example_data(&cl, &harness.dataset_url).await;

        let query = format!(
            "select offset, city, population from \"{}\" order by offset desc",
            harness.dataset_alias
        );
        let query_url = format!("{}query", harness.root_url);
        let res = cl
            .get(&query_url)
            .query(&[("query", query.as_str()), ("schema", "true")])
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
                "schema": {
                    "fields": [{
                        "data_type": "Int64",
                        "dict_id": 0,
                        "dict_is_ordered": false,
                        "metadata": {},
                        "name": "offset",
                        "nullable": true
                    }, {
                        "data_type": "Utf8",
                        "dict_id": 0,
                        "dict_is_ordered": false,
                        "metadata": {},
                        "name": "city",
                        "nullable": true
                    }, {
                        "data_type": "Int64",
                        "dict_id": 0,
                        "dict_is_ordered": false,
                        "metadata": {},
                        "name": "population",
                        "nullable": true
                    }],
                    "metadata": {}
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
        add_example_data(&cl, &harness.dataset_url).await;

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
                ("schema", "false"),
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
                ("schema", "false"),
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
async fn test_data_query_handler_formats() {
    let harness = Harness::new().await;

    let client = async move {
        let cl = reqwest::Client::new();
        add_example_data(&cl, &harness.dataset_url).await;

        let query = format!(
            "select offset, city, population from \"{}\" order by offset desc",
            harness.dataset_alias
        );
        let query_url = format!("{}query", harness.root_url);
        let res = cl
            .get(&query_url)
            .query(&[
                ("query", query.as_str()),
                ("format", "json-aos"),
                ("schema", "false"),
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
                ("format", "json-soa"),
                ("schema", "false"),
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
                ("format", "json-aoa"),
                ("schema", "false"),
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
