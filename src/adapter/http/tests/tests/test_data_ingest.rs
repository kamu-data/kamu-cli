// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, TimeZone, Utc};
use dill::Component;
use indoc::indoc;
use kamu::domain::*;
use kamu::testing::DatasetDataHelper;
use kamu::*;
use kamu_accounts::DUMMY_ACCESS_TOKEN;
use kamu_adapter_http::{
    make_upload_token,
    FileUploadLimitConfig,
    UploadService,
    UploadServiceLocal,
};
use opendatafabric::{MergeStrategy, *};
use serde_json::json;
use url::Url;
use uuid::Uuid;

use crate::harness::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_push_ingest_handler() {
    let harness = DataIngestHarness::new();

    let create_result = harness.create_population_dataset(true).await;

    let dataset_url = harness.dataset_http_url(&create_result.dataset_handle.alias);

    let client = async move {
        let cl = reqwest::Client::new();
        let dataset_helper = DatasetDataHelper::new(create_result.dataset.clone());
        let ingest_url = format!("{dataset_url}/ingest");
        tracing::info!(%ingest_url, "Client request");

        // OK - uses the only push source
        let res = cl
            .execute(
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
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::OK);

        dataset_helper
            .assert_last_data_eq(
                indoc!(
                    r#"
                    message arrow_schema {
                      OPTIONAL INT64 offset;
                      REQUIRED INT32 op;
                      REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL BYTE_ARRAY city (STRING);
                      OPTIONAL INT64 population;
                    }
                   "#
                ),
                indoc!(
                    r#"
                    +--------+----+----------------------+----------------------+------+------------+
                    | offset | op | system_time          | event_time           | city | population |
                    +--------+----+----------------------+----------------------+------+------------+
                    | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | A    | 100        |
                    | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-02T00:00:00Z | B    | 200        |
                    +--------+----+----------------------+----------------------+------+------------+
                    "#
                ),
            )
            .await;

        // Add another source
        create_result
            .dataset
            .commit_event(
                AddPushSource {
                    source_name: "source2".to_string(),
                    read: ReadStepNdJson {
                        schema: Some(vec![
                            "event_time TIMESTAMP".to_owned(),
                            "city STRING".to_owned(),
                            "population BIGINT".to_owned(),
                        ]),
                        ..Default::default()
                    }
                    .into(),
                    preprocess: Some(
                        TransformSql {
                            engine: "datafusion".to_string(),
                            version: None,
                            temporal_tables: None,
                            query: None,
                            queries: Some(vec![SqlQueryStep {
                                query: "select event_time, city, population + 1 as population \
                                        from input"
                                    .to_string(),
                                alias: None,
                            }]),
                        }
                        .into(),
                    ),
                    merge: MergeStrategy::Ledger(MergeStrategyLedger {
                        primary_key: vec!["event_time".to_owned(), "city".to_owned()],
                    }),
                }
                .into(),
                CommitOpts {
                    system_time: Some(harness.system_time),
                    ..CommitOpts::default()
                },
            )
            .await
            .unwrap();

        // ERR - now need to disambiguate
        let res = cl
            .execute(
                cl.post(&ingest_url)
                    .json(&json!(
                        [
                            {
                                "event_time": "2020-01-03T00:00:00",
                                "city": "D",
                                "population": 300,
                            }
                        ]
                    ))
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::BAD_REQUEST);

        // OK - uses named source
        let res = cl
            .execute(
                cl.post(&ingest_url)
                    .query(&[("sourceName", "source2")])
                    .json(&json!(
                        [
                            {
                                "event_time": "2020-01-03T00:00:00",
                                "city": "C",
                                "population": 300,
                            }
                        ]
                    ))
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::OK);

        dataset_helper
            .assert_last_data_eq(
                indoc!(
                    r#"
                    message arrow_schema {
                      OPTIONAL INT64 offset;
                      REQUIRED INT32 op;
                      REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL BYTE_ARRAY city (STRING);
                      OPTIONAL INT64 population;
                    }
                   "#
                ),
                indoc!(
                    r#"
                    +--------+----+----------------------+----------------------+------+------------+
                    | offset | op | system_time          | event_time           | city | population |
                    +--------+----+----------------------+----------------------+------+------------+
                    | 2      | 0  | 2050-01-01T12:00:00Z | 2020-01-03T00:00:00Z | C    | 301        |
                    +--------+----+----------------------+----------------------+------+------------+
                    "#
                ),
            )
            .await;

        // ERR - invalid source
        let res = cl
            .execute(
                cl.post(&ingest_url)
                    .query(&[("sourceName", "source3")])
                    .json(&json!(
                        [
                            {
                                "event_time": "2020-01-04T00:00:00",
                                "city": "D",
                                "population": 400,
                            }
                        ]
                    ))
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::BAD_REQUEST);

        // OK - transcoding from CSV
        let res = cl
            .execute(
                cl.post(&ingest_url)
                    .query(&[("sourceName", "source1")])
                    .header("Content-Type", "text/csv")
                    .body("2020-01-04T00:00:00,D,400")
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::OK);

        dataset_helper
            .assert_last_data_eq(
                indoc!(
                    r#"
                    message arrow_schema {
                      OPTIONAL INT64 offset;
                      REQUIRED INT32 op;
                      REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL BYTE_ARRAY city (STRING);
                      OPTIONAL INT64 population;
                    }
                   "#
                ),
                indoc!(
                    r#"
                    +--------+----+----------------------+----------------------+------+------------+
                    | offset | op | system_time          | event_time           | city | population |
                    +--------+----+----------------------+----------------------+------+------------+
                    | 3      | 0  | 2050-01-01T12:00:00Z | 2020-01-04T00:00:00Z | D    | 400        |
                    +--------+----+----------------------+----------------------+------+------------+
                    "#
                ),
            )
            .await;
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_push_ingest_upload_token_no_initial_source() {
    let harness = DataIngestHarness::new();

    let create_result = harness.create_population_dataset(false).await;

    const FILE_CONTENT: &str = r#"
        [
            {
                "city": "A",
                "population": 100
            },
            {
                "city": "B",
                "population": 200
            }
        ]
        "#;

    let upload_id = Uuid::new_v4().simple().to_string();

    harness
        .upload_file(&upload_id, "population.json", FILE_CONTENT)
        .await;

    let upload_token = make_upload_token(
        upload_id,
        String::from("population.json"),
        String::from("application/json"),
        FILE_CONTENT.len(),
    );

    let dataset_url = harness.dataset_http_url(&create_result.dataset_handle.alias);

    let client = async move {
        let cl = reqwest::Client::new();
        let dataset_helper = DatasetDataHelper::new(create_result.dataset.clone());
        let ingest_url = format!("{dataset_url}/ingest?uploadToken={upload_token}");
        tracing::info!(%ingest_url, "Client request");

        let res = cl
            .execute(
                cl.post(&ingest_url)
                    .bearer_auth(DUMMY_ACCESS_TOKEN)
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::OK);

        dataset_helper
            .assert_last_data_eq(
                indoc!(
                    r#"
                    message arrow_schema {
                      OPTIONAL INT64 offset;
                      REQUIRED INT32 op;
                      REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL BYTE_ARRAY city (STRING);
                      OPTIONAL INT64 population;
                    }
                   "#
                ),
                indoc!(
                    r#"
                    +--------+----+----------------------+----------------------+------+------------+
                    | offset | op | system_time          | event_time           | city | population |
                    +--------+----+----------------------+----------------------+------+------------+
                    | 0      | 0  | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | A    | 100        |
                    | 1      | 0  | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | B    | 200        |
                    +--------+----+----------------------+----------------------+------+------------+
                    "#
                ),
            )
            .await;
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_push_ingest_upload_token_with_initial_source() {
    let harness = DataIngestHarness::new();

    let create_result = harness.create_population_dataset(true).await;

    const FILE_CONTENT: &str = r#"
        [
            {
                "event_time": "2020-01-01T00:00:00",
                "city": "A",
                "population": 100
            },
            {
                "event_time": "2020-01-02T00:00:00",
                "city": "B",
                "population": 200
            }
        ]
        "#;

    let upload_id = Uuid::new_v4().simple().to_string();

    harness
        .upload_file(&upload_id, "population.json", FILE_CONTENT)
        .await;

    let upload_token = make_upload_token(
        upload_id,
        String::from("population.json"),
        String::from("application/json"),
        FILE_CONTENT.len(),
    );

    let dataset_url = harness.dataset_http_url(&create_result.dataset_handle.alias);

    let client = async move {
        let cl = reqwest::Client::new();
        let dataset_helper = DatasetDataHelper::new(create_result.dataset.clone());
        let ingest_url = format!("{dataset_url}/ingest?uploadToken={upload_token}");
        tracing::info!(%ingest_url, "Client request");

        let res = cl
            .execute(
                cl.post(&ingest_url)
                    .bearer_auth(DUMMY_ACCESS_TOKEN)
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::OK);

        dataset_helper
            .assert_last_data_eq(
                indoc!(
                    r#"
                    message arrow_schema {
                      OPTIONAL INT64 offset;
                      REQUIRED INT32 op;
                      REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL BYTE_ARRAY city (STRING);
                      OPTIONAL INT64 population;
                    }
                   "#
                ),
                indoc!(
                    r#"
                    +--------+----+----------------------+----------------------+------+------------+
                    | offset | op | system_time          | event_time           | city | population |
                    +--------+----+----------------------+----------------------+------+------------+
                    | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | A    | 100        |
                    | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-02T00:00:00Z | B    | 200        |
                    +--------+----+----------------------+----------------------+------+------------+
                    "#
                ),
            )
            .await;
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_push_ingest_upload_token_actual_file_different_size() {
    let harness = DataIngestHarness::new();

    let create_result = harness.create_population_dataset(false).await;

    const FILE_CONTENT: &str = r#"
        [
            {
                "event_time": "2020-01-01T00:00:00",
                "city": "A",
                "population": 100
            },
            {
                "event_time": "2020-01-02T00:00:00",
                "city": "B",
                "population": 200
            }
        ]
        "#;

    let upload_id = Uuid::new_v4().simple().to_string();

    harness
        .upload_file(&upload_id, "population.json", FILE_CONTENT)
        .await;

    let upload_token = make_upload_token(
        upload_id,
        String::from("population.json"),
        String::from("application/json"),
        FILE_CONTENT.len() - 17, // Intentionally wrong file size
    );

    let dataset_url = harness.dataset_http_url(&create_result.dataset_handle.alias);

    let client = async move {
        let cl = reqwest::Client::new();
        let ingest_url = format!("{dataset_url}/ingest?uploadToken={upload_token}");
        tracing::info!(%ingest_url, "Client request");

        let res = cl
            .execute(
                cl.post(&ingest_url)
                    .bearer_auth(DUMMY_ACCESS_TOKEN)
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), http::StatusCode::BAD_REQUEST);
        assert_eq!(
            "Actual content length 318 does not match the initially declared length 301",
            res.text().await.unwrap()
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

struct DataIngestHarness {
    pub workspace_dir: tempfile::TempDir,
    pub server_harness: ServerSideLocalFsHarness,
    pub system_time: DateTime<Utc>,
}

impl DataIngestHarness {
    fn new() -> Self {
        let workspace_dir = tempfile::tempdir().unwrap();
        let run_info_dir = workspace_dir.path().join("run");
        let cache_dir = workspace_dir.path().join("cache");

        let catalog = dill::CatalogBuilder::new()
            .add::<DataFormatRegistryImpl>()
            .add_builder(PushIngestServiceImpl::builder().with_run_info_dir(run_info_dir))
            .bind::<dyn PushIngestService, PushIngestServiceImpl>()
            .add::<EngineProvisionerNull>()
            .add_builder(UploadServiceLocal::builder().with_cache_dir(cache_dir.clone()))
            .bind::<dyn UploadService, UploadServiceLocal>()
            .add_value(FileUploadLimitConfig {
                max_file_size_in_bytes: 1000,
            })
            .build();

        let server_harness = ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            multi_tenant: true,
            authorized_writes: true,
            base_catalog: Some(catalog),
        });

        let system_time = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();
        server_harness.system_time_source().set(system_time);

        Self {
            workspace_dir,
            server_harness,
            system_time,
        }
    }

    async fn create_population_dataset(&self, with_schema: bool) -> CreateDatasetResult {
        self.server_harness
            .cli_dataset_repository()
            .create_dataset_from_snapshot(DatasetSnapshot {
                name: DatasetAlias::new(
                    self.server_harness.operating_account_name(),
                    DatasetName::new_unchecked("population"),
                ),
                kind: DatasetKind::Root,
                metadata: if with_schema {
                    vec![AddPushSource {
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
                    .into()]
                } else {
                    vec![]
                },
            })
            .await
            .unwrap()
    }

    fn dataset_http_url(&self, alias: &DatasetAlias) -> Url {
        self.server_harness.dataset_url_with_scheme(alias, "http")
    }

    async fn upload_file(&self, upload_id: &str, file_name: &str, file_content: &str) {
        let upload_dir = self
            .workspace_dir
            .path()
            .join("cache")
            .join("uploads")
            .join(
                AccountID::new_seeded_ed25519(SERVER_ACCOUNT_NAME.as_bytes())
                    .as_multibase()
                    .to_string(),
            )
            .join(upload_id);

        std::fs::create_dir_all(upload_dir.clone()).unwrap();

        let file_upload_path = upload_dir.join(file_name);
        tokio::fs::write(&file_upload_path, file_content)
            .await
            .unwrap();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
