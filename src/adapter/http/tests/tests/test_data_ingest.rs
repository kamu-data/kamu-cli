// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, TimeZone, Utc};
use indoc::indoc;
use kamu::domain::upload_service::{FileUploadLimitConfig, UploadToken, UploadTokenBase64Json};
use kamu::domain::*;
use kamu::testing::DatasetDataHelper;
use kamu::*;
use kamu_adapter_http::platform::UploadServiceLocal;
use kamu_datasets::CreateDatasetResult;
use serde_json::json;
use url::Url;
use uuid::Uuid;

use crate::harness::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_push_ingest_handler() {
    let harness = DataIngestHarness::new().await;

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
        pretty_assertions::assert_eq!(http::StatusCode::OK, res.status());

        dataset_helper
            .assert_last_data_eq(
                indoc!(
                    r#"
                    message arrow_schema {
                      REQUIRED INT64 offset;
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
                odf::metadata::AddPushSource {
                    source_name: "source2".to_string(),
                    read: odf::metadata::ReadStepNdJson {
                        schema: Some(vec![
                            "event_time TIMESTAMP".to_owned(),
                            "city STRING".to_owned(),
                            "population BIGINT".to_owned(),
                        ]),
                        ..Default::default()
                    }
                    .into(),
                    preprocess: Some(
                        odf::metadata::TransformSql {
                            engine: "datafusion".to_string(),
                            version: None,
                            temporal_tables: None,
                            query: None,
                            queries: Some(vec![odf::metadata::SqlQueryStep {
                                query: "select event_time, city, population + 1 as population \
                                        from input"
                                    .to_string(),
                                alias: None,
                            }]),
                        }
                        .into(),
                    ),
                    merge: odf::metadata::MergeStrategy::Ledger(
                        odf::metadata::MergeStrategyLedger {
                            primary_key: vec!["event_time".to_owned(), "city".to_owned()],
                        },
                    ),
                }
                .into(),
                odf::dataset::CommitOpts {
                    system_time: Some(harness.system_time),
                    ..odf::dataset::CommitOpts::default()
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
        pretty_assertions::assert_eq!(http::StatusCode::BAD_REQUEST, res.status());

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
        pretty_assertions::assert_eq!(http::StatusCode::OK, res.status());

        dataset_helper
            .assert_last_data_eq(
                indoc!(
                    r#"
                    message arrow_schema {
                      REQUIRED INT64 offset;
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
        pretty_assertions::assert_eq!(http::StatusCode::BAD_REQUEST, res.status());

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
        pretty_assertions::assert_eq!(http::StatusCode::OK, res.status());

        dataset_helper
            .assert_last_data_eq(
                indoc!(
                    r#"
                    message arrow_schema {
                      REQUIRED INT64 offset;
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_push_ingest_upload_token_no_initial_source() {
    let harness = DataIngestHarness::new().await;

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

    let upload_token = UploadTokenBase64Json(UploadToken {
        upload_id,
        file_name: String::from("population.json"),
        owner_account_id: harness.server_account_id().to_string(),
        content_type: Some(MediaType(String::from("application/json"))),
        content_length: FILE_CONTENT.len(),
    });

    let dataset_url = harness.dataset_http_url(&create_result.dataset_handle.alias);

    let client = async move {
        let cl = reqwest::Client::new();
        let dataset_helper = DatasetDataHelper::new(create_result.dataset.clone());
        let ingest_url = format!("{dataset_url}/ingest?uploadToken={upload_token}");
        tracing::info!(%ingest_url, "Client request");

        let res = cl
            .execute(
                cl.post(&ingest_url)
                    .bearer_auth(odf::dataset::DUMMY_ODF_ACCESS_TOKEN)
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        pretty_assertions::assert_eq!(http::StatusCode::OK, res.status());

        dataset_helper
            .assert_last_data_eq(
                indoc!(
                    r#"
                    message arrow_schema {
                      REQUIRED INT64 offset;
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_push_ingest_upload_token_with_initial_source() {
    let harness = DataIngestHarness::new().await;

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

    let upload_token = UploadTokenBase64Json(UploadToken {
        upload_id,
        file_name: String::from("population.json"),
        owner_account_id: harness.server_account_id().to_string(),
        content_type: Some(MediaType(String::from("application/json"))),
        content_length: FILE_CONTENT.len(),
    });

    let dataset_url = harness.dataset_http_url(&create_result.dataset_handle.alias);

    let client = async move {
        let cl = reqwest::Client::new();
        let dataset_helper = DatasetDataHelper::new(create_result.dataset.clone());
        let ingest_url = format!("{dataset_url}/ingest?uploadToken={upload_token}");
        tracing::info!(%ingest_url, "Client request");

        let res = cl
            .execute(
                cl.post(&ingest_url)
                    .bearer_auth(odf::dataset::DUMMY_ODF_ACCESS_TOKEN)
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        pretty_assertions::assert_eq!(http::StatusCode::OK, res.status());

        dataset_helper
            .assert_last_data_eq(
                indoc!(
                    r#"
                    message arrow_schema {
                      REQUIRED INT64 offset;
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_push_ingest_upload_content_type_not_specified() {
    let harness = DataIngestHarness::new().await;

    let create_result = harness.create_population_dataset(false).await;

    const FILE_CONTENT: &str = indoc::indoc!(
        r#"
        {"city": "A", "population": 100}
        {"city": "B", "population": 200}
        "#
    );

    let upload_id = Uuid::new_v4().simple().to_string();

    harness
        .upload_file(&upload_id, "population.ndjson", FILE_CONTENT)
        .await;

    let upload_token = UploadTokenBase64Json(UploadToken {
        upload_id,
        file_name: String::from("population.ndjson"),
        owner_account_id: harness.server_account_id().to_string(),
        content_type: None,
        content_length: FILE_CONTENT.len(),
    });

    let dataset_url = harness.dataset_http_url(&create_result.dataset_handle.alias);

    let client = async move {
        let cl = reqwest::Client::new();
        let dataset_helper = DatasetDataHelper::new(create_result.dataset.clone());
        let ingest_url = format!("{dataset_url}/ingest?uploadToken={upload_token}");
        tracing::info!(%ingest_url, "Client request");

        cl.execute(
            cl.post(&ingest_url)
                .bearer_auth(odf::dataset::DUMMY_ODF_ACCESS_TOKEN)
                .build()
                .unwrap(),
        )
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

        dataset_helper
            .assert_last_data_eq(
                indoc!(
                    r#"
                    message arrow_schema {
                      REQUIRED INT64 offset;
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_push_ingest_upload_token_actual_file_different_size() {
    let harness = DataIngestHarness::new().await;

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

    let upload_token = UploadTokenBase64Json(UploadToken {
        upload_id,
        file_name: String::from("population.json"),
        owner_account_id: harness.server_account_id().to_string(),
        content_type: Some(MediaType(String::from("application/json"))),
        content_length: FILE_CONTENT.len() - 17, // Intentionally wrong file size
    });

    let dataset_url = harness.dataset_http_url(&create_result.dataset_handle.alias);

    let client = async move {
        let cl = reqwest::Client::new();
        let ingest_url = format!("{dataset_url}/ingest?uploadToken={upload_token}");
        tracing::info!(%ingest_url, "Client request");

        let res = cl
            .execute(
                cl.post(&ingest_url)
                    .bearer_auth(odf::dataset::DUMMY_ODF_ACCESS_TOKEN)
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::BAD_REQUEST, res.status());
        pretty_assertions::assert_eq!(
            json!({
                "message": "Actual content length 318 does not match the initially declared length 301"
            }),
            res.json::<serde_json::Value>().await.unwrap()
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DataIngestHarness {
    pub server_harness: ServerSideLocalFsHarness,
    pub system_time: DateTime<Utc>,
}

impl DataIngestHarness {
    async fn new() -> Self {
        let catalog = dill::CatalogBuilder::new()
            .add::<DataFormatRegistryImpl>()
            .add_value(EngineConfigDatafusionEmbeddedIngest::default())
            .add::<PushIngestExecutorImpl>()
            .add::<PushIngestPlannerImpl>()
            .add::<EngineProvisionerNull>()
            .add::<UploadServiceLocal>()
            .add::<PushIngestDataUseCaseImpl>()
            .add_value(FileUploadLimitConfig::new_in_bytes(1000))
            .build();

        let server_harness = ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authorized_writes: true,
            base_catalog: Some(catalog),
        })
        .await;

        let system_time = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();
        server_harness.system_time_source().set(system_time);

        Self {
            server_harness,
            system_time,
        }
    }

    fn server_account_id(&self) -> &odf::AccountID {
        &(self.server_harness.server_account().id)
    }

    async fn create_population_dataset(&self, with_schema: bool) -> CreateDatasetResult {
        self.server_harness
            .cli_create_dataset_from_snapshot_use_case()
            .execute(
                odf::DatasetSnapshot {
                    name: odf::DatasetAlias::new(
                        self.server_harness.operating_account_name(),
                        odf::DatasetName::new_unchecked("population"),
                    ),
                    kind: odf::DatasetKind::Root,
                    metadata: if with_schema {
                        vec![
                            odf::metadata::AddPushSource {
                                source_name: "source1".to_string(),
                                read: odf::metadata::ReadStepNdJson {
                                    schema: Some(vec![
                                        "event_time TIMESTAMP".to_owned(),
                                        "city STRING".to_owned(),
                                        "population BIGINT".to_owned(),
                                    ]),
                                    ..Default::default()
                                }
                                .into(),
                                preprocess: None,
                                merge: odf::metadata::MergeStrategy::Ledger(
                                    odf::metadata::MergeStrategyLedger {
                                        primary_key: vec![
                                            "event_time".to_owned(),
                                            "city".to_owned(),
                                        ],
                                    },
                                ),
                            }
                            .into(),
                        ]
                    } else {
                        vec![]
                    },
                },
                Default::default(),
            )
            .await
            .unwrap()
    }

    fn dataset_http_url(&self, alias: &odf::DatasetAlias) -> Url {
        self.server_harness.dataset_url_with_scheme(alias, "http")
    }

    async fn upload_file(&self, upload_id: &str, file_name: &str, file_content: &str) {
        let cache_dir = self
            .server_harness
            .base_catalog()
            .get_one::<CacheDir>()
            .unwrap();

        let upload_dir = cache_dir
            .join("uploads")
            .join(self.server_account_id().to_string())
            .join(upload_id);

        std::fs::create_dir_all(upload_dir.clone()).unwrap();

        let file_upload_path = upload_dir.join(file_name);
        tokio::fs::write(&file_upload_path, file_content)
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
