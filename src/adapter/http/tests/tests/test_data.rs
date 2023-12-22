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
use indoc::indoc;
use kamu::domain::*;
use kamu::testing::DatasetDataHelper;
use kamu::*;
use opendatafabric::{MergeStrategy, *};
use serde_json::json;

use crate::harness::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_push_ingest_handler() {
    // TODO: Need access to these from harness level
    let run_info_dir = tempfile::tempdir().unwrap();

    let catalog = dill::CatalogBuilder::new()
        .add::<DataFormatRegistryImpl>()
        .add_builder(
            PushIngestServiceImpl::builder().with_run_info_dir(run_info_dir.path().to_path_buf()),
        )
        .bind::<dyn PushIngestService, PushIngestServiceImpl>()
        .add::<ObjectStoreRegistryImpl>()
        .add::<ObjectStoreBuilderLocalFs>()
        .build();

    let server_harness = ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
        multi_tenant: true,
        authorized_writes: true,
        base_catalog: Some(catalog),
    })
    .await;

    server_harness
        .system_time_source()
        .set(Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap());

    let create_result = server_harness
        .cli_dataset_repository()
        .create_dataset_from_snapshot(
            server_harness.operating_account_name(),
            DatasetSnapshot {
                name: "population".try_into().unwrap(),
                kind: DatasetKind::Root,
                metadata: vec![
                    AddPushSource {
                        source_name: None, // Default source
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
                    .into(),
                    AddPushSource {
                        source_name: Some("device1".to_string()),
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
                    .into(),
                ],
            },
        )
        .await
        .unwrap();

    let dataset_url =
        server_harness.dataset_url_with_scheme(&create_result.dataset_handle.alias, "http");

    let client = async move {
        let cl: reqwest::Client = reqwest::Client::new();
        let dataset_helper = DatasetDataHelper::new(create_result.dataset);
        let ingest_url = format!("{}/ingest", dataset_url);
        tracing::info!(%ingest_url, "Client request");

        // OK - uses default push source
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
                      REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL BYTE_ARRAY city (STRING);
                      OPTIONAL INT64 population;
                    }
                   "#
                ),
                indoc!(
                    r#"
                    +--------+----------------------+----------------------+------+------------+
                    | offset | system_time          | event_time           | city | population |
                    +--------+----------------------+----------------------+------+------------+
                    | 0      | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | A    | 100        |
                    | 1      | 2050-01-01T12:00:00Z | 2020-01-02T00:00:00Z | B    | 200        |
                    +--------+----------------------+----------------------+------+------------+
                    "#
                ),
            )
            .await;

        // OK - uses named source
        let res = cl
            .execute(
                cl.post(&ingest_url)
                    .query(&[("sourceName", "device1")])
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
                      REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL BYTE_ARRAY city (STRING);
                      OPTIONAL INT64 population;
                    }
                   "#
                ),
                indoc!(
                    r#"
                    +--------+----------------------+----------------------+------+------------+
                    | offset | system_time          | event_time           | city | population |
                    +--------+----------------------+----------------------+------+------------+
                    | 2      | 2050-01-01T12:00:00Z | 2020-01-03T00:00:00Z | C    | 300        |
                    +--------+----------------------+----------------------+------+------------+
                    "#
                ),
            )
            .await;

        // ERR - invalid source
        let res = cl
            .execute(
                cl.post(&ingest_url)
                    .query(&[("sourceName", "device2")])
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
                      REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL BYTE_ARRAY city (STRING);
                      OPTIONAL INT64 population;
                    }
                   "#
                ),
                indoc!(
                    r#"
                    +--------+----------------------+----------------------+------+------------+
                    | offset | system_time          | event_time           | city | population |
                    +--------+----------------------+----------------------+------+------------+
                    | 3      | 2050-01-01T12:00:00Z | 2020-01-04T00:00:00Z | D    | 400        |
                    +--------+----------------------+----------------------+------+------------+
                    "#
                ),
            )
            .await;
    };

    await_client_server_flow!(server_harness.api_server_run(), client);
}
