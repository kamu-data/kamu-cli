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
        .add::<EngineProvisionerNull>()
        .build();

    let server_harness = ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
        multi_tenant: true,
        authorized_writes: true,
        base_catalog: Some(catalog),
    })
    .await;

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

    let dataset_url =
        server_harness.dataset_url_with_scheme(&create_result.dataset_handle.alias, "http");

    let client = async move {
        let cl: reqwest::Client = reqwest::Client::new();
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
                    system_time: Some(system_time),
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

    await_client_server_flow!(server_harness.api_server_run(), client);
}
