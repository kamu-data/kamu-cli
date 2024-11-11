// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use container_runtime::*;
use datafusion::prelude::*;
use dill::Component;
use indoc::indoc;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets_services::DatasetKeyValueServiceSysEnv;
use opendatafabric::*;
use tempfile::TempDir;
use time_source::{SystemTimeSource, SystemTimeSourceStub};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_snapshot() {
    let harness = IngestTestHarness::new();

    let src_path = harness.temp_dir.path().join("data.csv");

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(FetchStep::Url(FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(vec![
                        "city STRING".to_string(),
                        "population BIGINT".to_string(),
                    ]),
                    ..ReadStepCsv::default()
                }))
                .preprocess(TransformSql {
                    engine: "datafusion".to_string(),
                    version: None,
                    query: None,
                    queries: Some(vec![
                        SqlQueryStep {
                            alias: Some("step1".to_string()),
                            query: indoc::indoc!(
                                r#"
                                select
                                    city,
                                    population * 10 as population
                                from input
                                "#
                            )
                            .to_string(),
                        },
                        SqlQueryStep {
                            alias: None,
                            query: indoc::indoc!(
                                r#"
                                select
                                    city,
                                    population + 1 as population
                                from step1
                                "#
                            )
                            .to_string(),
                        },
                    ]),
                    temporal_tables: None,
                })
                .merge(MergeStrategySnapshot {
                    primary_key: vec!["city".to_string()],
                    compare_columns: None,
                })
                .build(),
        )
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let created = harness.create_dataset(dataset_snapshot).await;
    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    // Round 1
    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            A,1000
            B,2000
            C,3000
            "
        ),
    )
    .unwrap();

    harness.ingest(&created).await.unwrap();

    data_helper
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
                | 0      | 0  | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | A    | 10001      |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | B    | 20001      |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | C    | 30001      |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    assert_eq!(
        data_helper
            .get_last_data_block()
            .await
            .event
            .new_watermark
            .map(|dt| dt.to_rfc3339()),
        Some("2050-01-01T12:00:00+00:00".to_string())
    );

    // Round 2
    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            A,1000
            B,2000
            C,4000
            "
        ),
    )
    .unwrap();

    harness
        .time_source
        .set(Utc.with_ymd_and_hms(2050, 2, 1, 12, 0, 0).unwrap());

    harness.ingest(&created).await.unwrap();

    data_helper
        .assert_last_data_records_eq(indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 3      | 2  | 2050-02-01T12:00:00Z | 2050-01-01T12:00:00Z | C    | 30001      |
            | 4      | 3  | 2050-02-01T12:00:00Z | 2050-02-01T12:00:00Z | C    | 40001      |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ))
        .await;

    assert_eq!(
        data_helper
            .get_last_data_block()
            .await
            .event
            .new_watermark
            .map(|dt| dt.to_rfc3339()),
        Some("2050-02-01T12:00:00+00:00".to_string())
    );

    // Round 3 (no-op, only updates source state)
    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            A,1000
            B,2000
            C,4000
            "
        ),
    )
    .unwrap();

    harness
        .time_source
        .set(Utc.with_ymd_and_hms(2050, 2, 1, 12, 0, 0).unwrap());

    harness.ingest(&created).await.unwrap();
    let event = data_helper.get_last_block_typed::<AddData>().await.event;

    assert_eq!(event.new_data, None);
    assert_eq!(
        event.new_watermark.map(|dt| dt.to_rfc3339()),
        Some("2050-02-01T12:00:00+00:00".to_string())
    );
    assert!(event.new_source_state.is_some());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_ledger() {
    let harness = IngestTestHarness::new();
    let src_path = harness.temp_dir.path().join("data.csv");

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(FetchStep::Url(FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        ["date TIMESTAMP", "city STRING", "population BIGINT"]
                            .iter()
                            .map(|s| (*s).to_string())
                            .collect(),
                    ),
                    ..ReadStepCsv::default()
                }))
                .merge(MergeStrategyLedger {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                })
                .build(),
        )
        .push_event(SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let created = harness.create_dataset(dataset_snapshot).await;
    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    // Round 1
    std::fs::write(
        &src_path,
        indoc!(
            "
            date,city,population
            2020-01-01,A,1000
            2020-01-01,B,2000
            2020-01-01,C,3000
            "
        ),
    )
    .unwrap();

    harness.ingest(&created).await.unwrap();
    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+------+------------+
                | offset | op | system_time          | date                 | city | population |
                +--------+----+----------------------+----------------------+------+------------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | A    | 1000       |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | B    | 2000       |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | C    | 3000       |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;

    assert_eq!(
        data_helper
            .get_last_data_block()
            .await
            .event
            .new_watermark
            .map(|dt| dt.to_rfc3339()),
        Some("2020-01-01T00:00:00+00:00".to_string())
    );

    // Round 2
    std::fs::write(
        &src_path,
        indoc!(
            "
            date,city,population
            2020-01-01,B,2000
            2020-01-01,C,3000
            2021-01-01,C,4000
            "
        ),
    )
    .unwrap();

    harness.ingest(&created).await.unwrap();

    data_helper
        .assert_last_data_records_eq(indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | date                 | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 3      | 0  | 2050-01-01T12:00:00Z | 2021-01-01T00:00:00Z | C    | 4000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ))
        .await;

    assert_eq!(
        data_helper
            .get_last_data_block()
            .await
            .event
            .new_watermark
            .map(|dt| dt.to_rfc3339()),
        Some("2021-01-01T00:00:00+00:00".to_string())
    );

    // Round 3
    std::fs::write(
        &src_path,
        indoc!(
            "
            date,city,population
            2020-01-01,D,4000
            "
        ),
    )
    .unwrap();

    harness.ingest(&created).await.unwrap();

    data_helper
        .assert_last_data_records_eq(indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | date                 | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 4      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | D    | 4000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ))
        .await;

    assert_eq!(
        data_helper
            .get_last_data_block()
            .await
            .event
            .new_watermark
            .map(|dt| dt.to_rfc3339()),
        Some("2021-01-01T00:00:00+00:00".to_string())
    );

    // Round 4 (duplicate data, commit only updates the source state)
    std::fs::write(
        &src_path,
        indoc!(
            "
            date,city,population
            2020-01-01,D,4000
            "
        ),
    )
    .unwrap();

    harness.ingest(&created).await.unwrap();
    let event = data_helper.get_last_block_typed::<AddData>().await.event;

    assert_eq!(event.new_data, None);
    assert_eq!(
        event.new_watermark.map(|dt| dt.to_rfc3339()),
        Some("2021-01-01T00:00:00+00:00".to_string())
    );
    assert!(event.new_source_state.is_some());

    // Round 5 (empty data, commit only updates the source state)
    std::fs::write(&src_path, "").unwrap();

    harness.ingest(&created).await.unwrap();
    let event = data_helper.get_last_block_typed::<AddData>().await.event;

    assert_eq!(event.new_data, None);
    assert_eq!(
        event.new_watermark.map(|dt| dt.to_rfc3339()),
        Some("2021-01-01T00:00:00+00:00".to_string())
    );
    assert!(event.new_source_state.is_some());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_empty_data() {
    let harness = IngestTestHarness::new();
    let src_path = harness.temp_dir.path().join("data.csv");

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(FetchStep::Url(FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(false),
                    schema: None,
                    ..ReadStepCsv::default()
                }))
                .preprocess(TransformSql {
                    engine: "datafusion".to_string(),
                    version: None,
                    query: Some("select date, city, population from input".to_string()),
                    queries: None,
                    temporal_tables: None,
                })
                .merge(MergeStrategyLedger {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                })
                .build(),
        )
        .push_event(SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let created = harness.create_dataset(dataset_snapshot).await;
    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    std::fs::write(&src_path, "").unwrap();
    harness.ingest(&created).await.unwrap();

    // Should only contain source state
    let event = data_helper.get_last_block_typed::<AddData>().await.event;
    assert_eq!(event.new_data, None);
    assert_eq!(event.new_watermark, None);
    assert!(event.new_source_state.is_some());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_event_time_as_date() {
    let harness = IngestTestHarness::new();
    let src_path = harness.temp_dir.path().join("data.csv");

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(FetchStep::Url(FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        ["date DATE", "city STRING", "population BIGINT"]
                            .iter()
                            .map(|s| (*s).to_string())
                            .collect(),
                    ),
                    ..ReadStepCsv::default()
                }))
                .merge(MergeStrategySnapshot {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                    compare_columns: None,
                })
                .build(),
        )
        .push_event(SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let created = harness.create_dataset(dataset_snapshot).await;
    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    std::fs::write(
        &src_path,
        indoc!(
            "
            date,city,population
            2020-01-01,A,1000
            2020-01-01,B,2000
            2020-01-01,C,3000
            "
        ),
    )
    .unwrap();

    harness.ingest(&created).await.unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT32 date (DATE);
                  OPTIONAL BYTE_ARRAY city (STRING);
                  OPTIONAL INT64 population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+------------+------+------------+
                | offset | op | system_time          | date       | city | population |
                +--------+----+----------------------+------------+------+------------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-01 | A    | 1000       |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-01 | B    | 2000       |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2020-01-01 | C    | 3000       |
                +--------+----+----------------------+------------+------+------------+
                "#
            ),
        )
        .await;

    assert_eq!(
        data_helper
            .get_last_data_block()
            .await
            .event
            .new_watermark
            .map(|dt| dt.to_rfc3339()),
        Some("2020-01-01T00:00:00+00:00".to_string())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_event_time_of_invalid_type() {
    let harness = IngestTestHarness::new();
    let src_path = harness.temp_dir.path().join("data.csv");

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(FetchStep::Url(FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(vec![
                        "date STRING".to_string(),
                        "city STRING".to_string(),
                        "population BIGINT".to_string(),
                    ]),
                    ..ReadStepCsv::default()
                }))
                .build(),
        )
        .push_event(SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .build();

    let created = harness.create_dataset(dataset_snapshot).await;

    std::fs::write(
        &src_path,
        indoc!(
            "
            date,city,population
            2020-01-01,A,1000
            2020-01-01,B,2000
            2020-01-01,C,3000
            "
        ),
    )
    .unwrap();

    let res = harness.ingest(&created).await;
    assert_matches!(res, Err(PollingIngestError::BadInputSchema(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_bad_column_names_preserve() {
    let harness = IngestTestHarness::new();
    let src_path = harness.temp_dir.path().join("data.json");

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(FetchStep::Url(FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(ReadStepNdJson {
                    schema: Some(vec![
                        "\"Date (UTC)\" DATE not null".to_string(),
                        "\"City Name\" STRING not null".to_string(),
                        "\"Population\" BIGINT not null".to_string(),
                    ]),
                    ..ReadStepNdJson::default()
                })
                .build(),
        )
        .push_event(SetVocab {
            event_time_column: Some("Date (UTC)".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let created = harness.create_dataset(dataset_snapshot).await;
    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    std::fs::write(
        &src_path,
        indoc!(
            r#"
            {"Date (UTC)": "2020-01-01", "City Name": "A", "Population": 1000}
            {"Date (UTC)": "2020-01-01", "City Name": "B", "Population": 2000}
            {"Date (UTC)": "2020-01-01", "City Name": "C", "Population": 3000}
            "#
        ),
    )
    .unwrap();

    harness.ingest(&created).await.unwrap();
    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT32 Date (UTC) (DATE);
                  REQUIRED BYTE_ARRAY City Name (STRING);
                  REQUIRED INT64 Population;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+------------+-----------+------------+
                | offset | op | system_time          | Date (UTC) | City Name | Population |
                +--------+----+----------------------+------------+-----------+------------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-01 | A         | 1000       |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-01 | B         | 2000       |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2020-01-01 | C         | 3000       |
                +--------+----+----------------------+------------+-----------+------------+
                "#
            ),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_bad_column_names_rename() {
    let harness = IngestTestHarness::new();
    let src_path = harness.temp_dir.path().join("data.json");

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(FetchStep::Url(FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(ReadStepNdJson {
                    ..ReadStepNdJson::default()
                })
                .preprocess(TransformSql {
                    engine: "datafusion".to_string(),
                    version: None,
                    query: Some(
                        r#"
                    select
                        to_timestamp_millis("Timestamp (UTC)") as event_time,
                        "City Name" as city,
                        "Population" as population
                    from input
                    "#
                        .to_string(),
                    ),
                    queries: None,
                    temporal_tables: None,
                })
                .build(),
        )
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let created = harness.create_dataset(dataset_snapshot).await;
    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    std::fs::write(
        &src_path,
        indoc!(
            r#"
            {"Timestamp (UTC)": "2020-01-01T12:00:00", "City Name": "A", "Population": 1000}
            {"Timestamp (UTC)": "2020-01-01T12:00:00", "City Name": "B", "Population": 2000}
            {"Timestamp (UTC)": "2020-01-01T12:00:00", "City Name": "C", "Population": 3000}
            "#
        ),
    )
    .unwrap();

    harness.ingest(&created).await.unwrap();

    data_helper
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
                | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T12:00:00Z | A    | 1000       |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T12:00:00Z | B    | 2000       |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T12:00:00Z | C    | 3000       |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// See: https://github.com/apache/datafusion/issues/7460
// See: https://github.com/kamu-data/kamu-cli/issues/899
#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_schema_case_sensitivity() {
    let harness = IngestTestHarness::new();

    let src_path = harness.temp_dir.path().join("data.csv");

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(FetchStep::Url(FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: None,
                    cache: None,
                    headers: None,
                }))
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(vec![
                        "date TIMESTAMP".to_string(),
                        "UPPER STRING".to_string(),
                        "lower BIGINT".to_string(),
                    ]),
                    ..ReadStepCsv::default()
                }))
                .merge(MergeStrategyLedger {
                    primary_key: vec!["date".to_string()],
                })
                .build(),
        )
        .push_event(SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let created = harness.create_dataset(dataset_snapshot).await;
    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    // Round 1
    std::fs::write(
        &src_path,
        indoc!(
            "
            date,UPPER,lower
            2020-01-01,A,1000
            2020-01-02,B,2000
            2020-01-03,C,3000
            "
        ),
    )
    .unwrap();

    harness.ingest(&created).await.unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
                  OPTIONAL BYTE_ARRAY UPPER (STRING);
                  OPTIONAL INT64 lower;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+-------+-------+
                | offset | op | system_time          | date                 | UPPER | lower |
                +--------+----+----------------------+----------------------+-------+-------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | A     | 1000  |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2020-01-02T00:00:00Z | B     | 2000  |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2020-01-03T00:00:00Z | C     | 3000  |
                +--------+----+----------------------+----------------------+-------+-------+
                "#
            ),
        )
        .await;

    assert_eq!(
        data_helper
            .get_last_data_block()
            .await
            .event
            .new_watermark
            .map(|dt| dt.to_rfc3339()),
        Some("2020-01-03T00:00:00+00:00".to_string())
    );

    // Round 2
    std::fs::write(
        &src_path,
        indoc!(
            "
            date,UPPER,lower
            2020-01-01,A,1000
            2020-01-02,B,2000
            2020-01-03,C,3000
            2020-01-04,D,4000
            "
        ),
    )
    .unwrap();

    harness
        .time_source
        .set(Utc.with_ymd_and_hms(2050, 1, 2, 12, 0, 0).unwrap());

    harness.ingest(&created).await.unwrap();

    data_helper
        .assert_last_data_records_eq(indoc!(
            r#"
            +--------+----+----------------------+----------------------+-------+-------+
            | offset | op | system_time          | date                 | UPPER | lower |
            +--------+----+----------------------+----------------------+-------+-------+
            | 3      | 0  | 2050-01-02T12:00:00Z | 2020-01-04T00:00:00Z | D     | 4000  |
            +--------+----+----------------------+----------------------+-------+-------+
            "#
        ))
        .await;

    assert_eq!(
        data_helper
            .get_last_data_block()
            .await
            .event
            .new_watermark
            .map(|dt| dt.to_rfc3339()),
        Some("2020-01-04T00:00:00+00:00".to_string())
    );

    // Round 3 (no-op)
    std::fs::write(
        &src_path,
        indoc!(
            "
            date,UPPER,lower
            2020-01-01,A,1000
            2020-01-02,B,2000
            2020-01-03,C,3000
            2020-01-04,D,4000
            "
        ),
    )
    .unwrap();

    harness
        .time_source
        .set(Utc.with_ymd_and_hms(2050, 1, 3, 12, 0, 0).unwrap());

    harness.ingest(&created).await.unwrap();
    let event = data_helper.get_last_block_typed::<AddData>().await.event;

    assert_eq!(event.new_data, None);
    assert_eq!(
        event.new_watermark.map(|dt| dt.to_rfc3339()),
        Some("2020-01-04T00:00:00+00:00".to_string())
    );
    assert!(event.new_source_state.is_some());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, ingest, spark)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_preprocess_with_spark() {
    let harness = IngestTestHarness::new();

    let src_path = harness.temp_dir.path().join("data.csv");
    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            A,1000
            B,2000
            C,3000
            "
        ),
    )
    .unwrap();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch_file(&src_path)
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(vec![
                        "city STRING".to_string(),
                        "population BIGINT".to_string(),
                    ]),
                    ..ReadStepCsv::default()
                }))
                .preprocess(TransformSql {
                    engine: "spark".to_string(),
                    version: None,
                    query: Some(
                        indoc::indoc!(
                            r#"
                            select
                                city,
                                population * 10 as population
                            from input
                            "#
                        )
                        .to_string(),
                    ),
                    queries: None,
                    temporal_tables: None,
                })
                .build(),
        )
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let created = harness.create_dataset(dataset_snapshot).await;
    harness.ingest(&created).await.unwrap();

    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    data_helper
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
                | 0      | 0  | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | A    | 10000      |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | B    | 20000      |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | C    | 30000      |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, ingest, flink)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_preprocess_with_flink() {
    let harness = IngestTestHarness::new();

    let src_path = harness.temp_dir.path().join("data.csv");
    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            A,1000
            B,2000
            C,3000
            "
        ),
    )
    .unwrap();

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch_file(&src_path)
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(vec![
                        "city STRING".to_string(),
                        "population BIGINT".to_string(),
                    ]),
                    ..ReadStepCsv::default()
                }))
                .preprocess(TransformSql {
                    engine: "flink".to_string(),
                    version: None,
                    query: Some(
                        indoc::indoc!(
                            r#"
                            select
                                city,
                                population * 10 as population
                            from input
                            "#
                        )
                        .to_string(),
                    ),
                    queries: None,
                    temporal_tables: None,
                })
                .build(),
        )
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let created = harness.create_dataset(dataset_snapshot).await;
    harness.ingest(&created).await.unwrap();

    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    data_helper
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
                | 0      | 0  | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | A    | 10000      |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | B    | 20000      |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | C    | 30000      |
                +--------+----+----------------------+----------------------+------+------------+
                "#
            ),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct IngestTestHarness {
    temp_dir: TempDir,
    dataset_repo: Arc<DatasetRepositoryLocalFs>,
    ingest_svc: Arc<dyn PollingIngestService>,
    time_source: Arc<SystemTimeSourceStub>,
    ctx: SessionContext,
}

impl IngestTestHarness {
    fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = temp_dir.path().join("run");
        let cache_dir = temp_dir.path().join("cache");
        let datasets_dir = temp_dir.path().join("datasets");
        std::fs::create_dir(&run_info_dir).unwrap();
        std::fs::create_dir(&cache_dir).unwrap();
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add_value(RunInfoDir::new(run_info_dir))
            .add_value(CacheDir::new(cache_dir))
            .add_value(ContainerRuntimeConfig::default())
            .add::<ContainerRuntime>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(DatasetRepositoryLocalFs::builder().with_root(datasets_dir))
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(EngineProvisionerLocalConfig::default())
            .add::<EngineProvisionerLocal>()
            .add_value(SystemTimeSourceStub::new_set(
                Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
            ))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<DataFormatRegistryImpl>()
            .add::<FetchService>()
            .add::<PollingIngestServiceImpl>()
            .add::<DatasetKeyValueServiceSysEnv>()
            .build();

        let dataset_repo = catalog.get_one::<DatasetRepositoryLocalFs>().unwrap();
        let ingest_svc = catalog.get_one::<dyn PollingIngestService>().unwrap();
        let time_source = catalog.get_one::<SystemTimeSourceStub>().unwrap();

        Self {
            temp_dir,
            dataset_repo,
            ingest_svc,
            time_source,
            ctx: SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1)),
        }
    }

    async fn create_dataset(&self, dataset_snapshot: DatasetSnapshot) -> CreateDatasetResult {
        self.dataset_repo
            .create_dataset_from_snapshot(dataset_snapshot)
            .await
            .unwrap()
            .create_dataset_result
    }

    async fn ingest(
        &self,
        created: &CreateDatasetResult,
    ) -> Result<PollingIngestResult, PollingIngestError> {
        self.ingest_svc
            .ingest(
                ResolvedDataset::from(created),
                PollingIngestOptions::default(),
                None,
            )
            .await
            .result
    }

    async fn dataset_data_helper(&self, dataset_alias: &DatasetAlias) -> DatasetDataHelper {
        let dataset = self
            .dataset_repo
            .get_dataset_by_ref(&dataset_alias.as_local_ref())
            .await
            .unwrap();

        DatasetDataHelper::new_with_context(dataset, self.ctx.clone())
    }
}
