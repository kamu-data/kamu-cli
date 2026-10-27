// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use container_runtime::*;
use datafusion::prelude::*;
use indoc::indoc;
use internal_error::ResultIntoInternal;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::*;
use kamu_datasets_services::DatasetKeyValueServiceSysEnv;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::testing::MetadataFactory;
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
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(odf::metadata::FetchStep::Url(odf::metadata::FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(odf::metadata::EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(odf::metadata::ReadStep::Csv(odf::metadata::ReadStepCsv {
                    header: Some(true),
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("city"),
                        odf::schema::DataField::i64("population"),
                    ])),
                    ..odf::metadata::ReadStepCsv::default()
                }))
                .preprocess(odf::metadata::TransformSql {
                    engine: "datafusion".to_string(),
                    version: None,
                    query: None,
                    queries: Some(vec![
                        odf::metadata::SqlQueryStep {
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
                        odf::metadata::SqlQueryStep {
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
                .merge(odf::metadata::MergeStrategySnapshot {
                    primary_key: vec!["city".to_string()],
                    compare_columns: None,
                })
                .build(),
        )
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

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

    harness.ingest(target.clone()).await.unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
                  REQUIRED BYTE_ARRAY city (STRING);
                  REQUIRED INT64 population;
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

    harness.ingest(target.clone()).await.unwrap();

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

    harness.ingest(target).await.unwrap();
    let event = data_helper
        .get_last_block_typed::<odf::metadata::AddData>()
        .await
        .event;

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
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(odf::metadata::FetchStep::Url(odf::metadata::FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(odf::metadata::EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(odf::metadata::ReadStep::Csv(odf::metadata::ReadStepCsv {
                    header: Some(true),
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::timestamp_millis_utc("date"),
                        odf::schema::DataField::string("city"),
                        odf::schema::DataField::i64("population"),
                    ])),
                    ..odf::metadata::ReadStepCsv::default()
                }))
                .merge(odf::metadata::MergeStrategyLedger {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                })
                .build(),
        )
        .push_event(odf::metadata::SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

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

    harness.ingest(target.clone()).await.unwrap();
    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
                  REQUIRED BYTE_ARRAY city (STRING);
                  REQUIRED INT64 population;
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

    harness.ingest(target.clone()).await.unwrap();

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

    harness.ingest(target.clone()).await.unwrap();

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

    harness.ingest(target.clone()).await.unwrap();
    let event = data_helper
        .get_last_block_typed::<odf::metadata::AddData>()
        .await
        .event;

    assert_eq!(event.new_data, None);
    assert_eq!(
        event.new_watermark.map(|dt| dt.to_rfc3339()),
        Some("2021-01-01T00:00:00+00:00".to_string())
    );
    assert!(event.new_source_state.is_some());

    // Round 5 (empty data, commit only updates the source state)
    std::fs::write(&src_path, "").unwrap();

    harness.ingest(target).await.unwrap();
    let event = data_helper
        .get_last_block_typed::<odf::metadata::AddData>()
        .await
        .event;

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
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(odf::metadata::FetchStep::Url(odf::metadata::FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(odf::metadata::EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(odf::metadata::ReadStep::Csv(odf::metadata::ReadStepCsv {
                    header: Some(false),
                    schema: None,
                    ..odf::metadata::ReadStepCsv::default()
                }))
                .preprocess(odf::metadata::TransformSql {
                    engine: "datafusion".to_string(),
                    version: None,
                    query: Some("select date, city, population from input".to_string()),
                    queries: None,
                    temporal_tables: None,
                })
                .merge(odf::metadata::MergeStrategyLedger {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                })
                .build(),
        )
        .push_event(odf::metadata::SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    std::fs::write(&src_path, "").unwrap();
    harness.ingest(target).await.unwrap();

    // Should only contain source state
    let event = data_helper
        .get_last_block_typed::<odf::metadata::AddData>()
        .await
        .event;
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
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(odf::metadata::FetchStep::Url(odf::metadata::FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(odf::metadata::EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(odf::metadata::ReadStep::Csv(odf::metadata::ReadStepCsv {
                    header: Some(true),
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::date("date"),
                        odf::schema::DataField::string("city"),
                        odf::schema::DataField::i64("population"),
                    ])),
                    ..odf::metadata::ReadStepCsv::default()
                }))
                .merge(odf::metadata::MergeStrategySnapshot {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                    compare_columns: None,
                })
                .build(),
        )
        .push_event(odf::metadata::SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

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

    harness.ingest(target).await.unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT32 date (DATE);
                  REQUIRED BYTE_ARRAY city (STRING);
                  REQUIRED INT64 population;
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
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(odf::metadata::FetchStep::Url(odf::metadata::FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(odf::metadata::EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(odf::metadata::ReadStep::Csv(odf::metadata::ReadStepCsv {
                    header: Some(true),
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("date"),
                        odf::schema::DataField::string("city"),
                        odf::schema::DataField::i64("population"),
                    ])),
                    ..odf::metadata::ReadStepCsv::default()
                }))
                .build(),
        )
        .push_event(odf::metadata::SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

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

    let res = harness.ingest(target).await;
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
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(odf::metadata::FetchStep::Url(odf::metadata::FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(odf::metadata::EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(odf::metadata::ReadStepNdJson {
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::date("Date (UTC)"),
                        odf::schema::DataField::string("City Name"),
                        odf::schema::DataField::i64("Population"),
                    ])),
                    ..odf::metadata::ReadStepNdJson::default()
                })
                .build(),
        )
        .push_event(odf::metadata::SetVocab {
            event_time_column: Some("Date (UTC)".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

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

    harness.ingest(target).await.unwrap();
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
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(odf::metadata::FetchStep::Url(odf::metadata::FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(odf::metadata::EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(odf::metadata::ReadStepNdJson {
                    ..odf::metadata::ReadStepNdJson::default()
                })
                .preprocess(odf::metadata::TransformSql {
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
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

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

    harness.ingest(target).await.unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
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
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(odf::metadata::FetchStep::Url(odf::metadata::FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: None,
                    cache: None,
                    headers: None,
                }))
                .read(odf::metadata::ReadStep::Csv(odf::metadata::ReadStepCsv {
                    header: Some(true),
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::timestamp_millis_utc("date"),
                        odf::schema::DataField::string("UPPER"),
                        odf::schema::DataField::i64("lower"),
                    ])),
                    ..odf::metadata::ReadStepCsv::default()
                }))
                .merge(odf::metadata::MergeStrategyLedger {
                    primary_key: vec!["date".to_string()],
                })
                .build(),
        )
        .push_event(odf::metadata::SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

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

    harness.ingest(target.clone()).await.unwrap();

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
                  REQUIRED BYTE_ARRAY UPPER (STRING);
                  REQUIRED INT64 lower;
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

    harness.ingest(target.clone()).await.unwrap();

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

    harness.ingest(target).await.unwrap();
    let event = data_helper
        .get_last_block_typed::<odf::metadata::AddData>()
        .await
        .event;

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
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch_file(&src_path)
                .read(odf::metadata::ReadStep::Csv(odf::metadata::ReadStepCsv {
                    header: Some(true),
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("city"),
                        odf::schema::DataField::i64("population"),
                    ])),
                    ..odf::metadata::ReadStepCsv::default()
                }))
                .preprocess(odf::metadata::TransformSql {
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
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

    harness.ingest(target).await.unwrap();

    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
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
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch_file(&src_path)
                .read(odf::metadata::ReadStep::Csv(odf::metadata::ReadStepCsv {
                    header: Some(true),
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("city"),
                        odf::schema::DataField::i64("population"),
                    ])),
                    ..odf::metadata::ReadStepCsv::default()
                }))
                .preprocess(odf::metadata::TransformSql {
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
    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

    harness.ingest(target).await.unwrap();

    let data_helper = harness.dataset_data_helper(&dataset_alias).await;

    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
                  REQUIRED BYTE_ARRAY city (STRING);
                  REQUIRED INT64 population;
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

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_deprecated_ddl_schema() {
    let harness = IngestTestHarness::new();
    let src_path = harness.temp_dir.path().join("data.csv");

    let dataset_snapshot = MetadataFactory::dataset_snapshot()
        .name("foo.bar")
        .kind(odf::DatasetKind::Root)
        .push_event(
            MetadataFactory::set_polling_source()
                .fetch(odf::metadata::FetchStep::Url(odf::metadata::FetchStepUrl {
                    url: url::Url::from_file_path(&src_path)
                        .unwrap()
                        .as_str()
                        .to_owned(),
                    event_time: Some(odf::metadata::EventTimeSourceFromSystemTime {}.into()),
                    cache: None,
                    headers: None,
                }))
                .read(odf::metadata::ReadStep::Csv(odf::metadata::ReadStepCsv {
                    header: Some(true),
                    ddl_schema: Some(vec![
                        "date TIMESTAMP".to_owned(),
                        "city STRING".to_owned(),
                        "population BIGINT".to_owned(),
                    ]),
                    ..odf::metadata::ReadStepCsv::default()
                }))
                .merge(odf::metadata::MergeStrategyLedger {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                })
                .build(),
        )
        .push_event(odf::metadata::SetVocab {
            event_time_column: Some("date".to_string()),
            ..Default::default()
        })
        .build();

    let dataset_alias = dataset_snapshot.name.clone();

    let stored = harness.create_dataset(dataset_snapshot).await;
    let target = ResolvedDataset::from_stored(&stored, &dataset_alias);

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

    harness.ingest(target.clone()).await.unwrap();
    data_helper
        .assert_last_data_eq(
            indoc!(
                r#"
                message arrow_schema {
                  REQUIRED INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  REQUIRED INT64 date (TIMESTAMP(MILLIS,true));
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_reports_increment_and_removes_its_directory() {
    let harness = IngestTestHarness::new();
    let src_path = harness.temp_dir.path().join("data.csv");
    let target = harness.create_csv_polling_dataset(&src_path).await;
    std::fs::write(&src_path, "city,population\nA,1000\nB,2000\nC,3000\n").unwrap();

    let res = harness.ingest(target).await.unwrap();

    assert_matches!(
        res.result,
        PollingIngestResult::Updated {
            increment: odf::dataset::MetadataChainIncrementInterval {
                num_blocks: 2,
                num_records: 3,
                ..
            },
            new_source_state: Some(odf::metadata::SourceState { kind, .. }),
            has_more: false,
            ..
        } if kind == odf::metadata::SourceState::KIND_LAST_MODIFIED
    );
    assert_eq!(harness.ingest_dirs_left(), Vec::<String>::new());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_failure_keeps_its_directory() {
    let harness = IngestTestHarness::new();
    let src_path = harness.temp_dir.path().join("data.csv");
    let target = harness.create_csv_polling_dataset(&src_path).await;
    std::fs::write(&src_path, "city,population\nA,not-a-number\n").unwrap();

    let res = harness.ingest(target).await;

    assert_matches!(res, Err(e) if !e.is_recoverable());
    assert_eq!(harness.ingest_dirs_left().len(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_savepoint_without_its_data_fetches_again() {
    let harness = IngestTestHarness::new();
    let target = harness.create_csv_glob_polling_dataset("data-*.csv").await;

    // The failed run leaves a savepoint that refers to the source file itself
    harness.write_source_file("data-1.csv", "city,population\nA,not-a-number\n");
    assert_matches!(harness.ingest(target.clone()).await, Err(_));
    assert_eq!(harness.savepoints_left(), 1);

    harness.remove_source_file("data-1.csv");
    harness.write_source_file("data-2.csv", "city,population\nB,2000\n");

    let res = harness.ingest(target).await.unwrap();

    assert_matches!(
        res.result,
        PollingIngestResult::Updated {
            increment: odf::dataset::MetadataChainIncrementInterval { num_records: 1, .. },
            ..
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_polling_unreadable_savepoint_fetches_again() {
    let harness = IngestTestHarness::new();
    let src_path = harness.temp_dir.path().join("data.csv");
    let target = harness.create_csv_polling_dataset(&src_path).await;

    std::fs::write(&src_path, "city,population\nA,not-a-number\n").unwrap();
    assert_matches!(harness.ingest(target.clone()).await, Err(_));
    assert_eq!(harness.savepoints_left(), 1);
    harness.truncate_savepoints();

    std::fs::write(&src_path, "city,population\nA,1000\n").unwrap();
    let res = harness.ingest(target).await.unwrap();

    assert_matches!(
        res.result,
        PollingIngestResult::Updated {
            increment: odf::dataset::MetadataChainIncrementInterval { num_records: 1, .. },
            ..
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct IngestTestHarness {
    temp_dir: TempDir,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    ingest_svc: Arc<dyn PollingIngestService>,
    did_generator: Arc<dyn DidGenerator>,
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

        let mut b = dill::CatalogBuilder::new();

        b.add::<DidGeneratorDefault>()
            .add_value(RunInfoDir::new(run_info_dir))
            .add_value(CacheDir::new(cache_dir))
            .add_value(ContainerRuntimeConfig::default())
            .add::<ContainerRuntime>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                datasets_dir,
            ))
            .add::<odf::dataset::DatasetLfsBuilderDefault>()
            .add::<DatasetRegistrySoloUnitBridge>()
            .add_value(EngineProvisionerLocalConfig::default())
            .add::<EngineProvisionerLocal>()
            .add_value(SystemTimeSourceStub::new_set(
                Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
            ))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<DataFormatRegistryImpl>()
            .add::<FetchService>()
            .add_value(EngineConfigDatafusionEmbeddedIngest::default())
            .add::<PollingIngestServiceImpl>()
            .add::<DatasetKeyValueServiceSysEnv>();

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);
        let catalog = b.build();

        Self {
            temp_dir,
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
            ingest_svc: catalog.get_one().unwrap(),
            did_generator: catalog.get_one().unwrap(),
            time_source: catalog.get_one().unwrap(),
            ctx: SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1)),
        }
    }

    async fn create_dataset(
        &self,
        dataset_snapshot: odf::DatasetSnapshot,
    ) -> odf::dataset::StoreDatasetResult {
        create_test_dataset_from_snapshot(
            self.dataset_registry.as_ref(),
            self.dataset_storage_unit_writer.as_ref(),
            dataset_snapshot,
            self.did_generator.generate_dataset_id().0,
            self.time_source.now(),
        )
        .await
        .unwrap()
    }

    /// A root dataset polling a CSV file with `city` and `population`
    async fn create_csv_polling_dataset(&self, src_path: &std::path::Path) -> ResolvedDataset {
        self.create_csv_dataset_fetching(odf::metadata::FetchStep::Url(
            odf::metadata::FetchStepUrl {
                url: url::Url::from_file_path(src_path)
                    .unwrap()
                    .as_str()
                    .to_owned(),
                event_time: Some(odf::metadata::EventTimeSourceFromSystemTime {}.into()),
                cache: None,
                headers: None,
            },
        ))
        .await
    }

    /// A root dataset polling CSV files with `city` and `population` that
    /// match `glob` in the temporary directory, one file per iteration
    async fn create_csv_glob_polling_dataset(&self, glob: &str) -> ResolvedDataset {
        self.create_csv_dataset_fetching(odf::metadata::FetchStep::FilesGlob(
            odf::metadata::FetchStepFilesGlob {
                path: self
                    .temp_dir
                    .path()
                    .join(glob)
                    .to_string_lossy()
                    .into_owned(),
                event_time: Some(odf::metadata::EventTimeSourceFromSystemTime {}.into()),
                cache: None,
                order: None,
            },
        ))
        .await
    }

    async fn create_csv_dataset_fetching(
        &self,
        fetch_step: odf::metadata::FetchStep,
    ) -> ResolvedDataset {
        let dataset_snapshot = MetadataFactory::dataset_snapshot()
            .name("foo")
            .kind(odf::DatasetKind::Root)
            .push_event(
                MetadataFactory::set_polling_source()
                    .fetch(fetch_step)
                    .read(odf::metadata::ReadStep::Csv(odf::metadata::ReadStepCsv {
                        header: Some(true),
                        schema: Some(odf::schema::DataSchema::new(vec![
                            odf::schema::DataField::string("city"),
                            odf::schema::DataField::i64("population"),
                        ])),
                        ..odf::metadata::ReadStepCsv::default()
                    }))
                    .build(),
            )
            .build();

        let dataset_alias = dataset_snapshot.name.clone();
        let stored = self.create_dataset(dataset_snapshot).await;
        ResolvedDataset::from_stored(&stored, &dataset_alias)
    }

    fn write_source_file(&self, name: &str, contents: &str) {
        std::fs::write(self.temp_dir.path().join(name), contents).unwrap();
    }

    fn remove_source_file(&self, name: &str) {
        std::fs::remove_file(self.temp_dir.path().join(name)).unwrap();
    }

    fn savepoint_paths(&self) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(self.temp_dir.path().join("cache"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("fetch-savepoint-")
            })
            .collect()
    }

    fn savepoints_left(&self) -> usize {
        self.savepoint_paths().len()
    }

    /// Cuts every savepoint short, as a write interrupted by a crash would
    fn truncate_savepoints(&self) {
        for path in self.savepoint_paths() {
            std::fs::write(
                path,
                "kind: FetchSavepoint\nversion: 1\ncontent:\n  createdAt: 20",
            )
            .unwrap();
        }
    }

    /// Ingest operation directories left in the run directory
    fn ingest_dirs_left(&self) -> Vec<String> {
        std::fs::read_dir(self.temp_dir.path().join("run"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("ingest-"))
            .collect()
    }

    async fn ingest(
        &self,
        target: ResolvedDataset,
    ) -> Result<PollingIngestResponse, PollingIngestError> {
        let metadata_state =
            DataWriterMetadataState::build(target.clone(), &odf::BlockRef::Head, None, None)
                .await
                .unwrap();

        let ingest_res = self
            .ingest_svc
            .ingest(
                target.clone(),
                PollingIngestPlan::new(target.get_handle(), Box::new(metadata_state)).unwrap(),
                PollingIngestOptions::default(),
                &IngestEnvVars::default(),
                &random_strings::get_random_name(None, 10),
                None,
            )
            .await;

        if let Ok(PollingIngestResponse { result, .. }) = &ingest_res
            && let PollingIngestResult::Updated {
                old_head, new_head, ..
            } = result
        {
            target
                .as_metadata_chain()
                .set_ref(
                    &odf::BlockRef::Head,
                    new_head,
                    odf::dataset::SetRefOpts {
                        validate_block_present: true,
                        check_ref_is: Some(Some(old_head)),
                    },
                )
                .await
                .int_err()?;
        }

        ingest_res
    }

    async fn dataset_data_helper(&self, dataset_alias: &odf::DatasetAlias) -> DatasetDataHelper {
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_ref(&dataset_alias.as_local_ref())
            .await
            .unwrap();

        DatasetDataHelper::new_with_context((*resolved_dataset).clone(), self.ctx.clone())
    }
}
