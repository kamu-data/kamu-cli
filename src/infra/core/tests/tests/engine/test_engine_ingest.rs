// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use container_runtime::ContainerRuntime;
use datafusion::parquet::record::RowAccessor;
use datafusion::prelude::*;
use futures::StreamExt;
use indoc::indoc;
use itertools::Itertools;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use opendatafabric::*;
use tempfile::TempDir;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, ingest, spark)]
#[test_log::test(tokio::test)]
async fn test_ingest_legacy_spark() {
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
                        "population INT".to_string(),
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

    let dataset_name = dataset_snapshot.name.clone();

    harness.create_dataset(dataset_snapshot).await;
    harness.ingest(&dataset_name).await.unwrap();

    let parquet_reader = harness.read_datafile(&dataset_name).await;

    assert_eq!(
        parquet_reader.get_column_names(),
        ["offset", "system_time", "event_time", "city", "population"]
    );

    assert_eq!(
        parquet_reader
            .get_row_iter()
            .map(|r| r.unwrap())
            .map(IngestTestHarness::row_mapper)
            .sorted()
            .collect::<Vec<_>>(),
        [
            (0, "A".to_owned(), 10000),
            (1, "B".to_owned(), 20000),
            (2, "C".to_owned(), 30000)
        ]
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_datafusion_snapshot() {
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
                    event_time: Some(EventTimeSource::FromSystemTime),
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
                    observation_column: None,
                    obsv_added: None,
                    obsv_changed: None,
                    obsv_removed: None,
                })
                .build(),
        )
        .build();

    let dataset_name = dataset_snapshot.name.clone();

    harness.create_dataset(dataset_snapshot).await;

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

    harness.ingest(&dataset_name).await.unwrap();

    let df = harness.get_last_data(&dataset_name).await;
    kamu_data_utils::testing::assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
              REQUIRED BYTE_ARRAY observed (STRING);
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT64 population;
            }
            "#
        ),
    );

    kamu_data_utils::testing::assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----------------------+----------------------+----------+------+------------+
            | offset | system_time          | event_time           | observed | city | population |
            +--------+----------------------+----------------------+----------+------+------------+
            | 0      | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | I        | A    | 10001      |
            | 1      | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | I        | B    | 20001      |
            | 2      | 2050-01-01T12:00:00Z | 2050-01-01T12:00:00Z | I        | C    | 30001      |
            +--------+----------------------+----------------------+----------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        harness
            .get_last_data_block(&dataset_name)
            .await
            .event
            .output_watermark
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

    harness.ingest(&dataset_name).await.unwrap();

    let df = harness.get_last_data(&dataset_name).await;
    kamu_data_utils::testing::assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----------------------+----------------------+----------+------+------------+
            | offset | system_time          | event_time           | observed | city | population |
            +--------+----------------------+----------------------+----------+------+------------+
            | 3      | 2050-02-01T12:00:00Z | 2050-02-01T12:00:00Z | U        | C    | 40001      |
            +--------+----------------------+----------------------+----------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        harness
            .get_last_data_block(&dataset_name)
            .await
            .event
            .output_watermark
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

    harness.ingest(&dataset_name).await.unwrap();
    let event = harness.get_last_data_block(&dataset_name).await.event;

    assert_eq!(event.output_data, None);
    assert_eq!(
        event.output_watermark.map(|dt| dt.to_rfc3339()),
        Some("2050-02-01T12:00:00+00:00".to_string())
    );
    assert!(event.source_state.is_some());
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_datafusion_ledger() {
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
                    event_time: Some(EventTimeSource::FromSystemTime),
                    cache: None,
                    headers: None,
                }))
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        ["date TIMESTAMP", "city STRING", "population BIGINT"]
                            .iter()
                            .map(|s| s.to_string())
                            .collect(),
                    ),
                    ..ReadStepCsv::default()
                }))
                .preprocess(TransformSql {
                    engine: "datafusion".to_string(),
                    version: None,
                    query: Some("select * from input".to_string()),
                    queries: None,
                    temporal_tables: None,
                })
                .merge(MergeStrategyLedger {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                })
                .build(),
        )
        .push_event(SetVocab {
            system_time_column: None,
            event_time_column: Some("date".to_string()),
            offset_column: None,
        })
        .build();

    let dataset_name = dataset_snapshot.name.clone();

    harness.create_dataset(dataset_snapshot).await;

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

    harness.ingest(&dataset_name).await.unwrap();

    let df = harness.get_last_data(&dataset_name).await;
    kamu_data_utils::testing::assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 date (TIMESTAMP(MILLIS,true));
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT64 population;
            }
            "#
        ),
    );

    kamu_data_utils::testing::assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----------------------+----------------------+------+------------+
            | offset | system_time          | date                 | city | population |
            +--------+----------------------+----------------------+------+------------+
            | 0      | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | A    | 1000       |
            | 1      | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | B    | 2000       |
            | 2      | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | C    | 3000       |
            +--------+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        harness
            .get_last_data_block(&dataset_name)
            .await
            .event
            .output_watermark
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

    harness.ingest(&dataset_name).await.unwrap();

    let df = harness.get_last_data(&dataset_name).await;
    kamu_data_utils::testing::assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----------------------+----------------------+------+------------+
            | offset | system_time          | date                 | city | population |
            +--------+----------------------+----------------------+------+------------+
            | 3      | 2050-01-01T12:00:00Z | 2021-01-01T00:00:00Z | C    | 4000       |
            +--------+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        harness
            .get_last_data_block(&dataset_name)
            .await
            .event
            .output_watermark
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

    harness.ingest(&dataset_name).await.unwrap();

    let df = harness.get_last_data(&dataset_name).await;
    kamu_data_utils::testing::assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----------------------+----------------------+------+------------+
            | offset | system_time          | date                 | city | population |
            +--------+----------------------+----------------------+------+------------+
            | 4      | 2050-01-01T12:00:00Z | 2020-01-01T00:00:00Z | D    | 4000       |
            +--------+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        harness
            .get_last_data_block(&dataset_name)
            .await
            .event
            .output_watermark
            .map(|dt| dt.to_rfc3339()),
        Some("2021-01-01T00:00:00+00:00".to_string())
    );

    // Round 4 (no-op, only updates source state)
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

    harness.ingest(&dataset_name).await.unwrap();
    let event = harness.get_last_data_block(&dataset_name).await.event;

    assert_eq!(event.output_data, None);
    assert_eq!(
        event.output_watermark.map(|dt| dt.to_rfc3339()),
        Some("2021-01-01T00:00:00+00:00".to_string())
    );
    assert!(event.source_state.is_some());
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_datafusion_event_time_as_date() {
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
                    event_time: Some(EventTimeSource::FromSystemTime),
                    cache: None,
                    headers: None,
                }))
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(
                        ["date DATE", "city STRING", "population BIGINT"]
                            .iter()
                            .map(|s| s.to_string())
                            .collect(),
                    ),
                    ..ReadStepCsv::default()
                }))
                .preprocess(TransformSql {
                    engine: "datafusion".to_string(),
                    version: None,
                    query: Some("select * from input".to_string()),
                    queries: None,
                    temporal_tables: None,
                })
                .merge(MergeStrategyLedger {
                    primary_key: vec!["date".to_string(), "city".to_string()],
                })
                .build(),
        )
        .push_event(SetVocab {
            system_time_column: None,
            event_time_column: Some("date".to_string()),
            offset_column: None,
        })
        .build();

    let dataset_name = dataset_snapshot.name.clone();

    harness.create_dataset(dataset_snapshot).await;

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

    harness.ingest(&dataset_name).await.unwrap();

    let df = harness.get_last_data(&dataset_name).await;
    kamu_data_utils::testing::assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT32 date (DATE);
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT64 population;
            }
            "#
        ),
    );

    kamu_data_utils::testing::assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----------------------+------------+------+------------+
            | offset | system_time          | date       | city | population |
            +--------+----------------------+------------+------+------------+
            | 0      | 2050-01-01T12:00:00Z | 2020-01-01 | A    | 1000       |
            | 1      | 2050-01-01T12:00:00Z | 2020-01-01 | B    | 2000       |
            | 2      | 2050-01-01T12:00:00Z | 2020-01-01 | C    | 3000       |
            +--------+----------------------+------------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        harness
            .get_last_data_block(&dataset_name)
            .await
            .event
            .output_watermark
            .map(|dt| dt.to_rfc3339()),
        Some("2020-01-01T00:00:00+00:00".to_string())
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_datafusion_event_time_of_invalid_type() {
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
                    event_time: Some(EventTimeSource::FromSystemTime),
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
                .preprocess(TransformSql {
                    engine: "datafusion".to_string(),
                    version: None,
                    query: Some("select * from input".to_string()),
                    queries: None,
                    temporal_tables: None,
                })
                .build(),
        )
        .push_event(SetVocab {
            system_time_column: None,
            event_time_column: Some("date".to_string()),
            offset_column: None,
        })
        .build();

    let dataset_name = dataset_snapshot.name.clone();

    harness.create_dataset(dataset_snapshot).await;

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

    let res = harness.ingest(&dataset_name).await;
    assert_matches!(
        res,
        Err(IngestError::EngineError(EngineError::InvalidQuery(_)))
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_datafusion_optimal_parquet_encoding() {
    use ::datafusion::parquet::basic::{Compression, Encoding, PageType};
    use ::datafusion::parquet::file::reader::FileReader;

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
                    event_time: Some(EventTimeSource::FromSystemTime),
                    cache: None,
                    headers: None,
                }))
                .read(ReadStep::Csv(ReadStepCsv {
                    header: Some(true),
                    schema: Some(vec![
                        "date TIMESTAMP".to_string(),
                        "city STRING".to_string(),
                        "population BIGINT".to_string(),
                    ]),
                    ..ReadStepCsv::default()
                }))
                .preprocess(TransformSql {
                    engine: "datafusion".to_string(),
                    version: None,
                    query: Some("select * from input".to_string()),
                    queries: None,
                    temporal_tables: None,
                })
                .build(),
        )
        .push_event(SetVocab {
            system_time_column: None,
            event_time_column: Some("date".to_string()),
            offset_column: None,
        })
        .build();

    let dataset_name = dataset_snapshot.name.clone();

    harness.create_dataset(dataset_snapshot).await;

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

    harness.ingest(&dataset_name).await.unwrap();
    let parquet = harness.read_datafile(&dataset_name).await;
    let meta = parquet.reader.metadata();

    // TODO: Migrate to Parquet v2 and DATA_PAGE_V2
    let assert_data_encoding = |col, enc| {
        let data_page = parquet
            .reader
            .get_row_group(0)
            .unwrap()
            .get_column_page_reader(col)
            .unwrap()
            .map(|p| p.unwrap())
            .filter(|p| p.page_type() == PageType::DATA_PAGE)
            .next()
            .unwrap();

        assert_eq!(data_page.encoding(), enc);
    };

    assert_eq!(meta.num_row_groups(), 1);

    let offset_col = meta.row_group(0).column(0);
    assert_eq!(offset_col.column_path().string(), "offset");
    assert_eq!(offset_col.compression(), Compression::SNAPPY);

    // TODO: Validate the encoding
    // See: https://github.com/kamu-data/kamu-engine-flink/issues/3
    // assert_data_encoding(0, Encoding::DELTA_BINARY_PACKED);

    let system_time_col = meta.row_group(0).column(1);
    assert_eq!(system_time_col.column_path().string(), "system_time");
    assert_eq!(system_time_col.compression(), Compression::SNAPPY);
    assert_data_encoding(1, Encoding::RLE_DICTIONARY);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_datafusion_bad_column_names_preserve() {
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
                    event_time: Some(EventTimeSource::FromSystemTime),
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
                .preprocess(TransformSql {
                    engine: "datafusion".to_string(),
                    version: None,
                    query: Some("select * from input".to_string()),
                    queries: None,
                    temporal_tables: None,
                })
                .build(),
        )
        .push_event(SetVocab {
            system_time_column: None,
            event_time_column: Some("Date (UTC)".to_string()),
            offset_column: None,
        })
        .build();

    let dataset_name = dataset_snapshot.name.clone();

    harness.create_dataset(dataset_snapshot).await;

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

    harness.ingest(&dataset_name).await.unwrap();

    let df = harness.get_last_data(&dataset_name).await;
    kamu_data_utils::testing::assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT32 Date (UTC) (DATE);
              REQUIRED BYTE_ARRAY City Name (STRING);
              REQUIRED INT64 Population;
            }
            "#
        ),
    );

    kamu_data_utils::testing::assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----------------------+------------+-----------+------------+
            | offset | system_time          | Date (UTC) | City Name | Population |
            +--------+----------------------+------------+-----------+------------+
            | 0      | 2050-01-01T12:00:00Z | 2020-01-01 | A         | 1000       |
            | 1      | 2050-01-01T12:00:00Z | 2020-01-01 | B         | 2000       |
            | 2      | 2050-01-01T12:00:00Z | 2020-01-01 | C         | 3000       |
            +--------+----------------------+------------+-----------+------------+
            "#
        ),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ingest_datafusion_bad_column_names_rename() {
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
                    event_time: Some(EventTimeSource::FromSystemTime),
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

    let dataset_name = dataset_snapshot.name.clone();

    harness.create_dataset(dataset_snapshot).await;

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

    harness.ingest(&dataset_name).await.unwrap();

    let df = harness.get_last_data(&dataset_name).await;
    kamu_data_utils::testing::assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT64 population;
            }
            "#
        ),
    );

    kamu_data_utils::testing::assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----------------------+----------------------+------+------------+
            | offset | system_time          | event_time           | city | population |
            +--------+----------------------+----------------------+------+------------+
            | 0      | 2050-01-01T12:00:00Z | 2020-01-01T12:00:00Z | A    | 1000       |
            | 1      | 2050-01-01T12:00:00Z | 2020-01-01T12:00:00Z | B    | 2000       |
            | 2      | 2050-01-01T12:00:00Z | 2020-01-01T12:00:00Z | C    | 3000       |
            +--------+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

struct IngestTestHarness {
    temp_dir: TempDir,
    dataset_repo: Arc<DatasetRepositoryLocalFs>,
    ingest_svc: Arc<IngestServiceImpl>,
    time_source: Arc<SystemTimeSourceMock>,
    ctx: SessionContext,
}

impl IngestTestHarness {
    fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = temp_dir.path().join("run");
        let cache_dir = temp_dir.path().join("cache");
        std::fs::create_dir(&run_info_dir).unwrap();
        std::fs::create_dir(&cache_dir).unwrap();

        let dataset_action_authorizer =
            Arc::new(kamu_core::auth::AlwaysHappyDatasetActionAuthorizer::new());

        let dataset_repo = Arc::new(
            DatasetRepositoryLocalFs::create(
                temp_dir.path().join("datasets"),
                Arc::new(CurrentAccountSubject::new_test()),
                dataset_action_authorizer.clone(),
                false,
            )
            .unwrap(),
        );

        let engine_provisioner = Arc::new(EngineProvisionerLocal::new(
            EngineProvisionerLocalConfig::default(),
            ContainerRuntime::default(),
            dataset_repo.clone(),
            run_info_dir.clone(),
        ));

        let time_source = Arc::new(SystemTimeSourceMock::new(
            Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
        ));

        let ingest_svc = Arc::new(IngestServiceImpl::new(
            dataset_repo.clone(),
            dataset_action_authorizer,
            engine_provisioner,
            Arc::new(ObjectStoreRegistryImpl::new(vec![Arc::new(
                ObjectStoreBuilderLocalFs::new(),
            )])),
            Arc::new(ContainerRuntime::default()),
            run_info_dir,
            cache_dir,
            time_source.clone(),
        ));

        Self {
            temp_dir,
            dataset_repo,
            ingest_svc,
            time_source,
            ctx: SessionContext::with_config(SessionConfig::new().with_target_partitions(1)),
        }
    }

    async fn create_dataset(&self, dataset_snapshot: DatasetSnapshot) {
        self.dataset_repo
            .create_dataset_from_snapshot(None, dataset_snapshot)
            .await
            .unwrap();
    }

    async fn ingest(&self, dataset_name: &DatasetName) -> Result<IngestResult, IngestError> {
        self.ingest_svc
            .ingest(
                &DatasetAlias::new(None, dataset_name.clone()).as_local_ref(),
                IngestOptions::default(),
                None,
            )
            .await
    }

    async fn get_last_data_block(&self, dataset_name: &DatasetName) -> MetadataBlockTyped<AddData> {
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_name.as_local_ref())
            .await
            .unwrap();

        let mut stream = dataset.as_metadata_chain().iter_blocks();
        while let Some(v) = stream.next().await {
            let (_, b) = v.unwrap();
            if let Some(b) = b.into_typed::<AddData>() {
                return b;
            }
        }

        unreachable!()
    }

    async fn get_last_data_file(&self, dataset_name: &DatasetName) -> PathBuf {
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_name.as_local_ref())
            .await
            .unwrap();

        let block = self.get_last_data_block(dataset_name).await;

        kamu_data_utils::data::local_url::into_local_path(
            dataset
                .as_data_repo()
                .get_internal_url(&block.event.output_data.unwrap().physical_hash)
                .await,
        )
        .unwrap()
    }

    async fn read_datafile(&self, dataset_name: &DatasetName) -> ParquetReaderHelper {
        let part_file = self.get_last_data_file(dataset_name).await;
        ParquetReaderHelper::open(&part_file)
    }

    /// Deprecated: use [kamu_data_utils::testing::assert_data_eq]
    fn row_mapper(r: datafusion::parquet::record::Row) -> (i64, String, i32) {
        (
            r.get_long(0).unwrap().clone(),
            r.get_string(3).unwrap().clone(),
            r.get_int(4).unwrap(),
        )
    }

    async fn get_last_data(&self, dataset_name: &DatasetName) -> DataFrame {
        let part_file = self.get_last_data_file(dataset_name).await;
        self.ctx
            .read_parquet(
                part_file.to_string_lossy().as_ref(),
                ParquetReadOptions {
                    file_extension: part_file
                        .extension()
                        .and_then(|s| s.to_str())
                        .unwrap_or_default(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
    }
}
