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

use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::*;
use dill::Component;
use event_bus::EventBus;
use indoc::indoc;
use kamu::testing::MetadataFactory;
use kamu::{DatasetRepositoryLocalFs, DependencyGraphServiceInMemory};
use kamu_core::*;
use kamu_data_utils::testing::{assert_data_eq, assert_schema_eq};
use kamu_ingest_datafusion::*;
use odf::{AsTypedBlock, DatasetAlias};
use opendatafabric as odf;

///////////////////////////////////////////////////////////////
// TODO: This test belongs in kamu-ingest-datafusion crate.
// We currently cannot move it there as it needs DatasetRepositoryLocalFs to
// function. We should move it there once we further decompose the kamu core
// crate.
///////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_happy_path() {
    let mut harness = Harness::new(vec![MetadataFactory::set_polling_source()
        .merge(odf::MergeStrategySnapshot {
            primary_key: vec!["city".to_string()],
            compare_columns: None,
        })
        .build()
        .into()])
    .await;

    // Round 1
    let res = harness
        .write(
            indoc!(
                r#"
                city,population
                A,1000
                B,2000
                C,3000
                "#
            ),
            "city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
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
    );

    assert_data_eq(
        df.clone(),
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 0      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | A    | 1000       |
            | 1      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | B    | 2000       |
            | 2      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | C    | 3000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        res.new_block.event.new_watermark.as_ref(),
        Some(&harness.source_event_time)
    );

    // Compare schemas in block and in data
    let (schema_block_hash, schema_block) = harness.get_last_schema_block().await;
    let schema_in_block = schema_block.event.schema_as_arrow().unwrap();
    let schema_in_data = SchemaRef::new(df.schema().into());
    assert_eq!(schema_in_block, schema_in_data);

    // Round 2
    harness.set_system_time(Utc.with_ymd_and_hms(2010, 1, 2, 12, 0, 0).unwrap());
    harness.set_source_event_time(Utc.with_ymd_and_hms(2000, 1, 2, 12, 0, 0).unwrap());

    let res = harness
        .write(
            indoc!(
                r#"
                city,population
                A,1000
                B,2000
                C,3000
                D,4000
                "#
            ),
            "city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
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
    );

    assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 3      | 0  | 2010-01-02T12:00:00Z | 2000-01-02T12:00:00Z | D    | 4000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        res.new_block.event.new_watermark.as_ref(),
        Some(&harness.source_event_time)
    );

    // Check schema block was reused
    assert_eq!(schema_block_hash, harness.get_last_schema_block().await.0);

    // Round 3 (nothing to commit)
    let prev_watermark = res.new_block.event.new_watermark.unwrap();
    harness.set_system_time(Utc.with_ymd_and_hms(2010, 1, 3, 12, 0, 0).unwrap());
    harness.set_source_event_time(Utc.with_ymd_and_hms(2000, 1, 3, 12, 0, 0).unwrap());

    let res = harness
        .write(
            indoc!(
                r#"
                city,population
                A,1000
                B,2000
                C,3000
                D,4000
                "#
            ),
            "city STRING, population BIGINT",
        )
        .await;

    assert_matches!(res, Err(WriteDataError::EmptyCommit(_)));

    // Round 4 (nothing but source state changed)
    let source_state = odf::SourceState {
        source_name: odf::SourceState::DEFAULT_SOURCE_NAME.to_string(),
        kind: "odf/etag".to_string(),
        value: "123".to_string(),
    };

    harness.set_system_time(Utc.with_ymd_and_hms(2010, 1, 4, 12, 0, 0).unwrap());
    harness.set_source_event_time(Utc.with_ymd_and_hms(2000, 1, 4, 12, 0, 0).unwrap());

    let res = harness
        .write_opts(
            indoc!(
                r#"
                city,population
                A,1000
                B,2000
                C,3000
                D,4000
                "#
            ),
            "city STRING, population BIGINT",
            Some(source_state.clone()),
        )
        .await
        .unwrap();

    assert_eq!(res.new_block.event.new_data, None);
    // Watermark is carried
    assert_eq!(res.new_block.event.new_watermark, Some(prev_watermark));
    // Source state updated
    assert_eq!(res.new_block.event.new_source_state, Some(source_state));
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_rejects_incompatible_schema() {
    let mut harness = Harness::new(vec![]).await;

    // Round 1
    harness
        .write(
            indoc!(
                r#"
                city,population
                A,1000
                B,2000
                C,3000
                "#
            ),
            "city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
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
    );

    assert_data_eq(
        df.clone(),
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 0      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | A    | 1000       |
            | 1      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | B    | 2000       |
            | 2      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | C    | 3000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;

    // Round 2 (still ok)
    harness.set_system_time(Utc.with_ymd_and_hms(2010, 1, 2, 12, 0, 0).unwrap());
    harness.set_source_event_time(Utc.with_ymd_and_hms(2000, 1, 2, 12, 0, 0).unwrap());

    harness
        .write(
            indoc!(
                r#"
                city,population
                D,4000
                "#
            ),
            "city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
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
    );

    assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 3      | 0  | 2010-01-02T12:00:00Z | 2000-01-02T12:00:00Z | D    | 4000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;

    // Round 3 (not ok - schema changed)
    harness.set_system_time(Utc.with_ymd_and_hms(2010, 1, 3, 12, 0, 0).unwrap());
    harness.set_source_event_time(Utc.with_ymd_and_hms(2000, 1, 3, 12, 0, 0).unwrap());

    let res = harness
        .write(
            indoc!(
                r#"
                city,state,population
                E,X,5000
                "#
            ),
            "city STRING, state STRING, population BIGINT",
        )
        .await;

    assert_matches!(res, Err(WriteDataError::IncompatibleSchema(_)));

    // Round 4 (still not ok after writer reset)
    harness.reset_writer().await;

    let res = harness
        .write(
            indoc!(
                r#"
                city,state,population
                E,X,5000
                "#
            ),
            "city STRING, state STRING, population BIGINT",
        )
        .await;

    assert_matches!(res, Err(WriteDataError::IncompatibleSchema(_)));

    // Round 5 (back to normal)
    harness
        .write(
            indoc!(
                r#"
                city,population
                E,5000
                "#
            ),
            "city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
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
    );

    assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 4      | 0  | 2010-01-03T12:00:00Z | 2000-01-03T12:00:00Z | E    | 5000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_ledger_orders_by_event_time() {
    let mut harness = Harness::new(vec![MetadataFactory::set_polling_source()
        .merge(odf::MergeStrategyAppend {})
        .build()
        .into()])
    .await;

    let res = harness
        .write(
            indoc!(
                r#"
                event_time,city,population
                2021-01-01,A,1000
                2023-01-01,B,2000
                2022-01-01,C,3000
                "#
            ),
            "event_time DATE, city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              OPTIONAL INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT32 event_time (DATE);
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT64 population;
            }
            "#
        ),
    );

    assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----+----------------------+------------+------+------------+
            | offset | op | system_time          | event_time | city | population |
            +--------+----+----------------------+------------+------+------------+
            | 0      | 0  | 2010-01-01T12:00:00Z | 2021-01-01 | A    | 1000       |
            | 1      | 0  | 2010-01-01T12:00:00Z | 2022-01-01 | C    | 3000       |
            | 2      | 0  | 2010-01-01T12:00:00Z | 2023-01-01 | B    | 2000       |
            +--------+----+----------------------+------------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        res.new_block
            .event
            .new_watermark
            .as_ref()
            .map(DateTime::to_rfc3339),
        Some("2023-01-01T00:00:00+00:00".to_string())
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_snapshot_orders_by_pk_and_operation_type() {
    let mut harness = Harness::new(vec![MetadataFactory::set_polling_source()
        .merge(odf::MergeStrategySnapshot {
            primary_key: vec!["city".to_string()],
            compare_columns: None,
        })
        .build()
        .into()])
    .await;

    let res = harness
        .write(
            indoc!(
                r#"
                city,population
                C,3000
                A,1000
                D,4000
                B,2000
                "#
            ),
            "city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
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
    );

    assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 0      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | A    | 1000       |
            | 1      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | B    | 2000       |
            | 2      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | C    | 3000       |
            | 3      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | D    | 4000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        res.new_block
            .event
            .new_watermark
            .as_ref()
            .map(DateTime::to_rfc3339),
        Some("2000-01-01T12:00:00+00:00".to_string())
    );

    // Round 2
    harness.set_system_time(Utc.with_ymd_and_hms(2010, 1, 2, 12, 0, 0).unwrap());
    harness.set_source_event_time(Utc.with_ymd_and_hms(2000, 1, 2, 12, 0, 0).unwrap());

    let res = harness
        .write(
            indoc!(
                r#"
                city,population
                C,3000
                B,4000
                D,5000
                "#
            ),
            "city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
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
    );

    assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 4      | 1  | 2010-01-02T12:00:00Z | 2000-01-01T12:00:00Z | A    | 1000       |
            | 5      | 2  | 2010-01-02T12:00:00Z | 2000-01-01T12:00:00Z | B    | 2000       |
            | 6      | 3  | 2010-01-02T12:00:00Z | 2000-01-02T12:00:00Z | B    | 4000       |
            | 7      | 2  | 2010-01-02T12:00:00Z | 2000-01-01T12:00:00Z | D    | 4000       |
            | 8      | 3  | 2010-01-02T12:00:00Z | 2000-01-02T12:00:00Z | D    | 5000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;

    assert_eq!(
        res.new_block
            .event
            .new_watermark
            .as_ref()
            .map(DateTime::to_rfc3339),
        Some("2000-01-02T12:00:00+00:00".to_string())
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_normalizes_timestamps_to_utc_millis() {
    let mut harness = Harness::new(vec![MetadataFactory::set_polling_source()
        .merge(odf::MergeStrategyLedger {
            primary_key: vec!["event_time".to_string(), "city".to_string()],
        })
        .build()
        .into()])
    .await;

    harness
        .write(
            indoc!(
                r#"
                event_time,city,population
                2000-01-01,A,1000
                2000-01-01,B,2000
                2000-01-01,C,3000
                "#
            ),
            "event_time TIMESTAMP, city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
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
    );

    assert_data_eq(
        df,
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 0      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T00:00:00Z | A    | 1000       |
            | 1      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T00:00:00Z | B    | 2000       |
            | 2      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T00:00:00Z | C    | 3000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_optimal_parquet_encoding() {
    use ::datafusion::parquet::basic::{Compression, Encoding, PageType};
    use ::datafusion::parquet::file::reader::FileReader;

    let mut harness = Harness::new(vec![MetadataFactory::set_polling_source()
        .merge(odf::MergeStrategyLedger {
            primary_key: vec!["event_time".to_string(), "city".to_string()],
        })
        .build()
        .into()])
    .await;

    harness
        .write(
            indoc!(
                r#"
                event_time,city,population
                2020-01-01,A,1000
                2020-01-01,B,2000
                2020-01-01,C,3000
                "#
            ),
            "event_time TIMESTAMP, city STRING, population BIGINT",
        )
        .await
        .unwrap();

    let parquet = kamu::testing::ParquetReaderHelper::open(&harness.get_last_data_file().await);
    let meta = parquet.reader.metadata();

    // TODO: Migrate to Parquet v2 and DATA_PAGE_V2
    let assert_data_encoding = |col, enc| {
        let data_page = parquet
            .reader
            .get_row_group(0)
            .unwrap()
            .get_column_page_reader(col)
            .unwrap()
            .map(Result::unwrap)
            .find(|p| p.page_type() == PageType::DATA_PAGE)
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

    let operation_type_col = meta.row_group(0).column(1);
    assert_eq!(operation_type_col.column_path().string(), "op");
    assert_eq!(operation_type_col.compression(), Compression::SNAPPY);
    assert_data_encoding(1, Encoding::RLE_DICTIONARY);

    let system_time_col = meta.row_group(0).column(2);
    assert_eq!(system_time_col.column_path().string(), "system_time");
    assert_eq!(system_time_col.compression(), Compression::SNAPPY);
    assert_data_encoding(2, Encoding::RLE_DICTIONARY);
}

/////////////////////////////////////////////////////////////////////////////////////////
// Builder
/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_data_writer_builder_scan_no_source() {
    let harness = Harness::new(vec![odf::SetVocab {
        event_time_column: Some("foo".to_string()),
        ..Default::default()
    }
    .into()])
    .await;

    let b = DataWriterDataFusion::builder(harness.dataset.clone(), harness.ctx.clone())
        .with_metadata_state_scanned(None)
        .await
        .unwrap();

    let head = harness
        .dataset
        .as_metadata_chain()
        .resolve_ref(&BlockRef::Head)
        .await
        .unwrap();

    assert_matches!(
        b.metadata_state().unwrap(),
        DataWriterMetadataState {
            head: h,
            schema: None,
            source_event: None,
            merge_strategy: odf::MergeStrategy::Append(_),
            vocab,
            prev_offset: None,
            prev_checkpoint: None,
            prev_watermark: None,
            prev_source_state: None,
            ..
        } if *h == head && vocab.event_time_column == "foo"

    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_data_writer_builder_scan_polling_source() {
    let harness = Harness::new(vec![MetadataFactory::set_polling_source()
        .merge(odf::MergeStrategyLedger {
            primary_key: vec!["event_time".to_string(), "city".to_string()],
        })
        .build()
        .into()])
    .await;

    let b = DataWriterDataFusion::builder(harness.dataset.clone(), harness.ctx.clone())
        .with_metadata_state_scanned(None)
        .await
        .unwrap();

    assert_matches!(
        b.metadata_state().unwrap(),
        DataWriterMetadataState {
            schema: None,
            source_event: Some(_),
            merge_strategy: odf::MergeStrategy::Ledger(_),
            vocab,
            prev_offset: None,
            prev_checkpoint: None,
            prev_watermark: None,
            prev_source_state: None,
            ..
        } if *vocab == odf::DatasetVocabulary::default()
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_data_writer_builder_scan_push_source() {
    let harness = Harness::new(vec![MetadataFactory::add_push_source()
        .read(odf::ReadStepNdJson {
            schema: Some(vec![
                "event_time".to_string(),
                "city".to_string(),
                "population".to_string(),
            ]),
            ..Default::default()
        })
        .merge(odf::MergeStrategyLedger {
            primary_key: vec!["event_time".to_string(), "city".to_string()],
        })
        .build()
        .into()])
    .await;

    let b = DataWriterDataFusion::builder(harness.dataset.clone(), harness.ctx.clone())
        .with_metadata_state_scanned(None)
        .await
        .unwrap();

    assert_matches!(
        b.metadata_state().unwrap(),
        DataWriterMetadataState {
            schema: None,
            source_event: Some(_),
            merge_strategy: odf::MergeStrategy::Ledger(_),
            vocab,
            prev_offset: None,
            prev_checkpoint: None,
            prev_watermark: None,
            prev_source_state: None,
            ..
        } if *vocab == odf::DatasetVocabulary::default()
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_data_writer_builder_scan_push_source_with_extra_events() {
    let harness = Harness::new(vec![
        MetadataFactory::add_push_source()
            .read(odf::ReadStepNdJson {
                schema: Some(vec![
                    "event_time".to_string(),
                    "city".to_string(),
                    "population".to_string(),
                ]),
                ..Default::default()
            })
            .merge(odf::MergeStrategyLedger {
                primary_key: vec!["event_time".to_string(), "city".to_string()],
            })
            .build()
            .into(),
        odf::SetLicense {
            name: "Open Government Licence - Canada".into(),
            short_name: "OGL-Canada-2.0".into(),
            spdx_id: Some("OGL-Canada-2.0".into()),
            website_url: "https://open.canada.ca/en/open-government-licence-canada".into(),
        }
        .into(),
    ])
    .await;

    let b = DataWriterDataFusion::builder(harness.dataset.clone(), harness.ctx.clone())
        .with_metadata_state_scanned(None)
        .await
        .unwrap();

    assert_matches!(
        b.metadata_state().unwrap(),
        DataWriterMetadataState {
            schema: None,
            source_event: Some(_),
            merge_strategy: odf::MergeStrategy::Ledger(_),
            vocab,
            prev_offset: None,
            prev_checkpoint: None,
            prev_watermark: None,
            prev_source_state: None,
            ..
        } if *vocab == odf::DatasetVocabulary::default()
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

struct Harness {
    temp_dir: tempfile::TempDir,
    dataset: Arc<dyn Dataset>,
    writer: DataWriterDataFusion,
    ctx: SessionContext,

    system_time: DateTime<Utc>,
    source_event_time: DateTime<Utc>,
}

impl Harness {
    async fn new(dataset_events: Vec<odf::MetadataEvent>) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let datasets_dir = temp_dir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let system_time = Utc.with_ymd_and_hms(2010, 1, 1, 12, 0, 0).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add::<kamu_core::auth::AlwaysHappyDatasetActionAuthorizer>()
            .add_value(CurrentAccountSubject::new_test())
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

        let dataset = dataset_repo
            .create_dataset(
                &DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo")),
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(odf::DatasetKind::Root).build(),
                )
                .system_time(system_time)
                .build_typed(),
            )
            .await
            .unwrap()
            .dataset;

        for event in dataset_events {
            dataset
                .commit_event(
                    event,
                    CommitOpts {
                        system_time: Some(system_time),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));

        let writer = DataWriterDataFusion::builder(dataset.clone(), ctx.clone())
            .with_metadata_state_scanned(None)
            .await
            .unwrap()
            .build();

        Self {
            temp_dir,
            dataset,
            writer,
            ctx,
            system_time,
            source_event_time: Utc.with_ymd_and_hms(2000, 1, 1, 12, 0, 0).unwrap(),
        }
    }

    fn set_system_time(&mut self, t: DateTime<Utc>) {
        self.system_time = t;
    }

    fn set_source_event_time(&mut self, t: DateTime<Utc>) {
        self.source_event_time = t;
    }

    async fn reset_writer(&mut self) {
        self.writer = DataWriterDataFusion::builder(self.dataset.clone(), self.ctx.clone())
            .with_metadata_state_scanned(None)
            .await
            .unwrap()
            .build();
    }

    async fn write_opts(
        &mut self,
        data: &str,
        schema: &str,
        new_source_state: Option<odf::SourceState>,
    ) -> Result<WriteDataResult, WriteDataError> {
        let df = if data.is_empty() {
            None
        } else {
            let data_path = self.temp_dir.path().join("data.bin");
            std::fs::write(&data_path, data).unwrap();

            let df = ReaderCsv::new(
                self.ctx.clone(),
                odf::ReadStepCsv {
                    header: Some(true),
                    schema: Some(schema.split(',').map(ToString::to_string).collect()),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .read(&data_path)
            .await
            .unwrap();

            Some(df)
        };

        self.writer
            .write(
                df,
                WriteDataOpts {
                    system_time: self.system_time,
                    source_event_time: self.source_event_time,
                    new_watermark: None,
                    new_source_state,
                    data_staging_path: self.temp_dir.path().join("write.tmp"),
                },
            )
            .await
    }

    async fn write(&mut self, data: &str, schema: &str) -> Result<WriteDataResult, WriteDataError> {
        self.write_opts(data, schema, None).await
    }

    async fn get_last_schema_block(
        &self,
    ) -> (odf::Multihash, odf::MetadataBlockTyped<odf::SetDataSchema>) {
        use futures::StreamExt;

        let (hash, block) = self
            .dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_ok(|(_, b)| b.as_typed::<odf::SetDataSchema>().is_some())
            .next()
            .await
            .unwrap()
            .unwrap();

        (hash, block.into_typed::<odf::SetDataSchema>().unwrap())
    }

    async fn get_last_data_block(&self) -> odf::MetadataBlockTyped<odf::AddData> {
        use futures::StreamExt;

        let (_, block) = self
            .dataset
            .as_metadata_chain()
            .iter_blocks()
            .next()
            .await
            .unwrap()
            .unwrap();
        block.into_typed::<odf::AddData>().unwrap()
    }

    async fn get_last_data_file(&self) -> PathBuf {
        let block = self.get_last_data_block().await;

        kamu_data_utils::data::local_url::into_local_path(
            self.dataset
                .as_data_repo()
                .get_internal_url(&block.event.new_data.unwrap().physical_hash)
                .await,
        )
        .unwrap()
    }

    async fn get_last_data(&self) -> DataFrame {
        let part_file = self.get_last_data_file().await;
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
