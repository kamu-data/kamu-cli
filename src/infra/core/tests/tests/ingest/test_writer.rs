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

use chrono::{DateTime, TimeZone, Utc};
use datafusion::prelude::*;
use indoc::indoc;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use kamu_datasets::*;
use kamu_ingest_datafusion::*;
use odf::metadata::testing::MetadataFactory;
use odf::metadata::{DataField, DataSchema};
use odf::utils::data::DataFrameExt;
use odf::utils::testing::{
    assert_arrow_schema_eq,
    assert_data_eq,
    assert_odf_schema_eq,
    assert_schema_eq,
};
use serde_json::json;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TODO: This test belongs in kamu-ingest-datafusion crate.
// We currently cannot move it there as it needs DatasetRepositoryLocalFs to
// function. We should move it there once we further decompose the kamu core
// crate.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_happy_path() {
    let mut harness = Harness::new(vec![
        MetadataFactory::set_polling_source()
            .merge(odf::metadata::MergeStrategySnapshot {
                primary_key: vec!["city".to_string()],
                compare_columns: None,
            })
            .build()
            .into(),
    ])
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
            "city STRING NOT NULL, population BIGINT NOT NULL",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    // Check schema of the data
    assert_arrow_schema_eq(
        df.schema().as_arrow(),
        json!({
            "fields": [{
                "name": "offset",
                "data_type": "Int64",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "op",
                "data_type": "Int32",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "system_time",
                "data_type": {
                    "Timestamp": [
                        "Millisecond",
                        "UTC",
                    ],
                },
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "event_time",
                "data_type": {
                    "Timestamp": [
                        "Millisecond",
                        "UTC",
                    ],
                },
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "city",
                // Note that Datafusion 49+ defaults to View for all text types
                "data_type": "Utf8View",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "population",
                "data_type": "Int64",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }],
            "metadata": {},
        }),
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
        res.add_data_block.unwrap().event.new_watermark.as_ref(),
        Some(&harness.source_event_time)
    );

    // Check schema in block SetDataSchema block
    let (schema_block_hash, schema_block) = harness.get_last_schema_block().await;

    assert_odf_schema_eq(
        schema_block.event.schema.as_ref().unwrap(),
        &DataSchema::new(vec![
            DataField::i64("offset"),
            DataField::i32("op"),
            DataField::timestamp_millis_utc("system_time"),
            DataField::timestamp_millis_utc("event_time"),
            // NOTE: In SetDataSchema we strip the encoding information, leaving only logical types
            DataField::string("city"),
            DataField::i64("population"),
        ]),
    );

    // Check the converted Arrow schema
    assert_arrow_schema_eq(
        &schema_block
            .event
            .schema_as_arrow(&odf::metadata::ToArrowSettings::default())
            .unwrap(),
        json!({
            "fields": [{
                "name": "offset",
                "data_type": "Int64",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "op",
                "data_type": "Int32",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "system_time",
                "data_type": {
                    "Timestamp": [
                        "Millisecond",
                        "UTC",
                    ],
                },
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "event_time",
                "data_type": {
                    "Timestamp": [
                        "Millisecond",
                        "UTC",
                    ],
                },
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "city",
                // NOTE: Not Utf8View due to stripped encoding
                "data_type": "Utf8",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }, {
                "name": "population",
                "data_type": "Int64",
                "dict_id": 0,
                "dict_is_ordered": false,
                "metadata": {},
                "nullable": false,
            }],
            "metadata": {},
        }),
    );

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
            "city STRING NOT NULL, population BIGINT NOT NULL",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
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
        res.add_data_block
            .as_ref()
            .unwrap()
            .event
            .new_watermark
            .as_ref(),
        Some(&harness.source_event_time)
    );

    // Check schema block was reused
    assert_eq!(schema_block_hash, harness.get_last_schema_block().await.0);

    // Round 3 (nothing to commit)
    let prev_watermark = res.add_data_block.unwrap().event.new_watermark.unwrap();
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
    let source_state = odf::metadata::SourceState {
        source_name: odf::metadata::SourceState::DEFAULT_SOURCE_NAME.to_string(),
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

    let new_block = res.add_data_block.unwrap();

    assert_eq!(new_block.event.new_data, None);
    // Watermark is carried
    assert_eq!(new_block.event.new_watermark, Some(prev_watermark));
    // Source state updated
    assert_eq!(new_block.event.new_source_state, Some(source_state));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
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
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
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
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_nullability_required_to_optional_incompatible() {
    let mut harness = Harness::new(vec![
        MetadataFactory::set_polling_source()
            .merge(odf::metadata::MergeStrategyLedger {
                primary_key: vec!["event_time".to_string(), "city".to_string()],
            })
            .build()
            .into(),
    ])
    .await;

    // Round 1
    harness
        .write(
            indoc!(
                r#"
                event_time,city,population
                2021-01-01,A,1000
                2021-01-01,B,2000
                2021-01-01,C,3000
                "#
            ),
            "event_time TIMESTAMP NOT NULL, city STRING NOT NULL, population BIGINT NOT NULL",
        )
        .await
        .unwrap();

    assert_odf_schema_eq(
        &harness.get_last_schema().await,
        &DataSchema::new(vec![
            DataField::i64("offset"),
            DataField::i32("op"),
            DataField::timestamp_millis_utc("system_time"),
            DataField::timestamp_millis_utc("event_time"),
            DataField::string("city"),
            DataField::i64("population"),
        ]),
    );

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
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
    );

    // Round 2 - input has optional column, but it gets coerced by reader
    harness
        .write(
            indoc!(
                r#"
                event_time,city,population
                2022-01-01,D,4000
                "#
            ),
            "event_time TIMESTAMP NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await
        .unwrap();

    // Round 3 - error during coercion
    let res = harness
        .write(
            indoc!(
                r#"
                event_time,city,population
                2022-01-01,E,
                "#
            ),
            "event_time TIMESTAMP NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    assert_matches!(
        res,
        Err(WriteDataError::ExecutionError(err))
        if err.to_string().contains("Column population contains 1 null values while none were expected")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_nullability_optional_to_required_coerces() {
    let mut harness = Harness::new(vec![
        MetadataFactory::set_polling_source()
            .merge(odf::metadata::MergeStrategySnapshot {
                primary_key: vec!["city".to_string()],
                compare_columns: None,
            })
            .build()
            .into(),
    ])
    .await;

    // Round 1
    harness
        .write(
            indoc!(
                r#"
                city,population
                A,1000
                B,2000
                C,
                "#
            ),
            "city STRING NOT NULL, population BIGINT",
        )
        .await
        .unwrap();

    assert_odf_schema_eq(
        &harness.get_last_schema().await,
        &DataSchema::new(vec![
            DataField::i64("offset"),
            DataField::i32("op"),
            DataField::timestamp_millis_utc("system_time"),
            DataField::timestamp_millis_utc("event_time"),
            DataField::string("city"),
            DataField::i64("population").optional(),
        ]),
    );

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
              REQUIRED BYTE_ARRAY city (STRING);
              OPTIONAL INT64 population;
            }
            "#
        ),
    );

    // Round 2 (coerces non-null column)
    harness.set_system_time(Utc.with_ymd_and_hms(2010, 1, 2, 12, 0, 0).unwrap());
    harness.set_source_event_time(Utc.with_ymd_and_hms(2000, 1, 2, 12, 0, 0).unwrap());
    harness
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
            "city STRING NOT NULL, population BIGINT NOT NULL",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_schema_eq(
        df.schema(),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
              REQUIRED BYTE_ARRAY city (STRING);
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
            | 3      | 2  | 2010-01-02T12:00:00Z | 2000-01-01T12:00:00Z | C    |            |
            | 4      | 3  | 2010-01-02T12:00:00Z | 2000-01-02T12:00:00Z | C    | 3000       |
            | 5      | 0  | 2010-01-02T12:00:00Z | 2000-01-02T12:00:00Z | D    | 4000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test]
fn test_data_writer_offsets_are_sequential_partitioned() {
    // Ensure our logic is resistant to partitioning
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));

    // Ensure we run with multiple threads
    // otherwise `target_partitions` doesn't matter
    let plan = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap()
        .block_on(test_data_writer_offsets_are_sequential_impl(ctx));

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Optimized physical plan:
            DataSinkExec: sink=ParquetSink(file_groups=[])
              SortPreservingMergeExec: [offset@0 ASC]
                ProjectionExec: expr=[CAST(CAST(row_number() PARTITION BY [Int32(1)] ORDER BY [event_time ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@5 AS Decimal128(20, 0)) + Some(-1),20,0 AS Int64) as offset, op@0 as op, system_time@4 as system_time, event_time@1 as event_time, city@2 as city, population@3 as population]
                  BoundedWindowAggExec: wdw=[row_number() PARTITION BY [Int32(1)] ORDER BY [event_time ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Field { name: "row_number() PARTITION BY [Int32(1)] ORDER BY [event_time ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
                    SortExec: expr=[event_time@1 ASC], preserve_partitioning=[true]
                      CoalesceBatchesExec: target_batch_size=8192
                        RepartitionExec: partitioning=Hash([1], 4), input_partitions=4
                          ProjectionExec: expr=[0 as op, coalesce(CAST(event_time@0 AS Timestamp(Millisecond, Some("UTC"))), 946728000000) as event_time, city@1 as city, population@2 as population, 1262347200000 as system_time]
                            DataSourceExec: file_groups={4 groups: [[tmp/data.ndjson:0..2991668], [tmp/data.ndjson:2991668..5983336], [tmp/data.ndjson:5983336..8975004], [tmp/data.ndjson:8975004..11966670]]}, projection=[event_time, city, population], file_type=json
            "#
        ).trim(),
        plan
    );
}

#[test_group::group(engine, ingest, datafusion)]
#[test]
fn test_data_writer_offsets_are_sequential_serialized() {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));

    // Ensure we run with multiple threads
    let plan = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap()
        .block_on(test_data_writer_offsets_are_sequential_impl(ctx));

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Optimized physical plan:
            DataSinkExec: sink=ParquetSink(file_groups=[])
              ProjectionExec: expr=[CAST(CAST(row_number() PARTITION BY [Int32(1)] ORDER BY [event_time ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@5 AS Decimal128(20, 0)) + Some(-1),20,0 AS Int64) as offset, op@0 as op, system_time@4 as system_time, event_time@1 as event_time, city@2 as city, population@3 as population]
                BoundedWindowAggExec: wdw=[row_number() PARTITION BY [Int32(1)] ORDER BY [event_time ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Field { name: "row_number() PARTITION BY [Int32(1)] ORDER BY [event_time ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
                  SortExec: expr=[event_time@1 ASC], preserve_partitioning=[false]
                    ProjectionExec: expr=[0 as op, coalesce(CAST(event_time@0 AS Timestamp(Millisecond, Some("UTC"))), 946728000000) as event_time, city@1 as city, population@2 as population, 1262347200000 as system_time]
                      DataSourceExec: file_groups={1 group: [[tmp/data.ndjson:0..11966670]]}, projection=[event_time, city, population], file_type=json
            "#
        ).trim(),
        plan
    );
}

async fn test_data_writer_offsets_are_sequential_impl(ctx: SessionContext) -> String {
    use std::io::Write;

    testing_logger::setup();

    let harness = Harness::new(vec![
        MetadataFactory::set_polling_source()
            .merge(odf::metadata::MergeStrategyLedger {
                primary_key: vec!["event_time".to_string(), "city".to_string()],
            })
            .build()
            .into(),
    ])
    .await;

    let mut writer = DataWriterDataFusion::from_metadata_chain(
        ctx.clone(),
        harness.dataset.clone(),
        &odf::BlockRef::Head,
        None,
    )
    .await
    .unwrap();

    let mut event_time = Utc.with_ymd_and_hms(2010, 1, 1, 0, 0, 0).unwrap();
    let data_path = harness.temp_dir.path().join("data.ndjson");
    let mut file = std::fs::File::create_new(&data_path).unwrap();

    // Generate a lot of data to make parquet split it into chunks
    for i in 0..50_000 {
        for city in ["A", "B", "C"] {
            writeln!(
                &mut file,
                "{{\"event_time\": \"{}\", \"city\": \"{}\", \"population\": \"{}\"}}",
                event_time.to_rfc3339(),
                city,
                i,
            )
            .unwrap();
        }
        event_time += chrono::Duration::minutes(1);
    }

    let df = ReaderNdJson::new(
        ctx.clone(),
        odf::metadata::ReadStepNdJson {
            schema: Some(vec![
                "event_time TIMESTAMP".to_string(),
                "city STRING".to_string(),
                "population BIGINT".to_string(),
            ]),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .read(&data_path)
    .await
    .unwrap();

    let write_result = writer
        .write(
            Some(df),
            WriteDataOpts {
                system_time: harness.system_time,
                source_event_time: harness.source_event_time,
                new_watermark: None,
                new_source_state: None,
                data_staging_path: harness.temp_dir.path().join("data.parquet"),
            },
        )
        .await
        .unwrap();

    harness
        .dataset
        .as_metadata_chain()
        .set_ref(
            &odf::BlockRef::Head,
            &write_result.new_head,
            odf::dataset::SetRefOpts {
                validate_block_present: true,
                check_ref_is: Some(Some(&write_result.old_head)),
            },
        )
        .await
        .unwrap();

    let data_path = harness.get_last_data_file().await;

    odf::utils::testing::assert_parquet_offsets_are_in_order(&data_path);

    let plan = std::sync::Mutex::new(String::new());
    testing_logger::validate(|capture| {
        let p = capture
            .iter()
            .find(|c| c.body.contains("Optimized physical plan:"))
            .unwrap()
            .body
            .trim()
            .replace(
                harness
                    .temp_dir
                    .path()
                    .display()
                    .to_string()
                    .trim_start_matches('/'),
                "tmp",
            );

        *plan.lock().unwrap() = p;
    });
    plan.into_inner().unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_ledger_orders_by_event_time() {
    let mut harness = Harness::new(vec![
        MetadataFactory::set_polling_source()
            .merge(odf::metadata::MergeStrategyLedger {
                primary_key: vec!["event_time".to_string(), "city".to_string()],
            })
            .build()
            .into(),
    ])
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
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT32 event_time (DATE);
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
        res.add_data_block
            .unwrap()
            .event
            .new_watermark
            .as_ref()
            .map(DateTime::to_rfc3339),
        Some("2023-01-01T00:00:00+00:00".to_string())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_snapshot_orders_by_pk_and_operation_type() {
    let mut harness = Harness::new(vec![
        MetadataFactory::set_polling_source()
            .merge(odf::metadata::MergeStrategySnapshot {
                primary_key: vec!["city".to_string()],
                compare_columns: None,
            })
            .build()
            .into(),
    ])
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
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
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
        res.add_data_block
            .unwrap()
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
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
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
        res.add_data_block
            .unwrap()
            .event
            .new_watermark
            .as_ref()
            .map(DateTime::to_rfc3339),
        Some("2000-01-02T12:00:00+00:00".to_string())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_normalizes_timestamps_to_utc_millis() {
    let mut harness = Harness::new(vec![
        MetadataFactory::set_polling_source()
            .merge(odf::metadata::MergeStrategyLedger {
                primary_key: vec!["event_time".to_string(), "city".to_string()],
            })
            .build()
            .into(),
    ])
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
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_optimal_parquet_encoding() {
    use ::datafusion::parquet::basic::{Compression, Encoding, PageType};
    use ::datafusion::parquet::file::reader::FileReader;

    let mut harness = Harness::new(vec![
        MetadataFactory::set_polling_source()
            .merge(odf::metadata::MergeStrategyLedger {
                primary_key: vec!["event_time".to_string(), "city".to_string()],
            })
            .build()
            .into(),
    ])
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Schema evolution
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_schema_evolution_from_inferred() {
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

    let schema = harness.get_last_schema().await;

    assert_odf_schema_eq(
        &schema,
        &odf::schema::DataSchema::new(vec![
            odf::schema::DataField::i64("offset"),
            odf::schema::DataField::i32("op"),
            odf::schema::DataField::timestamp_millis_utc("system_time"),
            odf::schema::DataField::timestamp_millis_utc("event_time"),
            odf::schema::DataField::string("city").optional(),
            odf::schema::DataField::i64("population").optional(),
        ]),
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

    // Set explicit schema with extra attributes
    harness
        .commit_event(odf::metadata::SetDataSchema::new(
            odf::schema::DataSchema::new(vec![
                odf::schema::DataField::i64("offset"),
                odf::schema::DataField::i32("op"),
                odf::schema::DataField::timestamp_millis_utc("system_time"),
                odf::schema::DataField::timestamp_millis_utc("event_time").extra(
                    odf::metadata::ext::AttrDescription::new(
                        "Date the census was done rounded to a year mark",
                    ),
                ),
                odf::schema::DataField::string("city")
                    .optional()
                    .extra(odf::metadata::ext::AttrDescription::new("Name of the city")),
                odf::schema::DataField::i64("population").optional().extra(
                    odf::metadata::ext::AttrDescription::new("Estimated population"),
                ),
            ]),
        ))
        .await
        .unwrap();

    // Round 2
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

    let schema = harness.get_last_schema().await;

    assert_odf_schema_eq(
        &schema,
        &odf::schema::DataSchema::new(vec![
            odf::schema::DataField::i64("offset"),
            odf::schema::DataField::i32("op"),
            odf::schema::DataField::timestamp_millis_utc("system_time"),
            odf::schema::DataField::timestamp_millis_utc("event_time").extra(
                odf::metadata::ext::AttrDescription::new(
                    "Date the census was done rounded to a year mark",
                ),
            ),
            odf::schema::DataField::string("city")
                .optional()
                .extra(odf::metadata::ext::AttrDescription::new("Name of the city")),
            odf::schema::DataField::i64("population").optional().extra(
                odf::metadata::ext::AttrDescription::new("Estimated population"),
            ),
        ]),
    );

    assert_data_eq(
        df.clone(),
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 3      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | D    | 4000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_schema_evolution_from_explicit() {
    let mut harness = Harness::new(vec![]).await;

    // Set explicit schema with extra attributes
    // NOTE: All fields are required, while actual Arrow schema will differ in
    // nullability
    harness
        .commit_event(odf::metadata::SetDataSchema::new(
            odf::schema::DataSchema::new(vec![
                odf::schema::DataField::i64("offset"),
                odf::schema::DataField::i32("op"),
                odf::schema::DataField::timestamp_millis_utc("system_time"),
                odf::schema::DataField::timestamp_millis_utc("event_time").extra(
                    odf::metadata::ext::AttrDescription::new(
                        "Date the census was done rounded to a year mark",
                    ),
                ),
                odf::schema::DataField::string("city")
                    .extra(odf::metadata::ext::AttrDescription::new("Name of the city")),
                odf::schema::DataField::i64("population").extra(
                    odf::metadata::ext::AttrDescription::new("Estimated population"),
                ),
            ]),
        ))
        .await
        .unwrap();

    // Round 1: Write conforming data
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

    // Ensure no new schema event written
    assert_eq!(harness.get_last_schema_block().await.1.sequence_number, 1);

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
#[expect(deprecated)]
async fn test_data_writer_schema_evolution_from_legacy() {
    use ::datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    let mut harness = Harness::new(vec![]).await;

    // Set explicit schema in legacy format
    harness
        .commit_event(odf::metadata::SetDataSchema::new_legacy_raw_arrow(
            &Schema::new(vec![
                Field::new("offset", DataType::Int64, false),
                Field::new("op", DataType::Int32, false),
                Field::new(
                    "system_time",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
                Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    true,
                ),
                Field::new("city", DataType::Utf8, true),
                Field::new("population", DataType::Int64, true),
            ]),
        ))
        .await
        .unwrap();

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

    let schema = harness.get_last_schema_block().await.1;

    // Ensure legacy schema is reused by writer without writing new event
    assert_eq!(schema.sequence_number, 1);
    assert!(schema.event.raw_arrow_schema.is_some());

    assert_odf_schema_eq(
        &schema.event.upgrade().schema,
        &odf::schema::DataSchema::new(vec![
            odf::schema::DataField::i64("offset"),
            odf::schema::DataField::i32("op"),
            odf::schema::DataField::timestamp_millis_utc("system_time"),
            odf::schema::DataField::timestamp_millis_utc("event_time").optional(),
            odf::schema::DataField::string("city").optional(),
            odf::schema::DataField::i64("population").optional(),
        ]),
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

    // Set explicit schema in new format
    // NOTE: The difference in optionality
    harness
        .commit_event(odf::metadata::SetDataSchema::new(
            odf::schema::DataSchema::new(vec![
                odf::schema::DataField::i64("offset"),
                odf::schema::DataField::i32("op"),
                odf::schema::DataField::timestamp_millis_utc("system_time"),
                odf::schema::DataField::timestamp_millis_utc("event_time"),
                odf::schema::DataField::string("city"),
                odf::schema::DataField::i64("population"),
            ]),
        ))
        .await
        .unwrap();

    // Round 2
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

    let schema = harness.get_last_schema().await;

    assert_odf_schema_eq(
        &schema,
        &odf::schema::DataSchema::new(vec![
            odf::schema::DataField::i64("offset"),
            odf::schema::DataField::i32("op"),
            odf::schema::DataField::timestamp_millis_utc("system_time"),
            odf::schema::DataField::timestamp_millis_utc("event_time"),
            odf::schema::DataField::string("city"),
            odf::schema::DataField::i64("population"),
        ]),
    );

    assert_data_eq(
        df.clone(),
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 3      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | D    | 4000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ObjectLink
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_writer_object_link() {
    let mut harness = Harness::new(vec![]).await;

    harness
        .commit_event(odf::metadata::SetDataSchema::new(
            odf::schema::DataSchema::new(vec![
                odf::schema::DataField::i64("offset"),
                odf::schema::DataField::i32("op"),
                odf::schema::DataField::timestamp_millis_utc("system_time"),
                odf::schema::DataField::timestamp_millis_utc("event_time"),
                odf::schema::DataField::string("city"),
                odf::schema::DataField::i64("population"),
                odf::schema::DataField::string("census_hash").type_ext(
                    odf::schema::ext::DataTypeExt::object_link(
                        odf::schema::ext::DataTypeExt::multihash(),
                    ),
                ),
            ]),
        ))
        .await
        .unwrap();

    // Round 1: Valid links
    let repo = harness.dataset.as_data_repo();

    repo.insert_bytes(b"census_a", Default::default())
        .await
        .unwrap();
    repo.insert_bytes(b"census_b", Default::default())
        .await
        .unwrap();
    repo.insert_bytes(b"census_c", Default::default())
        .await
        .unwrap();

    harness
        .write(
            &indoc::formatdoc!(
                r#"
                city,population,census_hash
                A,1000,{hash_a}
                B,2000,{hash_b}
                C,3000,{hash_c}
                "#,
                hash_a = odf::Multihash::from_digest_sha3_256(b"census_a"),
                hash_b = odf::Multihash::from_digest_sha3_256(b"census_b"),
                hash_c = odf::Multihash::from_digest_sha3_256(b"census_c"),
            ),
            "city STRING, population BIGINT, census_hash STRING",
        )
        .await
        .unwrap();

    let df = harness.get_last_data().await;

    assert_data_eq(
        df.clone(),
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+-----------------------------------------------------------------------+
            | offset | op | system_time          | event_time           | city | population | census_hash                                                           |
            +--------+----+----------------------+----------------------+------+------------+-----------------------------------------------------------------------+
            | 0      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | A    | 1000       | f1620ffd8538233905b7fb9a9c7bff1315bbb3f9eaecd1746c6562426328835785dcb |
            | 1      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | B    | 2000       | f162068c16896aced6b7498e338dc150640a07a7330c0f158bcb3365795c3f4418976 |
            | 2      | 0  | 2010-01-01T12:00:00Z | 2000-01-01T12:00:00Z | C    | 3000       | f16200846676d7d1ef6da1f516eca9e656d4c1a7058a5f6ba1559d7a2ef1c729303ed |
            +--------+----+----------------------+----------------------+------+------------+-----------------------------------------------------------------------+
            "#
        ),
    )
    .await;

    // Round 2: Dangling reference
    let res = harness
        .write(
            &indoc::formatdoc!(
                r#"
                city,population,census_hash
                D,4000,{hash_d}
                "#,
                hash_d = odf::Multihash::from_digest_sha3_256(b"census_d"),
            ),
            "city STRING, population BIGINT, census_hash STRING",
        )
        .await;

    assert_matches!(
        res,
        Err(WriteDataError::DataValidation(
            DataValidationError::DanglingReference(_)
        ))
    );

    // Round 3: Invalid value
    let res = harness
        .write(
            indoc!(
                r#"
                city,population,census_hash
                D,4000,not-a-multihash
                "#,
            ),
            "city STRING, population BIGINT, census_hash STRING",
        )
        .await;

    assert_matches!(
        res,
        Err(WriteDataError::DataValidation(
            DataValidationError::InvalidValue(_)
        ))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Builder
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_data_writer_builder_scan_no_source() {
    let harness = Harness::new(vec![
        odf::metadata::SetVocab {
            event_time_column: Some("foo".to_string()),
            ..Default::default()
        }
        .into(),
    ])
    .await;

    let metadata_state =
        DataWriterMetadataState::build(harness.dataset.clone(), &odf::BlockRef::Head, None, None)
            .await
            .unwrap();

    let head = harness
        .dataset
        .as_metadata_chain()
        .resolve_ref(&odf::BlockRef::Head)
        .await
        .unwrap();

    assert_matches!(
        metadata_state,
        DataWriterMetadataState {
            head: h,
            schema: None,
            source_event: None,
            merge_strategy: odf::metadata::MergeStrategy::Append(_),
            vocab,
            prev_offset: None,
            prev_checkpoint: None,
            prev_watermark: None,
            prev_source_state: None,
            ..
        } if h == head && vocab.event_time_column == "foo"

    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_data_writer_builder_scan_polling_source() {
    let harness = Harness::new(vec![
        MetadataFactory::set_polling_source()
            .merge(odf::metadata::MergeStrategyLedger {
                primary_key: vec!["event_time".to_string(), "city".to_string()],
            })
            .build()
            .into(),
    ])
    .await;

    let metadata_state =
        DataWriterMetadataState::build(harness.dataset.clone(), &odf::BlockRef::Head, None, None)
            .await
            .unwrap();

    assert_matches!(
        metadata_state,
        DataWriterMetadataState {
            schema: None,
            source_event: Some(_),
            merge_strategy: odf::metadata::MergeStrategy::Ledger(_),
            vocab,
            prev_offset: None,
            prev_checkpoint: None,
            prev_watermark: None,
            prev_source_state: None,
            ..
        } if vocab == odf::metadata::DatasetVocabulary::default()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_data_writer_builder_scan_push_source() {
    let harness = Harness::new(vec![
        MetadataFactory::add_push_source()
            .read(odf::metadata::ReadStepNdJson {
                schema: Some(vec![
                    "event_time".to_string(),
                    "city".to_string(),
                    "population".to_string(),
                ]),
                ..Default::default()
            })
            .merge(odf::metadata::MergeStrategyLedger {
                primary_key: vec!["event_time".to_string(), "city".to_string()],
            })
            .build()
            .into(),
    ])
    .await;

    let metadata_state =
        DataWriterMetadataState::build(harness.dataset.clone(), &odf::BlockRef::Head, None, None)
            .await
            .unwrap();

    assert_matches!(
        metadata_state,
        DataWriterMetadataState {
            schema: None,
            source_event: Some(_),
            merge_strategy: odf::metadata::MergeStrategy::Ledger(_),
            vocab,
            prev_offset: None,
            prev_checkpoint: None,
            prev_watermark: None,
            prev_source_state: None,
            ..
        } if vocab == odf::metadata::DatasetVocabulary::default()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_data_writer_builder_scan_push_source_with_extra_events() {
    let harness = Harness::new(vec![
        MetadataFactory::add_push_source()
            .read(odf::metadata::ReadStepNdJson {
                schema: Some(vec![
                    "event_time".to_string(),
                    "city".to_string(),
                    "population".to_string(),
                ]),
                ..Default::default()
            })
            .merge(odf::metadata::MergeStrategyLedger {
                primary_key: vec!["event_time".to_string(), "city".to_string()],
            })
            .build()
            .into(),
        odf::metadata::SetLicense {
            name: "Open Government Licence - Canada".into(),
            short_name: "OGL-Canada-2.0".into(),
            spdx_id: Some("OGL-Canada-2.0".into()),
            website_url: "https://open.canada.ca/en/open-government-licence-canada".into(),
        }
        .into(),
    ])
    .await;

    let metadata_state =
        DataWriterMetadataState::build(harness.dataset.clone(), &odf::BlockRef::Head, None, None)
            .await
            .unwrap();

    assert_matches!(
        metadata_state,
        DataWriterMetadataState {
            schema: None,
            source_event: Some(_),
            merge_strategy: odf::metadata::MergeStrategy::Ledger(_),
            vocab,
            prev_offset: None,
            prev_checkpoint: None,
            prev_watermark: None,
            prev_source_state: None,
            ..
        } if vocab == odf::metadata::DatasetVocabulary::default()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct Harness {
    temp_dir: tempfile::TempDir,
    dataset: ResolvedDataset,
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
            .add::<DidGeneratorDefault>()
            .add::<SystemTimeSourceDefault>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                datasets_dir,
            ))
            .add::<odf::dataset::DatasetLfsBuilderDefault>()
            .build();

        let storage_unit = catalog
            .get_one::<odf::dataset::DatasetStorageUnitLocalFs>()
            .unwrap();

        let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

        use odf::dataset::DatasetStorageUnitWriter;
        let foo_stored = storage_unit
            .store_dataset(
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(odf::DatasetKind::Root).build(),
                )
                .system_time(system_time)
                .build_typed(),
            )
            .await
            .unwrap();

        foo_stored
            .dataset
            .as_metadata_chain()
            .set_ref(
                &odf::BlockRef::Head,
                &foo_stored.seed,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Some(None),
                },
            )
            .await
            .unwrap();

        for event in dataset_events {
            foo_stored
                .dataset
                .commit_event(
                    event,
                    odf::dataset::CommitOpts {
                        system_time: Some(system_time),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }

        let dataset = ResolvedDataset::from_stored(&foo_stored, &foo_alias);

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));

        let writer = DataWriterDataFusion::from_metadata_chain(
            ctx.clone(),
            dataset.clone(),
            &odf::BlockRef::Head,
            None,
        )
        .await
        .unwrap();

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
        self.writer = DataWriterDataFusion::from_metadata_chain(
            self.ctx.clone(),
            self.dataset.clone(),
            &odf::BlockRef::Head,
            None,
        )
        .await
        .unwrap();
    }

    async fn commit_event(
        &mut self,
        event: impl Into<odf::metadata::MetadataEvent>,
    ) -> Result<odf::dataset::CommitResult, odf::dataset::CommitError> {
        let res = self
            .dataset
            .commit_event(
                event.into(),
                odf::dataset::CommitOpts {
                    system_time: Some(self.system_time),
                    ..Default::default()
                },
            )
            .await;

        if res.is_ok() {
            self.reset_writer().await;
        }

        res
    }

    async fn write_opts(
        &mut self,
        data: &str,
        schema: &str,
        new_source_state: Option<odf::metadata::SourceState>,
    ) -> Result<WriteDataResult, WriteDataError> {
        let df = if data.is_empty() {
            None
        } else {
            let data_path = self.temp_dir.path().join("data.bin");
            std::fs::write(&data_path, data).unwrap();

            let df = ReaderCsv::new(
                self.ctx.clone(),
                odf::metadata::ReadStepCsv {
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

        let write_result = self
            .writer
            .write(
                df,
                WriteDataOpts {
                    system_time: self.system_time,
                    source_event_time: self.source_event_time,
                    new_watermark: None,
                    new_source_state,
                    data_staging_path: self.temp_dir.path().join("data.parquet"),
                },
            )
            .await?;

        self.dataset
            .as_metadata_chain()
            .set_ref(
                &odf::BlockRef::Head,
                &write_result.new_head,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Some(Some(&write_result.old_head)),
                },
            )
            .await
            .unwrap();

        Ok(write_result)
    }

    async fn write(&mut self, data: &str, schema: &str) -> Result<WriteDataResult, WriteDataError> {
        self.write_opts(data, schema, None).await
    }

    async fn get_last_schema(&self) -> odf::schema::DataSchema {
        self.get_last_schema_block().await.1.event.schema.unwrap()
    }

    async fn get_last_schema_block(
        &self,
    ) -> (
        odf::Multihash,
        odf::MetadataBlockTyped<odf::metadata::SetDataSchema>,
    ) {
        use futures::StreamExt;
        use odf::dataset::{MetadataChainExt, TryStreamExtExt};
        use odf::metadata::AsTypedBlock;

        let (hash, block) = self
            .dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_ok(|(_, b)| b.as_typed::<odf::metadata::SetDataSchema>().is_some())
            .next()
            .await
            .unwrap()
            .unwrap();

        (
            hash,
            block.into_typed::<odf::metadata::SetDataSchema>().unwrap(),
        )
    }

    async fn get_last_data_block(&self) -> odf::MetadataBlockTyped<odf::metadata::AddData> {
        use futures::StreamExt;
        use odf::dataset::MetadataChainExt;
        use odf::metadata::AsTypedBlock;

        let (_, block) = self
            .dataset
            .as_metadata_chain()
            .iter_blocks()
            .next()
            .await
            .unwrap()
            .unwrap();
        block.into_typed::<odf::metadata::AddData>().unwrap()
    }

    async fn get_last_data_file(&self) -> PathBuf {
        let block = self.get_last_data_block().await;

        odf::utils::data::local_url::into_local_path(
            self.dataset
                .as_data_repo()
                .get_internal_url(&block.event.new_data.unwrap().physical_hash)
                .await,
        )
        .unwrap()
    }

    async fn get_last_data(&self) -> DataFrameExt {
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
            .into()
    }
}
