// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use chrono::DateTime;
use datafusion::prelude::SessionContext;
use indoc::indoc;
use kamu_ingest_datafusion::*;

use super::test_reader_common;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn write_test_data(path: impl AsRef<Path>) {
    use datafusion::arrow::array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            odf::metadata::DatasetVocabulary::DEFAULT_OFFSET_COLUMN_NAME,
            DataType::UInt64,
            false,
        ),
        Field::new(
            odf::metadata::DatasetVocabulary::DEFAULT_SYSTEM_TIME_COLUMN_NAME,
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            odf::metadata::DatasetVocabulary::DEFAULT_EVENT_TIME_COLUMN_NAME,
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("city", DataType::Utf8, false),
        Field::new("population", DataType::Int64, false),
    ]));

    let system_time = DateTime::parse_from_rfc3339("2023-02-01T00:00:00Z")
        .unwrap()
        .timestamp_millis();
    let event_time = DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z")
        .unwrap()
        .timestamp_millis();

    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(array::UInt64Array::from(vec![0, 1, 2])),
            Arc::new(
                array::TimestampMillisecondArray::from(vec![system_time, system_time, system_time])
                    .with_timezone("UTC"),
            ),
            Arc::new(
                array::TimestampMillisecondArray::from(vec![event_time, event_time, event_time])
                    .with_timezone("UTC"),
            ),
            Arc::new(array::StringArray::from(vec![
                "vancouver",
                "seattle",
                "kyiv",
            ])),
            Arc::new(array::Int64Array::from(vec![675_000, 733_000, 2_884_000])),
        ],
    )
    .unwrap();

    use datafusion::parquet::arrow::ArrowWriter;

    let mut arrow_writer = ArrowWriter::try_new(
        std::fs::File::create(path).unwrap(),
        record_batch.schema(),
        None,
    )
    .unwrap();

    arrow_writer.write(&record_batch).unwrap();
    arrow_writer.close().unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_parquet() {
    test_reader_common::test_reader_success(
        ReaderParquet::new(
            SessionContext::new(),
            odf::metadata::ReadStepParquet { schema: None },
        )
        .await
        .unwrap(),
        |path| async {
            write_test_data(path);
        },
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset (INTEGER(64,false));
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
              REQUIRED BYTE_ARRAY city (STRING);
              REQUIRED INT64 population;
            }
            "#
        ),
        indoc!(
            r#"
            +--------+----------------------+----------------------+-----------+------------+
            | offset | system_time          | event_time           | city      | population |
            +--------+----------------------+----------------------+-----------+------------+
            | 0      | 2023-02-01T00:00:00Z | 2023-01-01T00:00:00Z | vancouver | 675000     |
            | 1      | 2023-02-01T00:00:00Z | 2023-01-01T00:00:00Z | seattle   | 733000     |
            | 2      | 2023-02-01T00:00:00Z | 2023-01-01T00:00:00Z | kyiv      | 2884000    |
            +--------+----------------------+----------------------+-----------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_parquet_schema_coercion() {
    test_reader_common::test_reader_success(
        ReaderParquet::new(
            SessionContext::new(),
            odf::metadata::ReadStepParquet {
                schema: Some(vec![
                    "event_time string not null".to_string(),
                    "city string not null".to_string(),
                    "population int not null".to_string(),
                ]),
            },
        )
        .await
        .unwrap(),
        |path| async {
            write_test_data(path);
        },
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED BYTE_ARRAY event_time (STRING);
              REQUIRED BYTE_ARRAY city (STRING);
              REQUIRED INT32 population;
            }
            "#
        ),
        indoc!(
            r#"
            +----------------------+-----------+------------+
            | event_time           | city      | population |
            +----------------------+-----------+------------+
            | 2023-01-01T00:00:00Z | vancouver | 675000     |
            | 2023-01-01T00:00:00Z | seattle   | 733000     |
            | 2023-01-01T00:00:00Z | kyiv      | 2884000    |
            +----------------------+-----------+------------+
            "#
        ),
    )
    .await;
}
