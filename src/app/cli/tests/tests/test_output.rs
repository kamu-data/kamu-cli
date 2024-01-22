// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::DateTime;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use kamu_cli::records_writers::{RecordsWriter, TableWriter};
use kamu_cli::{ColumnFormat, RecordsFormat};

fn humanize_quantity(num: u64) -> String {
    use num_format::{Locale, ToFormattedString};
    if num == 0 {
        return "-".to_owned();
    }
    num.to_formatted_string(&Locale::en)
}

#[test_log::test(tokio::test)]
async fn test_records_format() {
    let records = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("num", DataType::UInt64, false),
            Field::new("str", DataType::Utf8, true),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
                false,
            ),
        ])),
        vec![
            Arc::new(UInt64Array::from(vec![0, 1000])),
            Arc::new(StringArray::from(vec!["a".to_string(), "b".to_string()])),
            Arc::new(
                TimestampMillisecondArray::from(vec![
                    DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z")
                        .unwrap()
                        .timestamp_millis(),
                    DateTime::parse_from_rfc3339("2023-01-02T00:00:00Z")
                        .unwrap()
                        .timestamp_millis(),
                ])
                .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();

    let fmt = RecordsFormat::default().with_column_formats(vec![
        ColumnFormat::new()
            .with_style_spec("r")
            .with_value_fmt_t(humanize_quantity),
        ColumnFormat::new().with_style_spec("l"),
        ColumnFormat::default(),
    ]);

    let mut buf = Vec::new();
    let mut writer = TableWriter::new(fmt, &mut buf);
    writer.write_batch(&records).unwrap();
    writer.finish().unwrap();

    assert_eq!(
        std::str::from_utf8(&buf)
            .unwrap()
            .trim()
            .replace("\r\n", "\n"),
        indoc::indoc!(
            r"
            ┌───────┬─────┬──────────────────────┐
            │  num  │ str │         time         │
            ├───────┼─────┼──────────────────────┤
            │     - │ a   │ 2023-01-01T00:00:00Z │
            │ 1,000 │ b   │ 2023-01-02T00:00:00Z │
            └───────┴─────┴──────────────────────┘
            "
        )
        .trim()
    );
}
