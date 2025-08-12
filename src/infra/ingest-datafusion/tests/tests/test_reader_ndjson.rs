// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use indoc::indoc;
use kamu_ingest_datafusion::*;

use super::test_reader_common;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_ndjson_with_schema() {
    test_reader_common::test_reader_success_textual(
        ReaderNdJson::new(
            SessionContext::new(),
            odf::metadata::ReadStepNdJson {
                schema: Some(vec![
                    "city string not null".to_string(),
                    "population int not null".to_string(),
                ]),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        indoc!(
            r#"
            {"city": "A", "population": 1000}
            {"city": "B", "population": 2000}
            {"city": "C", "population": 3000}
            "#
        ),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED BYTE_ARRAY city (STRING);
              REQUIRED INT32 population;
            }
            "#
        ),
        indoc!(
            r#"
            +------+------------+
            | city | population |
            +------+------------+
            | A    | 1000       |
            | B    | 2000       |
            | C    | 3000       |
            +------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_ndjson_infer_schema() {
    test_reader_common::test_reader_success_textual(
        ReaderNdJson::new(
            SessionContext::new(),
            odf::metadata::ReadStepNdJson {
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        indoc!(
            r#"
            {"city": "A", "population": 1000}
            {"city": "B", "population": 2000}
            {"city": "C", "population": 3000}
            "#
        ),
        indoc!(
            r#"
            message arrow_schema {
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT64 population;
            }
            "#
        ),
        indoc!(
            r#"
            +------+------------+
            | city | population |
            +------+------------+
            | A    | 1000       |
            | B    | 2000       |
            | C    | 3000       |
            +------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_ndjson_format_date() {
    test_reader_common::test_reader_success_textual(
        ReaderNdJson::new(
            SessionContext::new(),
            odf::metadata::ReadStepNdJson {
                schema: Some(vec!["date date not null".to_string()]),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        indoc!(
            r#"
            {"date": "2022-09-25"}
            "#
        ),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT32 date (DATE);
            }
            "#
        ),
        indoc!(
            r#"
            +------------+
            | date       |
            +------------+
            | 2022-09-25 |
            +------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_ndjson_format_timestamp() {
    // Test below not stating a correct behavior but simply attempt to capture how
    // DataFusion behaves currently.
    //
    // It appears that:
    // - naive date-times are treated as UTC
    // - different timezones are normalized to UTC with TZ information getting lost
    test_reader_common::test_reader_success_textual(
        ReaderNdJson::new(
            SessionContext::new(),
            odf::metadata::ReadStepNdJson {
                schema: Some(vec!["event_time timestamp not null".to_string()]),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        indoc!(
            r#"
            {"event_time": "2022-09-25 01:02:03"}
            {"event_time": "2022-09-25T01:02:03"}
            {"event_time": "2022-09-25T01:02:03Z"}
            {"event_time": "2022-09-25T01:02:03-01:00"}
            "#
        ),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 event_time (TIMESTAMP(NANOS,true));
            }
            "#
        ),
        indoc!(
            r#"
            +----------------------+
            | event_time           |
            +----------------------+
            | 2022-09-25T01:02:03Z |
            | 2022-09-25T01:02:03Z |
            | 2022-09-25T01:02:03Z |
            | 2022-09-25T02:02:03Z |
            +----------------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_ndjson_format_timestamp_parse_failed() {
    // Test below not stating a correct behavior but simply attempt to capture how
    // DataFusion behaves currently.
    //
    // It appears that:
    // - naive date-times are treated as UTC
    // - different timezones are normalized to UTC with TZ information getting lost
    test_reader_common::test_reader_textual(
        ReaderNdJson::new(
            SessionContext::new(),
            odf::metadata::ReadStepNdJson {
                schema: Some(vec!["event_time timestamp not null".to_string()]),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        indoc!(
            r#"
            {"event_time": "9/25/2022 1:02:03"}
            "#
        ),
        |res| async {
            let res = res.unwrap().collect().await;
            let Err(DataFusionError::ArrowError(arrow_error, _)) = res else {
                panic!("Expected ArrowError, got: {res:?}");
            };
            assert_matches!(
                *arrow_error,
                ::datafusion::arrow::error::ArrowError::JsonError(_)
            );
        },
    )
    .await;
}
