// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::prelude::SessionContext;
use indoc::indoc;
use kamu_ingest_datafusion::*;

use super::test_reader_common;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_csv_with_schema() {
    test_reader_common::test_reader_success_textual(
        ReaderCsv::new(
            SessionContext::new(),
            odf::metadata::ReadStepCsv {
                header: Some(true),
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
            city,population
            A,1000
            B,2000
            C,3000
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
async fn test_read_csv_no_schema_no_infer() {
    test_reader_common::test_reader_success_textual(
        ReaderCsv::new(
            SessionContext::new(),
            odf::metadata::ReadStepCsv {
                header: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        indoc!(
            r#"
            city,population
            A,1000
            B,2000
            C,3000
            "#
        ),
        indoc!(
            r#"
            message arrow_schema {
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL BYTE_ARRAY population (STRING);
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
async fn test_read_csv_no_schema_infer() {
    test_reader_common::test_reader_success_textual(
        ReaderCsv::new(
            SessionContext::new(),
            odf::metadata::ReadStepCsv {
                header: Some(true),
                infer_schema: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        indoc!(
            r#"
            city,population
            A,1000
            B,2000
            C,3000
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
async fn test_read_csv_no_header() {
    test_reader_common::test_reader_success_textual(
        ReaderCsv::new(
            SessionContext::new(),
            odf::metadata::ReadStepCsv {
                schema: Some(vec![
                    "city STRING".to_string(),
                    "population BIGINT".to_string(),
                ]),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        indoc!(
            r#"
            A,1000
            B,2000
            C,3000
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
async fn test_read_csv_null_values() {
    test_reader_common::test_reader_success_textual(
        ReaderCsv::new(
            SessionContext::new(),
            odf::metadata::ReadStepCsv {
                header: Some(true),
                infer_schema: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        indoc!(
            r#"
            city,population
            A,1000
            B,
            C,3000
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
            | B    |            |
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
async fn test_read_tsv_null_values() {
    test_reader_common::test_reader_success_textual(
        ReaderCsv::new(
            SessionContext::new(),
            odf::metadata::ReadStepCsv {
                header: Some(false),
                separator: Some("\t".to_string()),
                schema: Some(vec![
                    "a INT".to_string(),
                    "b INT".to_string(),
                    "c INT".to_string(),
                    "d INT".to_string(),
                ]),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        indoc!(
            "
            1\t2\t3\t4
            1\t\t\t4
            1\t2\t\t
            \t\t3\t4
            "
        ),
        indoc!(
            r#"
            message arrow_schema {
              OPTIONAL INT32 a;
              OPTIONAL INT32 b;
              OPTIONAL INT32 c;
              OPTIONAL INT32 d;
            }
            "#
        ),
        indoc!(
            r#"
            +---+---+---+---+
            | a | b | c | d |
            +---+---+---+---+
            | 1 | 2 | 3 | 4 |
            | 1 |   |   | 4 |
            | 1 | 2 |   |   |
            |   |   | 3 | 4 |
            +---+---+---+---+
            "#
        ),
    )
    .await;
}
