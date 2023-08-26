// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use indoc::indoc;
use kamu_ingest_datafusion::*;
use opendatafabric::*;

use super::test_reader_common;

///////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_csv_with_schema() {
    test_reader_common::test_reader_success_textual(
        ReaderCsv {},
        ReadStepCsv {
            header: Some(true),
            schema: Some(vec![
                "city string not null".to_string(),
                "population int not null".to_string(),
            ]),
            ..Default::default()
        },
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

///////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_csv_no_schema_no_infer() {
    test_reader_common::test_reader_success_textual(
        ReaderCsv {},
        ReadStepCsv {
            header: Some(true),
            ..Default::default()
        },
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

///////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_csv_no_schema_infer() {
    test_reader_common::test_reader_success_textual(
        ReaderCsv {},
        ReadStepCsv {
            header: Some(true),
            infer_schema: Some(true),
            ..Default::default()
        },
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

///////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_csv_null_values() {
    test_reader_common::test_reader_success_textual(
        ReaderCsv {},
        ReadStepCsv {
            header: Some(true),
            infer_schema: Some(true),
            ..Default::default()
        },
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
