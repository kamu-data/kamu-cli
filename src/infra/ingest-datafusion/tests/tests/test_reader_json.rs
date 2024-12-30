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
async fn test_read_json_object() {
    let temp_dir: tempfile::TempDir = tempfile::tempdir().unwrap();

    test_reader_common::test_reader_success_textual(
        ReaderJson::new(
            SessionContext::new(),
            odf::metadata::ReadStepJson {
                sub_path: Some("result.cities".to_string()),
                schema: Some(vec![
                    "city string not null".to_string(),
                    "population int not null".to_string(),
                ]),
                ..Default::default()
            },
            temp_dir.path().join("reader-tmp"),
        )
        .await
        .unwrap(),
        indoc!(
            r#"
            {
                "result": {
                    "cities": [
                        {"city": "A", "population": 1000},
                        {"city": "B", "population": 2000},
                        {"city": "C", "population": 3000}
                    ]
                }
            }
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
async fn test_read_json_array() {
    let temp_dir: tempfile::TempDir = tempfile::tempdir().unwrap();

    test_reader_common::test_reader_success_textual(
        ReaderJson::new(
            SessionContext::new(),
            odf::metadata::ReadStepJson {
                sub_path: None,
                schema: Some(vec![
                    "city string not null".to_string(),
                    "population int not null".to_string(),
                ]),
                ..Default::default()
            },
            temp_dir.path().join("reader-tmp"),
        )
        .await
        .unwrap(),
        indoc!(
            r#"
            [
                {"city": "A", "population": 1000},
                {"city": "B", "population": 2000},
                {"city": "C", "population": 3000}
            ]
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
