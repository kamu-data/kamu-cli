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
async fn test_read_ndgeojson_with_schema() {
    let temp_dir: tempfile::TempDir = tempfile::tempdir().unwrap();

    test_reader_common::test_reader_success_textual(
        ReaderNdGeoJson::new(
            SessionContext::new(),
            odf::metadata::ReadStepNdGeoJson {
                schema: Some(vec![
                    "id int not null".to_string(),
                    "zipcode string not null".to_string(),
                    "name string not null".to_string(),
                    "geometry string not null".to_string(),
                ]),
            },
            temp_dir.path().join("reader-tmp")
        )
        .await
        .unwrap(),
        indoc!(
            r#"
            {"type": "Feature", "properties": {"id": 0, "zipcode": "00101", "name": "A"}, "geometry": {"type": "Polygon", "coordinates": [[[0.0, 0.0],[10.0, 0.0],[10.0, 10.0],[0.0, 10.0],[0.0, 0.0]]]}}
            {"type": "Feature", "properties": {"id": 1, "zipcode": "00202", "name": "B"}, "geometry": {"type": "Polygon", "coordinates": [[[0.0, 0.0],[20.0, 0.0],[20.0, 20.0],[0.0, 20.0],[0.0, 0.0]]]}}
            "#
        ),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT32 id;
              REQUIRED BYTE_ARRAY zipcode (STRING);
              REQUIRED BYTE_ARRAY name (STRING);
              REQUIRED BYTE_ARRAY geometry (STRING);
            }
            "#
        ),
        indoc!(
            r#"
            +----+---------+------+--------------------------------------------------------------------------------------------+
            | id | zipcode | name | geometry                                                                                   |
            +----+---------+------+--------------------------------------------------------------------------------------------+
            | 0  | 00101   | A    | {"coordinates":[[[0.0,0.0],[10.0,0.0],[10.0,10.0],[0.0,10.0],[0.0,0.0]]],"type":"Polygon"} |
            | 1  | 00202   | B    | {"coordinates":[[[0.0,0.0],[20.0,0.0],[20.0,20.0],[0.0,20.0],[0.0,0.0]]],"type":"Polygon"} |
            +----+---------+------+--------------------------------------------------------------------------------------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_read_ndgeojson_infer_schema() {
    let temp_dir: tempfile::TempDir = tempfile::tempdir().unwrap();

    test_reader_common::test_reader_success_textual(
        ReaderNdGeoJson::new(
            SessionContext::new(),
            odf::metadata::ReadStepNdGeoJson {
                schema: None,
            },
            temp_dir.path().join("reader-tmp"),
        )
        .await
        .unwrap(),
        indoc!(
            r#"
            {"type": "Feature", "properties": {"id": 0, "zipcode": "00101", "name": "A"}, "geometry": {"type": "Polygon", "coordinates": [[[0.0, 0.0],[10.0, 0.0],[10.0, 10.0],[0.0, 10.0],[0.0, 0.0]]]}}
            {"type": "Feature", "properties": {"id": 1, "zipcode": "00202", "name": "B"}, "geometry": {"type": "Polygon", "coordinates": [[[0.0, 0.0],[20.0, 0.0],[20.0, 20.0],[0.0, 20.0],[0.0, 0.0]]]}}
            "#
        ),
        indoc!(
            r#"
            message arrow_schema {
              OPTIONAL BYTE_ARRAY geometry (STRING);
              OPTIONAL INT64 id;
              OPTIONAL BYTE_ARRAY name (STRING);
              OPTIONAL BYTE_ARRAY zipcode (STRING);
            }
            "#
        ),
        indoc!(
            r#"
            +--------------------------------------------------------------------------------------------+----+------+---------+
            | geometry                                                                                   | id | name | zipcode |
            +--------------------------------------------------------------------------------------------+----+------+---------+
            | {"coordinates":[[[0.0,0.0],[10.0,0.0],[10.0,10.0],[0.0,10.0],[0.0,0.0]]],"type":"Polygon"} | 0  | A    | 00101   |
            | {"coordinates":[[[0.0,0.0],[20.0,0.0],[20.0,20.0],[0.0,20.0],[0.0,0.0]]],"type":"Polygon"} | 1  | B    | 00202   |
            +--------------------------------------------------------------------------------------------+----+------+---------+
            "#
        ),
    )
    .await;
}
