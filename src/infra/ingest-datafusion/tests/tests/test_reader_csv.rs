// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::prelude::*;
use kamu_data_utils::testing::*;
use kamu_ingest_datafusion::*;
use opendatafabric::*;

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_read_csv_with_schema() {
    let temp_dir = tempfile::tempdir().unwrap();
    let ctx = SessionContext::new();

    let path = temp_dir.path().join("data.csv");
    std::fs::write(
        &path,
        r#"
city,population
A,1000
B,2000
C,3000
        "#
        .trim(),
    )
    .unwrap();

    let reader = ReaderCsv {};
    let df = reader
        .read(
            &ctx,
            &path,
            &ReadStep::Csv(ReadStepCsv {
                header: Some(true),
                schema: Some(vec![
                    "city string not null".to_string(),
                    "population int not null".to_string(),
                ]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    assert_schema_eq(
        df.schema(),
        r#"
message arrow_schema {
  REQUIRED BYTE_ARRAY city (STRING);
  REQUIRED INT32 population;
}
        "#,
    );

    assert_data_eq(
        df,
        r#"
+------+------------+
| city | population |
+------+------------+
| A    | 1000       |
| B    | 2000       |
| C    | 3000       |
+------+------------+
        "#,
    )
    .await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_read_csv_infer_schema() {
    let temp_dir = tempfile::tempdir().unwrap();
    let ctx = SessionContext::new();

    let path = temp_dir.path().join("data.csv");
    std::fs::write(
        &path,
        r#"
city,population
A,1000
B,2000
C,3000
        "#
        .trim(),
    )
    .unwrap();

    let reader = ReaderCsv {};
    let df = reader
        .read(
            &ctx,
            &path,
            &ReadStep::Csv(ReadStepCsv {
                header: Some(true),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    assert_schema_eq(
        df.schema(),
        r#"
message arrow_schema {
  OPTIONAL BYTE_ARRAY city (STRING);
  OPTIONAL INT64 population;
}
        "#,
    );

    assert_data_eq(
        df,
        r#"
+------+------------+
| city | population |
+------+------------+
| A    | 1000       |
| B    | 2000       |
| C    | 3000       |
+------+------------+
        "#,
    )
    .await;
}
