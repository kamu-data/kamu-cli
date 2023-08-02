// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::path::PathBuf;

use datafusion::prelude::*;
use kamu_data_utils::testing::*;
use kamu_ingest_datafusion::*;
use opendatafabric::*;

///////////////////////////////////////////////////////////////////////////////

pub async fn test_reader_success_textual<R: Reader, C: Into<ReadStep>>(
    reader: R,
    conf: C,
    input: &str,
    expected_schema: &str,
    expected_data: &str,
) {
    test_reader_success(
        reader,
        conf,
        |path| async {
            std::fs::write(path, input.trim()).unwrap();
        },
        expected_schema,
        expected_data,
    )
    .await
}

///////////////////////////////////////////////////////////////////////////////

pub async fn test_reader_textual<R: Reader, C: Into<ReadStep>, F, Fut>(
    reader: R,
    conf: C,
    input: &str,
    check_result: F,
) where
    F: FnOnce(Result<DataFrame, ReadError>) -> Fut,
    Fut: Future<Output = ()>,
{
    test_reader(
        reader,
        conf,
        |path| async {
            std::fs::write(path, input.trim()).unwrap();
        },
        check_result,
    )
    .await
}

///////////////////////////////////////////////////////////////////////////////

pub async fn test_reader_success<R: Reader, C: Into<ReadStep>, F, Fut>(
    reader: R,
    conf: C,
    input: F,
    expected_schema: &str,
    expected_data: &str,
) where
    F: FnOnce(PathBuf) -> Fut,
    Fut: Future<Output = ()>,
{
    test_reader(reader, conf, input, |res| async {
        let df = res.unwrap();
        assert_schema_eq(df.schema(), expected_schema);
        assert_data_eq(df, expected_data).await;
    })
    .await;
}

///////////////////////////////////////////////////////////////////////////////

pub async fn test_reader<R: Reader, C: Into<ReadStep>, F1, Fut1, F2, Fut2>(
    reader: R,
    conf: C,
    input: F1,
    check_result: F2,
) where
    F1: FnOnce(PathBuf) -> Fut1,
    Fut1: Future<Output = ()>,
    F2: FnOnce(Result<DataFrame, ReadError>) -> Fut2,
    Fut2: Future<Output = ()>,
{
    let temp_dir = tempfile::tempdir().unwrap();

    let path = temp_dir.path().join("data");
    input(path.clone()).await;

    let ctx = SessionContext::new();
    let res = reader.read(&ctx, &path, &conf.into()).await;
    check_result(res).await;
}
