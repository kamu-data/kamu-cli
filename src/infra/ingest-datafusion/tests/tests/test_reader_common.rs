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

use kamu_ingest_datafusion::*;
use odf::utils::data::DataFrameExt;
use odf::utils::testing::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_reader_success_textual<R: Reader>(
    reader: R,
    input: &str,
    expected_schema: &str,
    expected_data: &str,
) {
    test_reader_success(
        reader,
        |path| async {
            std::fs::write(path, input.trim()).unwrap();
        },
        expected_schema,
        expected_data,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_reader_textual<R: Reader, F, Fut>(reader: R, input: &str, check_result: F)
where
    F: FnOnce(Result<DataFrameExt, ReadError>) -> Fut,
    Fut: Future<Output = ()>,
{
    test_reader(
        reader,
        |path| async {
            std::fs::write(path, input.trim()).unwrap();
        },
        check_result,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_reader_success<R: Reader, F, Fut>(
    reader: R,
    input: F,
    expected_schema: &str,
    expected_data: &str,
) where
    F: FnOnce(PathBuf) -> Fut,
    Fut: Future<Output = ()>,
{
    test_reader(reader, input, |res| async {
        let df = res.unwrap();
        assert_schema_eq(df.schema(), expected_schema);
        assert_data_eq(df, expected_data).await;
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_reader<R: Reader, F1, Fut1, F2, Fut2>(reader: R, input: F1, check_result: F2)
where
    F1: FnOnce(PathBuf) -> Fut1,
    Fut1: Future<Output = ()>,
    F2: FnOnce(Result<DataFrameExt, ReadError>) -> Fut2,
    Fut2: Future<Output = ()>,
{
    let temp_dir = tempfile::tempdir().unwrap();

    let path = temp_dir.path().join("data");
    input(path.clone()).await;

    let res = reader.read(&path).await;
    check_result(res).await;
}
