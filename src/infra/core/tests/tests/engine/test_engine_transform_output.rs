// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use kamu::EngineTransformOutput;
use kamu::domain::engine::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_output_with_claimed_records() {
    let harness = EngineTransformOutputHarness::new();
    harness.write_data_file(3);

    let response = harness.respond_with(Some((10, 12)));

    assert_matches!(
        response,
        Ok(TransformResponseExt {
            new_data: Some(_),
            ..
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_output_with_more_records_than_claimed() {
    let harness = EngineTransformOutputHarness::new();
    harness.write_data_file(5);

    let response = harness.respond_with(Some((10, 12)));

    assert_matches!(response, Err(EngineError::ContractError(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_output_with_claimed_records_but_empty_file() {
    let harness = EngineTransformOutputHarness::new();
    harness.write_data_file(0);

    let response = harness.respond_with(Some((0, 99)));

    assert_matches!(response, Err(EngineError::ContractError(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_output_with_inverted_claimed_offsets() {
    let harness = EngineTransformOutputHarness::new();
    harness.write_data_file(3);

    let response = harness.respond_with(Some((12, 10)));

    assert_matches!(response, Err(EngineError::ContractError(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_output_with_unrepresentable_claimed_offsets() {
    let harness = EngineTransformOutputHarness::new();
    harness.write_data_file(0);

    let response = harness.respond_with(Some((0, u64::MAX)));

    assert_matches!(response, Err(EngineError::ContractError(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_output_with_records_but_no_claimed_offsets() {
    let harness = EngineTransformOutputHarness::new();
    harness.write_data_file(2);

    let response = harness.respond_with(None);

    assert_matches!(response, Err(EngineError::ContractError(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_output_with_claimed_records_but_no_file() {
    let harness = EngineTransformOutputHarness::new();

    let response = harness.respond_with(Some((0, 2)));

    assert_matches!(response, Err(EngineError::ContractError(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct EngineTransformOutputHarness {
    tempdir: tempfile::TempDir,
}

impl EngineTransformOutputHarness {
    fn new() -> Self {
        Self {
            tempdir: tempfile::tempdir().unwrap(),
        }
    }

    fn data_path(&self) -> PathBuf {
        self.tempdir.path().join("data.parquet")
    }

    /// Writes a data file with `num_records` records, as an engine would
    fn write_data_file(&self, num_records: i64) {
        let offsets: ArrayRef = Arc::new(Int64Array::from_iter_values(0..num_records));
        let batch = RecordBatch::try_from_iter([("offset", offsets)]).unwrap();

        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(self.data_path()).unwrap(),
            batch.schema(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Materializes the output for an engine response claiming
    /// `claimed_offsets`
    fn respond_with(
        &self,
        claimed_offsets: Option<(u64, u64)>,
    ) -> Result<TransformResponseExt, EngineError> {
        EngineTransformOutput {
            data_path: self.data_path(),
            checkpoint_path: self.tempdir.path().join("checkpoint"),
        }
        .into_response(odf::metadata::TransformResponseSuccess {
            new_offset_interval: claimed_offsets
                .map(|(start, end)| odf::metadata::OffsetInterval { start, end }),
            new_watermark: None,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
