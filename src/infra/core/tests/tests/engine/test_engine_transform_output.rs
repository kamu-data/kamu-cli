// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::path::{Path, PathBuf};
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
fn test_output_with_records_disagreeing_with_claimed_offsets() {
    // (case, records in the data file or no file, claimed offsets)
    let cases = [
        ("more records than claimed", Some(5), Some((10, 12))),
        ("claimed records but empty file", Some(0), Some((0, 99))),
        ("inverted claimed offsets", Some(3), Some((12, 10))),
        (
            "unrepresentable claimed offsets",
            Some(0),
            Some((0, u64::MAX)),
        ),
        ("records but no claimed offsets", Some(2), None),
        ("claimed records but no file", None, Some((0, 2))),
    ];

    for (case, num_records, claimed_offsets) in cases {
        let harness = EngineTransformOutputHarness::new();
        if let Some(num_records) = num_records {
            harness.write_data_file(num_records);
        }

        let response = harness.respond_with(claimed_offsets);

        assert_matches!(response, Err(EngineError::ContractError(_)), "{case}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_output_data_as_symlink() {
    let harness = EngineTransformOutputHarness::new();
    harness.write_data_file_behind_symlink(3);

    let response = harness.respond_with(Some((0, 2)));

    assert_matches!(response, Err(EngineError::ContractError(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_output_data_as_directory() {
    let harness = EngineTransformOutputHarness::new();
    harness.create_data_path_as_directory();

    let response = harness.respond_with(Some((0, 2)));

    assert_matches!(response, Err(EngineError::ContractError(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_output_checkpoint_as_plain_file() {
    let harness = EngineTransformOutputHarness::new();
    harness.write_checkpoint_file();

    let response = harness.respond_with(None);

    assert_matches!(
        response,
        Ok(TransformResponseExt {
            new_checkpoint: Some(_),
            ..
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_output_checkpoint_as_symlink() {
    let harness = EngineTransformOutputHarness::new();
    harness.write_checkpoint_file_behind_symlink();

    let response = harness.respond_with(None);

    assert_matches!(response, Err(EngineError::ContractError(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_output_checkpoint_as_directory() {
    let harness = EngineTransformOutputHarness::new();
    harness.create_checkpoint_path_as_directory();

    let response = harness.respond_with(None);

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

    fn checkpoint_path(&self) -> PathBuf {
        self.tempdir.path().join("checkpoint")
    }

    /// Writes a data file with `num_records` records, as an engine would
    fn write_data_file(&self, num_records: i64) {
        self.write_parquet_file(&self.data_path(), num_records);
    }

    /// Writes a valid data file elsewhere and points the data path at it
    fn write_data_file_behind_symlink(&self, num_records: i64) {
        let target = self.tempdir.path().join("elsewhere.parquet");
        self.write_parquet_file(&target, num_records);
        std::os::unix::fs::symlink(target, self.data_path()).unwrap();
    }

    fn create_data_path_as_directory(&self) {
        std::fs::create_dir(self.data_path()).unwrap();
    }

    fn create_checkpoint_path_as_directory(&self) {
        std::fs::create_dir(self.checkpoint_path()).unwrap();
    }

    fn write_checkpoint_file(&self) {
        std::fs::write(self.checkpoint_path(), b"checkpoint").unwrap();
    }

    /// Writes a checkpoint elsewhere and points the checkpoint path at it
    fn write_checkpoint_file_behind_symlink(&self) {
        let target = self.tempdir.path().join("elsewhere-checkpoint");
        std::fs::write(&target, b"checkpoint").unwrap();
        std::os::unix::fs::symlink(target, self.checkpoint_path()).unwrap();
    }

    fn write_parquet_file(&self, path: &Path, num_records: i64) {
        let offsets: ArrayRef = Arc::new(Int64Array::from_iter_values(0..num_records));
        let batch = RecordBatch::try_from_iter([("offset", offsets)]).unwrap();

        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(path).unwrap(), batch.schema(), None)
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
            checkpoint_path: self.checkpoint_path(),
        }
        .into_response(odf::metadata::TransformResponseSuccess {
            new_offset_interval: claimed_offsets
                .map(|(start, end)| odf::metadata::OffsetInterval { start, end }),
            new_watermark: None,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
