// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;

pub struct ParquetWriterHelper;

impl ParquetWriterHelper {
    pub fn from_record_batch(path: &Path, record_batch: &RecordBatch) -> Result<(), ParquetError> {
        use datafusion::parquet::arrow::ArrowWriter;

        let mut arrow_writer = ArrowWriter::try_new(
            std::fs::File::create(path).unwrap(),
            record_batch.schema(),
            None,
        )?;

        arrow_writer.write(&record_batch)?;
        arrow_writer.close()?;

        Ok(())
    }
}
