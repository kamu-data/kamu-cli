// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::errors::ParquetError;

pub struct ParquetWriterHelper;

impl ParquetWriterHelper {
    pub fn from_sample_data(path: &Path) -> Result<(), ParquetError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let a: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let b: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));

        let record_batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&a), Arc::clone(&b)])
                .unwrap();

        Self::from_record_batch(path, &record_batch)
    }

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
