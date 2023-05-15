// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;

pub use datafusion::arrow::csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
pub use datafusion::arrow::json::{
    ArrayWriter as JsonArrayWriter,
    LineDelimitedWriter as JsonLineDelimitedWriter,
};
use datafusion::arrow::record_batch::RecordBatch;

/////////////////////////////////////////////////////////////////////////////////////////

type Error = std::io::Error;

pub trait RecordsWriter {
    fn write_batch(&mut self, records: &RecordBatch) -> Result<(), Error>;

    fn write_batches(&mut self, record_batches: &[RecordBatch]) -> Result<(), Error> {
        for records in record_batches {
            self.write_batch(records)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// CSV
/////////////////////////////////////////////////////////////////////////////////////////

impl<W: Write> RecordsWriter for CsvWriter<W> {
    fn write_batch(&mut self, records: &RecordBatch) -> Result<(), Error> {
        CsvWriter::write(self, records).unwrap();
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// JSON
/////////////////////////////////////////////////////////////////////////////////////////

impl<W: Write> RecordsWriter for JsonArrayWriter<W> {
    fn write_batch(&mut self, _records: &RecordBatch) -> Result<(), Error> {
        unimplemented!();
    }

    fn write_batches(&mut self, record_batches: &[RecordBatch]) -> Result<(), Error> {
        JsonArrayWriter::write_batches(self, record_batches).unwrap();
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Error> {
        JsonArrayWriter::finish(self).unwrap();
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// JSON Line-Delimited
/////////////////////////////////////////////////////////////////////////////////////////

impl<W: Write> RecordsWriter for JsonLineDelimitedWriter<W> {
    fn write_batch(&mut self, _records: &RecordBatch) -> Result<(), Error> {
        unimplemented!();
    }

    fn write_batches(&mut self, record_batches: &[RecordBatch]) -> Result<(), Error> {
        JsonLineDelimitedWriter::write_batches(self, record_batches).unwrap();
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Error> {
        JsonLineDelimitedWriter::finish(self).unwrap();
        Ok(())
    }
}
